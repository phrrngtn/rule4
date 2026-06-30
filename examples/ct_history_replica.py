"""SQL Server Change Tracking -> DuckLake payload time-series, via the acquisition seam.

The same poll-since-HWM loop as ``ct_replica.py``, but driven through
``ColumnCollection.sync`` + ``ChangeTrackingDriver`` instead of hand-rolled CHANGETABLE
glue — and feeding ``HistoryReplica`` (one snapshot per CT *version*) rather than the
current-state ``Replica`` (one per poll). The version is the staple: CHANGETABLE's net
changes carry each key's ``SYS_CHANGE_VERSION``, and ``sync`` groups by it, so a single
poll lands as several version-addressable snapshots.

The driver is the only CT-specific piece; the apply side (``HistoryReplica``) is the same
code the user-column and (future) backlog drivers feed.

Integrated security — **no credentials in this file** (Kerberos via Trusted_Connection).
Prereqs: ``kinit paulharrington@PHRRNGTN.ARPA``; ``/Library/Preferences/edu.mit.Kerberos``
with ``rdns = false``; the ``gfe`` SQL Server reachable.
Run:  uv run python examples/ct_history_replica.py
"""
import os
import shutil

import pyodbc
from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

import ducklake_oob_writer as dl
from column_collection import Col, ColumnCollection, ChangeTrackingDriver

KEY = "id"
SRC_COLS = [("id", "int"), ("name", "nvarchar"), ("region", "nvarchar")]   # SQL Server types
MSSQL = ("DRIVER={ODBC Driver 18 for SQL Server};SERVER=gfe.phrrngtn.arpa;DATABASE=rule4_test;"
         "Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate=yes")


def main():
    src = pyodbc.connect(MSSQL, autocommit=True, timeout=15)
    cur = src.cursor()

    # --- enable Change Tracking + a fresh CT-enabled table ---
    cur.execute("""IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_databases
                                  WHERE database_id = DB_ID('rule4_test'))
                   ALTER DATABASE rule4_test
                     SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)""")
    cur.execute("IF OBJECT_ID('dbo.cust') IS NOT NULL DROP TABLE dbo.cust")
    cur.execute("CREATE TABLE dbo.cust (id INT PRIMARY KEY, name NVARCHAR(50), region NVARCHAR(50))")
    cur.execute("ALTER TABLE dbo.cust ENABLE CHANGE_TRACKING")
    cur.executemany("INSERT INTO dbo.cust (id, name, region) VALUES (?, ?, ?)",
                    [(1, "a", "X"), (2, "b", "X"), (3, "c", "Y")])

    # --- the column-collection drives the DuckLake DDL and the tail projection ---
    cc = ColumnCollection("dbo", "cust", [Col(n, t, dialect="sqlserver") for n, t in SRC_COLS],
                          key=KEY, dialect="sqlserver")
    driver = ChangeTrackingDriver(KEY, schema="dbo")

    base = "/tmp/ct_history"
    shutil.rmtree(base, ignore_errors=True)
    os.makedirs(base)
    eng = create_engine(f"sqlite:///{base}/cat.sqlite")
    dl.create_catalog(eng)
    w = dl.DuckLakeWriter(eng, dl.DUCKLAKE_METADATA)
    w.init_catalog(data_path=f"{base}/data")
    cc.record_in_ducklake(w)
    rep = dl.HistoryReplica(w, "cust", KEY)

    # --- initial snapshot load; the watermark is the current CT version ---
    v0 = cur.execute("SELECT CHANGE_TRACKING_CURRENT_VERSION()").fetchone()[0]
    base_rows = cur.execute("SELECT id, name, region FROM dbo.cust").fetchall()
    rep.apply_commit([{"op": "I", "key": r[0], "row": {"id": r[0], "name": r[1], "region": r[2]}}
                      for r in base_rows], snapshot_time=driver.snapshot_time(v0))
    logger.info("initial load: {n} rows at version {v} -> {t}",
                n=len(base_rows), v=v0, t=driver.snapshot_time(v0))

    # --- source changes (bound values), each its own transaction => its own CT version ---
    cur.execute("UPDATE dbo.cust SET name = ?, region = ? WHERE id = ?", ("b2", "Z", 2))
    cur.execute("DELETE FROM dbo.cust WHERE id = ?", (3,))
    cur.execute("INSERT INTO dbo.cust (id, name, region) VALUES (?, ?, ?)", (4, "d", "W"))

    # --- one poll -> several version-stamped snapshots, through the seam ---
    sa_eng = create_engine(URL.create("mssql+pyodbc", query={"odbc_connect": MSSQL}))
    with sa_eng.connect() as conn:
        wm = cc.sync(conn, v0, driver, rep)
    sa_eng.dispose()
    logger.info("poll since {v}: new watermark = {wm}", v=v0, wm=wm)
    eng.dispose()

    # --- verify: current state mirrors the source, and each version is a snapshot ---
    sql_state = {r[0]: (r[1], r[2]) for r in cur.execute("SELECT id, name, region FROM dbo.cust")}
    with dl.lake_reader(f"sqlite:{base}/cat.sqlite", f"{base}/data") as conn:
        lake_state = {r[0]: (r[1], r[2]) for r in
                      conn.execute(text("SELECT id, name, region FROM lake.cust")).fetchall()}
        snaps = conn.execute(text(
            "SELECT snapshot_id, snapshot_time FROM lake.snapshots() ORDER BY snapshot_id")).fetchall()
        asof0 = {r[0]: r[1] for r in conn.execute(
            text("SELECT id, name FROM lake.cust AT (TIMESTAMP => :ts) ORDER BY id")
            .bindparams(ts=driver.snapshot_time(v0))).fetchall()}
    logger.info("SQL Server now : {state}", state=sql_state)
    logger.info("DuckLake now   : {state}", state=lake_state)
    logger.info("mirrors source : {ok}", ok=sql_state == lake_state)
    logger.info("snapshots      : {n} (1 initial + 1 per change-version)", n=len(snaps))
    logger.info("AT initial ver : {state}  (== initial load)", state=asof0)

    cur.execute("IF OBJECT_ID('dbo.cust') IS NOT NULL DROP TABLE dbo.cust")   # cleanup
    src.close()


if __name__ == "__main__":
    main()
