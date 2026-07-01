"""Round-trip one table: gfe source -> DuckLake tt-history -> gfe TTST (db'), time-travel intact.

The outbound projector in action. dbo.widget evolves on gfe; we scrape it into a DuckLake
HistoryReplica (the canonical tt-history); ttst.sync projects the intervals out into
dbo.widget_ttst (a transaction-time state table on the same server) by poll-since-HWM, closing
intervals as they close. Then:

  * as-of-latest (the open intervals) must equal the live source  -> round-trip integrity;
  * PIT reconstruction (WHERE tt_start <= @snap < tt_end) replays an earlier state -> the db'
    time-travels *without* ever having touched the source's history;
  * we sample BOTH schemas (source widget + replica widget_ttst) into the same column_role
    time-series and diff them -> the tt_* columns are the only 'skew', confirming the data
    schema round-tripped.

The tt is the DuckLake snapshot_id (the logical clock), so the HWM is skew-immune.
Live vs gfe. Run from column_role/:  uv run python demo_ttst_roundtrip.py
"""
import datetime as dt
import os
import shutil

import pyodbc
from loguru import logger
from sqlalchemy import create_engine

import ducklake_oob_writer as dl
import ttst
from column_collection import ColumnCollection
from registry import Registry, capture

MSSQL = ("DRIVER={ODBC Driver 18 for SQL Server};SERVER=gfe.phrrngtn.arpa;DATABASE=rule4_test;"
         "Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate=yes")
SERVER, DB, TTST = "gfe", "rule4_test", "dbo.widget_ttst"
DL_COLS = [("id", "int64"), ("name", "varchar"), ("region", "varchar")]
SS_COLS = [("id", "INT"), ("name", "NVARCHAR(50)"), ("region", "NVARCHAR(50)")]


def main():
    src = pyodbc.connect(MSSQL, autocommit=True, timeout=15)
    cur = src.cursor()
    base = "/tmp/ttst_roundtrip"
    shutil.rmtree(base, ignore_errors=True)
    os.makedirs(base)
    eng = create_engine(f"sqlite:///{base}/cat.sqlite")
    dl.create_catalog(eng)
    w = dl.DuckLakeWriter(eng, dl.DUCKLAKE_METADATA)
    w.init_catalog(data_path=f"{base}/data")
    w.create_table("main", "widget", DL_COLS)
    rep = dl.HistoryReplica(w, "widget", "id")

    # fresh source + TTST (db')
    cur.execute("IF OBJECT_ID('dbo.widget_ttst') IS NOT NULL DROP TABLE dbo.widget_ttst")
    cur.execute("IF OBJECT_ID('dbo.widget') IS NOT NULL DROP TABLE dbo.widget")
    cur.execute("CREATE TABLE dbo.widget (id INT PRIMARY KEY, name NVARCHAR(50), region NVARCHAR(50))")
    for stmt in ttst.create_ttst_ddl(TTST, SS_COLS, "id"):
        cur.execute(stmt)

    def commit(sql_changes, ops, when):
        for s, params in sql_changes:            # keep the live source in lockstep
            cur.execute(s, params)
        return rep.apply_commit(ops, snapshot_time=when)["snapshot_id"]

    # snapshot A: insert 3 rows
    snapA = commit(
        [("INSERT INTO dbo.widget (id,name,region) VALUES (?,?,?)", (1, "a", "X")),
         ("INSERT INTO dbo.widget (id,name,region) VALUES (?,?,?)", (2, "b", "X")),
         ("INSERT INTO dbo.widget (id,name,region) VALUES (?,?,?)", (3, "c", "Y"))],
        [{"op": "I", "key": i, "row": {"id": i, "name": n, "region": r}}
         for i, n, r in [(1, "a", "X"), (2, "b", "X"), (3, "c", "Y")]],
        dt.datetime(2026, 6, 30, 10))
    r1 = ttst.sync(cur, eng, "widget", TTST, DL_COLS, "id")
    logger.info("sync 1 @ snap {a}: {r}", a=snapA, r=r1)

    # snapshot B: update id=2 ; snapshot C: delete id=3 + insert id=4
    commit([("UPDATE dbo.widget SET name=? WHERE id=?", ("b2", 2))],
           [{"op": "U", "key": 2, "row": {"id": 2, "name": "b2", "region": "X"}}],
           dt.datetime(2026, 6, 30, 11))
    commit([("DELETE FROM dbo.widget WHERE id=?", (3,)),
            ("INSERT INTO dbo.widget (id,name,region) VALUES (?,?,?)", (4, "d", "W"))],
           [{"op": "D", "key": 3}, {"op": "I", "key": 4, "row": {"id": 4, "name": "d", "region": "W"}}],
           dt.datetime(2026, 6, 30, 12))
    r2 = ttst.sync(cur, eng, "widget", TTST, DL_COLS, "id")   # incremental: only the delta
    logger.info("sync 2 (incremental): {r}", r=r2)

    # --- verify round-trip: as-of-latest == live source ---
    latest = {r[0]: (r[1], r[2]) for r in
              cur.execute(f"SELECT id,name,region FROM {TTST} WHERE tt_end IS NULL")}
    now = {r[0]: (r[1], r[2]) for r in cur.execute("SELECT id,name,region FROM dbo.widget")}
    logger.info("TTST as-of-latest : {l}", l=latest)
    logger.info("live source now   : {n}", n=now)
    logger.info("round-trip intact : {ok}", ok=latest == now)

    # --- PIT reconstruction by the logical clock (snapshot_id) ---
    pit = {r[0]: r[1] for r in cur.execute(
        f"SELECT id,name FROM {TTST} WHERE tt_start <= ? AND (tt_end > ? OR tt_end IS NULL) ORDER BY id",
        (snapA, snapA))}
    logger.info("PIT @ snap {a} (initial state): {p}", a=snapA, p=pit)

    # --- schema skew: sample BOTH schemas into one column_role series, diff data columns ---
    reg = Registry(f"{base}/reg.sqlite", f"{base}/regdata")
    reg.record(capture(cur, "sqlserver", SERVER, DB, dt.datetime(2026, 6, 30, 13),
                       only=("widget", "widget_ttst")), dt.datetime(2026, 6, 30, 13))
    T = dt.datetime(2026, 6, 30, 13)
    s_cols = {c.name for c in ColumnCollection.from_column_role(reg, SERVER, DB, "widget", T, schema="dbo").columns}
    d_cols = {c.name for c in ColumnCollection.from_column_role(reg, SERVER, DB, "widget_ttst", T, schema="dbo").columns}
    logger.info("source cols={s}", s=sorted(s_cols))
    logger.info("replica cols={d}", d=sorted(d_cols))
    logger.info("data-schema skew (replica minus tt_*): {skew}  (empty => data schema round-tripped)",
                skew=sorted((d_cols - s_cols) - {"tt_start", "tt_start_ts", "tt_end", "tt_end_ts"}))
    reg.dispose()

    cur.execute("IF OBJECT_ID('dbo.widget_ttst') IS NOT NULL DROP TABLE dbo.widget_ttst")
    cur.execute("IF OBJECT_ID('dbo.widget') IS NOT NULL DROP TABLE dbo.widget")
    eng.dispose()
    src.close()


if __name__ == "__main__":
    main()
