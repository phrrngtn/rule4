"""SQL Server Change Data Capture -> DuckLake payload time-series (full-fidelity history).

The full-fidelity SQL Server flavor: where ct_history_replica.py joins CHANGETABLE to the
live base table (current after-image only), this reads CDC's change tables via
fn_cdc_get_all_changes(..., 'all'), which carry the column *values* of **every** change. So
intermediate versions survive (id=2 goes b -> b2 -> b3 and all three land in DuckLake), and
each snapshot gets its **real commit time** from CDC's lsn_time_mapping
(fn_cdc_map_lsn_to_time) — no synthetic clock.

The driver is the only CDC-specific piece; CDCDriver + ColumnCollection.sync feed the same
HistoryReplica as the user-column / CT / backlog drivers.

CDC capture IS the log scan sys.sp_cdc_scan; the SQL Agent "capture job" just calls it on a
schedule. Agent is needed for *scheduled* capture and for cleanup, NOT for the mechanism — so
this drives the scan directly (Kerberos prereqs as in ct_history_replica.py).
Run:  uv run python examples/cdc_history_replica.py
"""
import os
import shutil
import time

import pyodbc
from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

import ducklake_oob_writer as dl
from column_collection import CDCDriver, Col, ColumnCollection

KEY = "id"
SRC_COLS = [("id", "int"), ("name", "nvarchar"), ("region", "nvarchar")]
MSSQL = ("DRIVER={ODBC Driver 18 for SQL Server};SERVER=gfe.phrrngtn.arpa;DATABASE=rule4_test;"
         "Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate=yes")


def main():
    src = pyodbc.connect(MSSQL, autocommit=True, timeout=15)
    cur = src.cursor()

    # --- enable CDC on the database + a fresh table ---
    cur.execute("IF (SELECT is_cdc_enabled FROM sys.databases WHERE name='rule4_test') = 0 "
                "EXEC sys.sp_cdc_enable_db")
    # idempotent reset: disable any existing capture instance *before* dropping, else a crash
    # leaves an orphaned dbo_cust instance that blocks re-enabling (22926).
    cur.execute("""IF OBJECT_ID('dbo.cust') IS NOT NULL
                   BEGIN
                     IF EXISTS (SELECT 1 FROM cdc.change_tables ct
                                JOIN sys.tables t ON t.object_id = ct.source_object_id
                                WHERE t.name = 'cust')
                       EXEC sys.sp_cdc_disable_table @source_schema='dbo', @source_name='cust',
                            @capture_instance='dbo_cust';
                     DROP TABLE dbo.cust;
                   END""")
    cur.execute("CREATE TABLE dbo.cust (id INT PRIMARY KEY, name NVARCHAR(50), region NVARCHAR(50))")
    cur.execute("""EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='cust',
                        @role_name=NULL, @supports_net_changes=1""")

    # --- changes (bound values), incl. an intermediate update (b -> b2 -> b3) CDC retains ---
    cur.executemany("INSERT INTO dbo.cust (id, name, region) VALUES (?, ?, ?)",
                    [(1, "a", "X"), (2, "b", "X"), (3, "c", "Y")])
    cur.execute("UPDATE dbo.cust SET name = ? WHERE id = ?", ("b2", 2))
    cur.execute("UPDATE dbo.cust SET name = ? WHERE id = ?", ("b3", 2))
    cur.execute("DELETE FROM dbo.cust WHERE id = ?", (3,))
    cur.execute("INSERT INTO dbo.cust (id, name, region) VALUES (?, ?, ?)", (4, "d", "W"))

    # --- the column-collection + DuckLake history replica ---
    cc = ColumnCollection("dbo", "cust", [Col(n, t, dialect="sqlserver") for n, t in SRC_COLS],
                          key=KEY, dialect="sqlserver")
    driver = CDCDriver(KEY)
    base = "/tmp/cdc_history"
    shutil.rmtree(base, ignore_errors=True)
    os.makedirs(base)
    eng = create_engine(f"sqlite:///{base}/cat.sqlite")
    dl.create_catalog(eng)
    w = dl.DuckLakeWriter(eng, dl.DUCKLAKE_METADATA)
    w.init_catalog(data_path=f"{base}/data")
    cc.record_in_ducklake(w)
    rep = dl.HistoryReplica(w, "cust", KEY)

    # --- drive the capture scan directly (no Agent), then replay through the seam ---
    sa_eng = create_engine(URL.create("mssql+pyodbc", query={"odbc_connect": MSSQL}))
    # the capture-instance name is a bound value; the function name is an identifier (can't bind)
    count_sql = ("DECLARE @f binary(10)=sys.fn_cdc_get_min_lsn(?), "
                 "@t binary(10)=sys.fn_cdc_get_max_lsn(); "
                 "SELECT COUNT(*) FROM cdc.fn_cdc_get_all_changes_dbo_cust(@f,@t,N'all')")
    captured = 0
    for _ in range(10):
        try:
            cur.execute("EXEC sys.sp_cdc_scan")
        except pyodbc.Error as e:
            logger.warning("sp_cdc_scan failed: {error}", error=e)
        captured = cur.execute(count_sql, ["dbo_cust"]).fetchone()[0]
        if captured >= 7:   # 3 inserts + 2 updates + 1 delete + 1 insert
            break
        time.sleep(3)
    if not captured:
        logger.error("no CDC changes captured (sp_cdc_scan produced nothing)")
        sa_eng.dispose(); eng.dispose(); src.close()
        return
    with sa_eng.connect() as conn:
        wm = cc.sync(conn, None, driver, rep)
    sa_eng.dispose()
    logger.info("captured {n} CDC changes; replayed to watermark LSN {lsn}",
                n=captured, lsn=wm.hex() if wm else wm)
    eng.dispose()

    # --- verify: current state mirrors source, and intermediate b2 survives ---
    sql_state = {r[0]: r[1] for r in cur.execute("SELECT id, name FROM dbo.cust")}
    with dl.lake_reader(f"sqlite:{base}/cat.sqlite", f"{base}/data") as conn:
        lake_state = {r[0]: r[1] for r in
                      conn.execute(text("SELECT id, name FROM lake.cust")).fetchall()}
        # Address history by the logical clock — the snapshot VERSION, not the wallclock. CDC
        # commits within the same second share a snapshot_time, so AT(TIMESTAMP) can't separate
        # them; each is still a distinct snapshot_id. (Schema-only snapshots before cust exists
        # raise and are skipped.)
        sids = [s[0] for s in conn.execute(text(
            "SELECT snapshot_id FROM lake.snapshots() ORDER BY snapshot_id")).fetchall()]
        id2_versions = []
        for sid in sids:
            try:
                rows = conn.execute(text(
                    f"SELECT name FROM lake.cust AT (VERSION => {int(sid)}) WHERE id = 2")).fetchall()
                id2_versions += [n for (n,) in rows]
            except Exception:
                pass  # snapshot predates the cust table
    logger.info("SQL Server now : {state}", state=sql_state)
    logger.info("DuckLake now   : {state}", state=lake_state)
    logger.info("mirrors source : {ok}", ok=sql_state == lake_state)
    logger.info("id=2 across snapshot versions (logical-clock addressed): {versions}",
                versions=id2_versions)
    logger.info("intermediate b2 retained: {ok}", ok="b2" in id2_versions)

    cur.execute("EXEC sys.sp_cdc_disable_table @source_schema='dbo', @source_name='cust', "
                "@capture_instance='dbo_cust'")
    cur.execute("IF OBJECT_ID('dbo.cust') IS NOT NULL DROP TABLE dbo.cust")
    src.close()


if __name__ == "__main__":
    main()
