"""SQL Server Change Tracking -> DuckLake current-state replica (worked example).

The source-specific *consumer* half that sits on top of the generic
`ducklake_oob_writer.Replica`. It enables Change Tracking on a real SQL Server table,
does an initial snapshot load, makes some changes, polls `CHANGETABLE` for **net**
changes since a watermark, splits them into upserts/deletes, and feeds `Replica.apply` —
so the DuckLake table mirrors the source's current state while retaining full
transaction-time history.

The whole pattern is *poll-since-HWM -> apply -> advance the watermark*. Treating CDC the
same as CT (net changes only) means the same loop works for both — swap `CHANGETABLE`
for `cdc.fn_cdc_get_net_changes_*` and the rest is unchanged.

Boundary: this file is the SQL-Server-specific consumer. The generic apply/merge lives in
`ducklake_oob_writer` (`Replica`, `delete_rows`, `register_parquet`); nothing here reaches
into it beyond the public API.

Prerequisites:
  * the `sql2025` container running, the `rule4_test_mssql` ODBC DSN configured;
  * the SA password in the environment:
        export MSSQL_SA_PW=$(docker inspect sql2025 \
            --format '{{range .Config.Env}}{{println .}}{{end}}' | sed -n 's/^MSSQL_SA_PASSWORD=//p')
Run:  uv run python examples/ct_replica.py
"""
import datetime as dt
import os
import shutil

import pyodbc
from sqlalchemy import create_engine

import ducklake_oob_writer as dl

KEY = "id"
COLS = [("id", "int64"), ("name", "varchar"), ("region", "varchar")]


def main():
    pw = os.environ["MSSQL_SA_PW"]
    src = pyodbc.connect(f"DSN=rule4_test_mssql;UID=sa;PWD={pw}", autocommit=True, timeout=15)
    cur = src.cursor()

    # --- enable Change Tracking + a fresh CT-enabled table ---
    cur.execute("""IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_databases
                                  WHERE database_id = DB_ID('rule4_test'))
                   ALTER DATABASE rule4_test
                     SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)""")
    cur.execute("IF OBJECT_ID('dbo.cust') IS NOT NULL DROP TABLE dbo.cust")
    cur.execute("CREATE TABLE dbo.cust (id INT PRIMARY KEY, name NVARCHAR(50), region NVARCHAR(50))")
    cur.execute("ALTER TABLE dbo.cust ENABLE CHANGE_TRACKING")
    cur.execute("INSERT INTO dbo.cust VALUES (1,'a','X'),(2,'b','X'),(3,'c','Y')")

    # --- the DuckLake replica ---
    base = "/tmp/ct_replica"
    shutil.rmtree(base, ignore_errors=True)
    os.makedirs(base)
    eng = create_engine(f"sqlite:///{base}/cat.sqlite")
    dl.create_catalog(eng)
    w = dl.DuckLakeWriter(eng, dl.DUCKLAKE_METADATA)
    w.init_catalog(data_path=f"{base}/data")
    w.create_table("main", "cust", COLS)
    rep = dl.Replica(w, "cust", KEY)

    # --- initial snapshot load; the watermark is the current CT version ---
    watermark = cur.execute("SELECT CHANGE_TRACKING_CURRENT_VERSION()").fetchone()[0]
    base_rows = cur.execute("SELECT id, name, region FROM dbo.cust").fetchall()
    rep.apply(upserts=[{"id": r[0], "name": r[1], "region": r[2]} for r in base_rows],
              snapshot_time=dt.datetime(2026, 6, 29, 10))
    print(f"initial load: {len(base_rows)} rows, watermark CT version = {watermark}")

    # --- source changes (update / delete / insert) ---
    cur.execute("UPDATE dbo.cust SET name='b2', region='Z' WHERE id=2")
    cur.execute("DELETE FROM dbo.cust WHERE id=3")
    cur.execute("INSERT INTO dbo.cust VALUES (4,'d','W')")

    # --- poll CHANGETABLE for NET changes since the watermark, then apply ---
    new_version = cur.execute("SELECT CHANGE_TRACKING_CURRENT_VERSION()").fetchone()[0]
    changes = cur.execute(f"""
        SELECT ct.SYS_CHANGE_OPERATION AS op, ct.id, b.name, b.region
        FROM CHANGETABLE(CHANGES dbo.cust, {watermark}) ct
        LEFT JOIN dbo.cust b ON b.id = ct.id
        ORDER BY ct.id""").fetchall()
    print("poll: " + ", ".join(f"{c.op} id={c.id}" for c in changes))
    upserts = [{"id": c.id, "name": c.name, "region": c.region}
               for c in changes if c.op in ("I", "U")]
    deletes = [c.id for c in changes if c.op == "D"]
    res = rep.apply(upserts=upserts, deletes=deletes, snapshot_time=dt.datetime(2026, 6, 29, 11))
    watermark = new_version
    print(f"applied: {res}  (new watermark = {watermark})")

    # --- verify: DuckLake replica == SQL Server current state, and history is retained ---
    sql_state = {r[0]: (r[1], r[2]) for r in cur.execute("SELECT id, name, region FROM dbo.cust").fetchall()}
    with dl.attach_lake(f"sqlite:{base}/cat.sqlite", f"{base}/data") as c:
        lake_state = {r[0]: (r[1], r[2]) for r in
                      c.execute("SELECT id, name, region FROM lake.cust").fetchall()}
        asof = {r[0]: r[1] for r in c.execute(
            "SELECT id, name FROM lake.cust AT (TIMESTAMP => TIMESTAMP '2026-06-29 10:30') "
            "ORDER BY id").fetchall()}
    print(f"\nSQL Server now : {sql_state}")
    print(f"DuckLake now   : {lake_state}")
    print(f"mirrors source : {sql_state == lake_state}")
    print(f"history AT 10:30 (pre-change): {asof}")

    cur.execute("IF OBJECT_ID('dbo.cust') IS NOT NULL DROP TABLE dbo.cust")   # cleanup
    src.close()


if __name__ == "__main__":
    main()
