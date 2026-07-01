"""HWM-pruned Read 2: full-sample only the objects the identity probe shows changed.

The two-read scraper's payoff. After a baseline full capture (Read 2), poll the cheap identity
probe (Read 1); dirty_objects diffs it against the last full capture by object_id
(clock-independent) to find what's new or recreated; capture(only=dirty) then transports the
wide column detail for *only* those objects. Wire cost scales with what changed, not with the
schema size.

Live vs gfe. Run from column_role/:  uv run python demo_pruned_capture.py
"""
import datetime as dt
import os
import shutil

import pyodbc
from loguru import logger

from registry import Registry, capture, capture_identity

MSSQL = ("DRIVER={ODBC Driver 18 for SQL Server};SERVER=gfe.phrrngtn.arpa;DATABASE=rule4_test;"
         "Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate=yes")
SERVER, DB = "gfe", "rule4_test"
T1, T2 = dt.datetime(2026, 6, 30, 10), dt.datetime(2026, 6, 30, 11)
MINE = ("prune_a", "prune_b", "prune_c")


def drop_all(cur):
    for t in MINE:
        cur.execute(f"IF OBJECT_ID('dbo.{t}') IS NOT NULL DROP TABLE dbo.{t}")


def main():
    src = pyodbc.connect(MSSQL, autocommit=True, timeout=15)
    cur = src.cursor()
    base = "/tmp/pruned_capture"
    shutil.rmtree(base, ignore_errors=True)
    os.makedirs(base)
    reg = Registry(f"{base}/cat.sqlite", f"{base}/data")

    # baseline: prune_a, prune_b exist; full capture (Read 2)
    drop_all(cur)
    cur.execute("CREATE TABLE dbo.prune_a (id INT, name NVARCHAR(50))")
    cur.execute("CREATE TABLE dbo.prune_b (id INT, name NVARCHAR(50))")
    reg.record(capture(cur, "sqlserver", SERVER, DB, T1), T1)

    # change: add prune_c (new); DROP+CREATE prune_b (new object_id); prune_a untouched
    cur.execute("CREATE TABLE dbo.prune_c (id INT)")
    cur.execute("DROP TABLE dbo.prune_b")
    cur.execute("CREATE TABLE dbo.prune_b (id INT, sku NVARCHAR(20))")

    # Read 1: cheap identity probe -> dirty set (clock-independent, by object_id)
    reg.record_identity(capture_identity(cur, "sqlserver", SERVER, DB, T2), T2)
    dirty = [d for d in reg.dirty_objects(SERVER, DB) if d in MINE]
    logger.info("dirty_objects (mine): {dirty}  (expect prune_b recreated + prune_c new; not prune_a)",
                dirty=dirty)

    # Read 2, pruned: full column detail for ONLY the dirty objects
    pruned = capture(cur, "sqlserver", SERVER, DB, T2, only=dirty)
    sampled = sorted({r[4] for r in pruned})   # object_name is index 4 (after 3 context + schema)
    logger.info("pruned Read 2 sampled objects: {sampled}", sampled=sampled)
    logger.info("prune_a re-sampled? {a}  (should be False — unchanged, wire saved)",
                a="prune_a" in sampled)

    drop_all(cur)
    reg.dispose()
    src.close()


if __name__ == "__main__":
    main()
