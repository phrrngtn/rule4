"""Schema migration from **real column_role captures** (the schema time-series, end to end).

Unlike ``demo_migration.py`` (hand-built revisions), this captures a live SQL Server table's
structure at two points in time into a DuckLake registry, builds a ColumnCollection from each
capture (``from_column_role(..., when=T_n)``), and diffs them into ALTER TABLE DDL. Then it
*validates* the result against the real server: it applies the generated forward DDL to a
fresh copy of the r1 table and re-captures it — the copy's schema must end up identical to r2,
proving the generated migration is both correct and executable.

The captured ``data_type`` is the base type name only (the registry keeps ``ty.name``, not
``max_length``/``precision``), so the changed columns here use length-free types
(int/bigint/date/datetime2/bit/uniqueidentifier) where the base name fully specifies the
type. Length-faithful DDL (nvarchar(50) vs (100)) would need the registry subset widened to
carry max_length/precision/scale — noted, not done here.

Integrated security — no credentials (Kerberos via Trusted_Connection); prereqs as in
examples/ct_history_replica.py.
Run from column_role/:  uv run python demo_migration_capture.py
"""
import datetime as dt
import os
import shutil

import pyodbc
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from column_collection import ColumnCollection
from migration import schema_diff
from registry import Registry, capture

MSSQL = ("DRIVER={ODBC Driver 18 for SQL Server};SERVER=gfe.phrrngtn.arpa;DATABASE=rule4_test;"
         "Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate=yes")
SERVER, DB = "gfe", "rule4_test"
T1, T2, T3 = dt.datetime(2026, 6, 30, 10), dt.datetime(2026, 6, 30, 11), dt.datetime(2026, 6, 30, 12)

# r1 schema; r2 is reached by the ALTERs below (the "real" evolution we then re-derive)
R1 = [("id", "INT"), ("name", "NVARCHAR(100)"), ("score", "INT"),
      ("created", "DATE"), ("is_active", "BIT")]
TO_R2 = ["ALTER COLUMN score BIGINT", "ALTER COLUMN created DATETIME2",
         "DROP COLUMN is_active", "ADD ext_id UNIQUEIDENTIFIER"]


def create_r1(cur, table):
    cur.execute(f"IF OBJECT_ID('dbo.{table}') IS NOT NULL DROP TABLE dbo.{table}")
    cur.execute(f"CREATE TABLE dbo.{table} (" + ", ".join(f"[{n}] {t}" for n, t in R1) + ")")


def cols_of(reg, table, when):
    cc = ColumnCollection.from_column_role(reg, SERVER, DB, table, when, schema="dbo", key="id")
    return cc


def show(title, ddl):
    body = "\n".join("  " + s for s in ddl) or "  (no change)"
    logger.info("{title}\n{body}", title=title, body=body)


def main():
    src = pyodbc.connect(MSSQL, autocommit=True, timeout=15)
    cur = src.cursor()
    saeng = create_engine(URL.create("mssql+pyodbc", query={"odbc_connect": MSSQL}))
    sconn = saeng.connect()
    base = "/tmp/mig_capture"
    shutil.rmtree(base, ignore_errors=True)
    os.makedirs(base)
    reg = Registry(f"{base}/cat.sqlite", f"{base}/data")

    # T1: create r1 and capture the live schema
    create_r1(cur, "cust")
    reg.record(capture(sconn, "sqlserver", SERVER, DB, T1), T1)

    # T2: evolve the real table, capture again
    for alter in TO_R2:
        cur.execute(f"ALTER TABLE dbo.cust {alter}")
    reg.record(capture(sconn, "sqlserver", SERVER, DB, T2), T2)

    # the two revisions, straight from the captures
    cc1, cc2 = cols_of(reg, "cust", T1), cols_of(reg, "cust", T2)
    logger.info("r1 (captured): {cols}", cols=[(c.name, c.source_type) for c in cc1.columns])
    logger.info("r2 (captured): {cols}", cols=[(c.name, c.source_type) for c in cc2.columns])
    show("r1 -> r2 (forward, derived from captures):", cc1.migration_to(cc2))
    show("r2 -> r1 (rollback):", cc2.migration_to(cc1))

    # --- validate against the real server: apply generated forward DDL to a fresh r1 copy ---
    create_r1(cur, "cust_copy")
    cc1c = ColumnCollection("dbo", "cust_copy", cc1.columns, key="id", dialect="sqlserver")
    cc2c = ColumnCollection("dbo", "cust_copy", cc2.columns, key="id", dialect="sqlserver")
    generated = cc1c.migration_to(cc2c)
    for stmt in generated:
        cur.execute(stmt.rstrip(";"))
    reg.record(capture(sconn, "sqlserver", SERVER, DB, T3), T3)

    cc_copy = cols_of(reg, "cust_copy", T3)
    residual = schema_diff(cc_copy, cc2)   # copy-after-migration vs the real r2
    logger.info("applied {n} generated statements to dbo.cust_copy", n=len(generated))
    logger.info("copy-after-migration vs r2: {residual}",
                residual=residual or "IDENTICAL — generated DDL reproduced r2")

    cur.execute("IF OBJECT_ID('dbo.cust') IS NOT NULL DROP TABLE dbo.cust")
    cur.execute("IF OBJECT_ID('dbo.cust_copy') IS NOT NULL DROP TABLE dbo.cust_copy")
    sconn.close()
    saeng.dispose()
    reg.dispose()
    src.close()


if __name__ == "__main__":
    main()
