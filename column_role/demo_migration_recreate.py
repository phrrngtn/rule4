"""Migration that emits DROP+CREATE on a detected drop-recreate (the object_id LT in action).

demo_migration_capture.py shows in-place evolution (ALTER). This shows the *other* branch:
when the object was dropped and recreated between captures, the two revisions carry different
object_ids, and migration_ddl emits DROP TABLE + CREATE TABLE instead of an ALTER changeset —
a distinction a column *set* alone can't make (both look like "some columns changed"). The
object_id identity is what tells evolve from replace.

Live vs gfe (Kerberos). Run from column_role/:  uv run python demo_migration_recreate.py
"""
import datetime as dt
import os
import shutil

import pyodbc
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from column_collection import ColumnCollection
from migration import migration_ddl
from registry import Registry, capture

MSSQL = ("DRIVER={ODBC Driver 18 for SQL Server};SERVER=gfe.phrrngtn.arpa;DATABASE=rule4_test;"
         "Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate=yes")
SERVER, DB = "gfe", "rule4_test"
T1, T2 = dt.datetime(2026, 6, 30, 10), dt.datetime(2026, 6, 30, 11)


def main():
    src = pyodbc.connect(MSSQL, autocommit=True, timeout=15)
    cur = src.cursor()
    saeng = create_engine(URL.create("mssql+pyodbc", query={"odbc_connect": MSSQL}))
    sconn = saeng.connect()
    base = "/tmp/mig_recreate"
    shutil.rmtree(base, ignore_errors=True)
    os.makedirs(base)
    reg = Registry(f"{base}/cat.sqlite", f"{base}/data")

    # T1: create widget v1, full capture
    cur.execute("IF OBJECT_ID('dbo.widget') IS NOT NULL DROP TABLE dbo.widget")
    cur.execute("CREATE TABLE dbo.widget (id INT, name NVARCHAR(50))")
    reg.record(capture(sconn, "sqlserver", SERVER, DB, T1), T1)

    # T2: DROP + CREATE (a different physical object -> new object_id), full capture
    cur.execute("DROP TABLE dbo.widget")
    cur.execute("CREATE TABLE dbo.widget (id INT, sku NVARCHAR(20), qty INT)")
    reg.record(capture(sconn, "sqlserver", SERVER, DB, T2), T2)

    cc1 = ColumnCollection.from_column_role(reg, SERVER, DB, "widget", T1, schema="dbo", key="id")
    cc2 = ColumnCollection.from_column_role(reg, SERVER, DB, "widget", T2, schema="dbo", key="id")
    logger.info("object_id: T1={a} T2={b} (differ -> recreate: {d})",
                a=cc1.object_id, b=cc2.object_id, d=cc1.object_id != cc2.object_id)
    logger.info("migration T1 -> T2 (object_id changed => DROP+CREATE, not ALTER):")
    for stmt in migration_ddl(cc1, cc2):
        logger.info("  {stmt}", stmt=stmt)

    cur.execute("IF OBJECT_ID('dbo.widget') IS NOT NULL DROP TABLE dbo.widget")
    sconn.close()
    saeng.dispose()
    reg.dispose()
    src.close()


if __name__ == "__main__":
    main()
