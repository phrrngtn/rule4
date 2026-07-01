"""Drop-recreate detection via the object_id logical clock (the two-read schema scraper).

Read 1 is the cheap identity probe (object_id + create/modify_date per object); it's the LT
change-feed. Ordering the probe time-series by transaction-time (sample_time) and diffing the
**object_id logical clock** distinguishes the three schema events a column *set* alone can't:

  * modify_date changes, object_id stays  -> altered   (in-place; a migration ALTER)
  * object_id changes                     -> recreated  (drop+create; a migration DROP+CREATE)
  * first sighting                        -> initial

This is the schema-side twin of CT/CDC: a logical clock detects what a wallclock can't (and
isn't fooled by clock skew, since object_id is a true identity, not a time).

Live against gfe (Kerberos). Run from column_role/:  uv run python demo_drop_recreate.py
"""
import datetime as dt

import pyodbc
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from registry import Registry, capture_identity

MSSQL = ("DRIVER={ODBC Driver 18 for SQL Server};SERVER=gfe.phrrngtn.arpa;DATABASE=rule4_test;"
         "Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate=yes")
SERVER, DB = "gfe", "rule4_test"
T1, T2, T3 = (dt.datetime(2026, 6, 30, 10), dt.datetime(2026, 6, 30, 11), dt.datetime(2026, 6, 30, 12))


def probe(reg, sconn, when):
    reg.record_identity(capture_identity(sconn, "sqlserver", SERVER, DB, when), when)


def object_id(cur):
    return cur.execute("SELECT object_id FROM sys.objects WHERE name = ?", ("widget",)).fetchone()[0]


def main():
    src = pyodbc.connect(MSSQL, autocommit=True, timeout=15)
    cur = src.cursor()
    saeng = create_engine(URL.create("mssql+pyodbc", query={"odbc_connect": MSSQL}))
    sconn = saeng.connect()
    import shutil
    import os
    base = "/tmp/drop_recreate"
    shutil.rmtree(base, ignore_errors=True)
    os.makedirs(base)
    reg = Registry(f"{base}/cat.sqlite", f"{base}/data")

    # T1: create the object, probe
    cur.execute("IF OBJECT_ID('dbo.widget') IS NOT NULL DROP TABLE dbo.widget")
    cur.execute("CREATE TABLE dbo.widget (id INT PRIMARY KEY, name NVARCHAR(50))")
    oid1 = object_id(cur)
    probe(reg, sconn, T1)

    # T2: in-place ALTER (object_id unchanged, modify_date advances)
    cur.execute("ALTER TABLE dbo.widget ADD price MONEY")
    oid2 = object_id(cur)
    probe(reg, sconn, T2)

    # T3: DROP + CREATE (new object_id — same name, different physical object)
    cur.execute("DROP TABLE dbo.widget")
    cur.execute("CREATE TABLE dbo.widget (id INT PRIMARY KEY)")
    oid3 = object_id(cur)
    probe(reg, sconn, T3)

    logger.info("object_id over time: T1={a} T2={b} (== T1: {same}) T3={c} (!= T2: {diff})",
                a=oid1, b=oid2, same=oid1 == oid2, c=oid3, diff=oid2 != oid3)
    logger.info("Read-2 pruning HWM (max modify_date): {hwm}", hwm=reg.hwm(SERVER, DB))
    logger.info("--- schema_anomalies (order by TT, detect via object_id LT) ---")
    for sch, name, when, prev_oid, oid, event in reg.schema_anomalies(SERVER, DB):
        if name == "widget":
            logger.info("{when} {sch}.{name}: {event}  (object_id {prev} -> {cur})",
                        when=when, sch=sch, name=name, event=event, prev=prev_oid, cur=oid)

    cur.execute("IF OBJECT_ID('dbo.widget') IS NOT NULL DROP TABLE dbo.widget")
    sconn.close()
    saeng.dispose()
    reg.dispose()
    src.close()


if __name__ == "__main__":
    main()
