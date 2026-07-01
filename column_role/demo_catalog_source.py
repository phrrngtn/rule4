"""catalog_source prototype: config-as-data drives multi-dialect catalog scraping.

Seed the catalog_source meta-registry (the *curated* essence->catalog-tables mapping, as data)
for the 'column' essence; then a *generic* generator emits the dialect-specific projection from
that data — no per-dialect code. We generate + run it against a live SQLite source and a live
SQL Server source: one generator, one registry, two working scrapes. The generated SQL is
printed so you can see the codegen output for each dialect.

Live vs gfe for the SQL Server half. Run from column_role/:  uv run python demo_catalog_source.py
"""
import os
import shutil

from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

import catalog_meta as cm

MSSQL = ("DRIVER={ODBC Driver 18 for SQL Server};SERVER=gfe.phrrngtn.arpa;DATABASE=rule4_test;"
         "Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate=yes")


def main():
    base = "/tmp/catsrc"
    shutil.rmtree(base, ignore_errors=True)
    os.makedirs(base)

    # --- the registry: config-as-data (curated once, per dialect/essence) ---
    reg = create_engine(f"sqlite:///{base}/registry.sqlite")
    with reg.begin() as rc:
        cm.create_registry(rc)
        cm.seed(rc)

    # --- SQLite source (self-contained) ---
    ssrc = create_engine(f"sqlite:///{base}/src.sqlite")
    with ssrc.begin() as c:
        c.execute(text("CREATE TABLE person (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"))
        c.execute(text("CREATE TABLE org (id INTEGER, title TEXT)"))
    with reg.connect() as rc:
        gen_sqlite = cm.generate_projection(rc, "sqlite", "column")
    logger.info("GENERATED (sqlite):  {sql}", sql=gen_sqlite)
    with ssrc.connect() as c:
        for row in c.execute(text(gen_sqlite)):
            logger.info("  sqlite column-essence: {r}", r=tuple(row))

    # --- SQL Server source (live gfe) ---
    with reg.connect() as rc:
        gen_mssql = cm.generate_projection(rc, "sqlserver", "column")
    logger.info("GENERATED (sqlserver): {sql}", sql=gen_mssql)
    dest = create_engine(URL.create("mssql+pyodbc", query={"odbc_connect": MSSQL}))
    with dest.begin() as c:
        c.execute(text("IF OBJECT_ID('dbo.cat_demo') IS NOT NULL DROP TABLE dbo.cat_demo"))
        c.execute(text("CREATE TABLE dbo.cat_demo (id INT PRIMARY KEY, label NVARCHAR(50), qty MONEY)"))
    with dest.connect() as c:
        rows = [tuple(r) for r in c.execute(text(gen_mssql)) if r.object_name == "cat_demo"]
    for r in rows:
        logger.info("  sqlserver column-essence (cat_demo): {r}", r=r)
    with dest.begin() as c:
        c.execute(text("IF OBJECT_ID('dbo.cat_demo') IS NOT NULL DROP TABLE dbo.cat_demo"))
    dest.dispose()

    # --- third dialect from the same registry, generated (not run — no duckdb source here) ---
    with reg.connect() as rc:
        logger.info("GENERATED (duckdb):  {sql}", sql=cm.generate_projection(rc, "duckdb", "column"))
        sig = rc.execute(text("SELECT dialect, change_signal FROM catalog_source WHERE essence='column'")).all()
    logger.info("change_signal per dialect (the cheap-tailing hook): {sig}", sig=[tuple(s) for s in sig])


if __name__ == "__main__":
    main()
