"""More essences + cheap tailing, all from the same registry + generic engine.

The catalog_source registry now carries index / foreign_key / primary_key essences alongside
column (as data in catalog_seed.json). The *same* generate_projection engine produces each —
no per-essence code. And any essence whose change_signal is set (the parent object's
modify_date on SQL Server) can be *tailed*: generate_projection(..., tail=True) adds
``change_signal > :hwm``, so "what changed since last scan" works for indexes and FKs too, not
just columns.

Live vs gfe for the SQL Server half. Run from column_role/:  uv run python demo_catalog_essences.py
"""
import os
import shutil

from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

import catalog_meta as cm

MSSQL = ("DRIVER={ODBC Driver 18 for SQL Server};SERVER=gfe.phrrngtn.arpa;DATABASE=rule4_test;"
         "Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate=yes")
ESSENCES = ("primary_key", "index", "foreign_key")


def main():
    base = "/tmp/cat_essences"
    shutil.rmtree(base, ignore_errors=True)
    os.makedirs(base)
    reg = create_engine(f"sqlite:///{base}/registry.sqlite")
    with reg.begin() as rc:
        cm.create_registry(rc)
        cm.load(rc)

    # --- SQLite source: a schema with a PK, an explicit index, and a FK ---
    ssrc = create_engine(f"sqlite:///{base}/src.sqlite")
    with ssrc.begin() as c:
        c.execute(text("PRAGMA foreign_keys=ON"))
        c.execute(text("CREATE TABLE dept (id INTEGER PRIMARY KEY, name TEXT)"))
        c.execute(text("CREATE TABLE emp (id INTEGER PRIMARY KEY, name TEXT, "
                       "dept_id INTEGER REFERENCES dept(id))"))
        c.execute(text("CREATE INDEX ix_emp_name ON emp(name)"))
    for essence in ESSENCES:
        with reg.connect() as rc:
            stmt = cm.generate_projection(rc, "sqlite", essence)
        with ssrc.connect() as c:
            rows = [tuple(r) for r in c.execute(stmt)]
        logger.info("sqlite {e}: {rows}", e=essence, rows=rows)

    # --- SQL Server source (live gfe): same essences, generated from the same registry ---
    dest = create_engine(URL.create("mssql+pyodbc", query={"odbc_connect": MSSQL}))
    with dest.begin() as c:
        for t in ("ess_emp", "ess_dept"):
            c.execute(text(f"IF OBJECT_ID('dbo.{t}') IS NOT NULL DROP TABLE dbo.{t}"))
        t0 = c.execute(text("SELECT SYSUTCDATETIME()")).scalar()   # tailing watermark: before we build
        c.execute(text("CREATE TABLE dbo.ess_dept (id INT CONSTRAINT pk_ess_dept PRIMARY KEY, name NVARCHAR(50))"))
        c.execute(text("CREATE TABLE dbo.ess_emp (id INT CONSTRAINT pk_ess_emp PRIMARY KEY, name NVARCHAR(50), "
                       "dept_id INT CONSTRAINT fk_ess_emp REFERENCES dbo.ess_dept(id))"))
        c.execute(text("CREATE INDEX ix_ess_emp_name ON dbo.ess_emp(name)"))
    mine = {"ess_emp", "ess_dept"}
    for essence in ESSENCES:
        with reg.connect() as rc:
            stmt = cm.generate_projection(rc, "sqlserver", essence)
        with dest.connect() as c:
            rows = [tuple(r) for r in c.execute(stmt) if r.object_name in mine]
        logger.info("sqlserver {e}: {rows}", e=essence, rows=rows)

    # --- cheap tailing of a non-column essence: indexes changed since t0 ---
    with reg.connect() as rc:
        tail_stmt = cm.generate_projection(rc, "sqlserver", "index", tail=True)
    logger.info("GENERATED (sqlserver index, tail): {sql}", sql=str(tail_stmt))
    with dest.connect() as c:
        since_t0 = sorted({r.object_name for r in c.execute(tail_stmt, {"hwm": t0})})
        since_now = sorted({r.object_name for r in c.execute(
            tail_stmt, {"hwm": c.execute(text("SELECT SYSUTCDATETIME()")).scalar()})})
    logger.info("index essence tailed since t0 (includes our new table): {a}", a=[o for o in since_t0 if o in mine])
    logger.info("index essence tailed since now (should be empty): {b}", b=since_now)

    with dest.begin() as c:
        for t in ("ess_emp", "ess_dept"):
            c.execute(text(f"IF OBJECT_ID('dbo.{t}') IS NOT NULL DROP TABLE dbo.{t}"))
    dest.dispose()


if __name__ == "__main__":
    main()
