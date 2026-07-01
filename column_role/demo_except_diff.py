"""EXCEPT-diff tier: signal-free change detection for a source with no change_signal and no PK.

SQLite has no per-object modify_date, so its essences can't cheap-tail — they use the EXCEPT
tier. Scrape the column essence, change the source (add a column, add a table, drop a table),
scrape again, and diff the two full scrapes set-based in DuckDB. EXCEPT keys on the whole row,
so a changed data_type or a new/dropped column surfaces with no PK to declare.

Run from column_role/:  uv run python demo_except_diff.py
"""
import os
import shutil

from loguru import logger
from sqlalchemy import create_engine, text

import catalog_diff as cd
import catalog_meta as cm

BASE = "/tmp/except_diff"


def scrape(reg, src, dialect, essence):
    with reg.connect() as rc:
        stmt = cm.generate_projection(rc, dialect, essence)
    cols = [c.name for c in stmt.selected_columns]
    with src.connect() as c:
        return [tuple(r) for r in c.execute(stmt)], cols


def main():
    shutil.rmtree(BASE, ignore_errors=True)
    os.makedirs(BASE)
    reg = create_engine(f"sqlite:///{BASE}/registry.sqlite")
    with reg.begin() as rc:
        cm.create_registry(rc)
        cm.load(rc)
    src = create_engine(f"sqlite:///{BASE}/src.sqlite")

    # T1 baseline
    with src.begin() as c:
        c.execute(text("CREATE TABLE person (id INTEGER, name TEXT)"))
        c.execute(text("CREATE TABLE gone (x TEXT)"))
    prev, cols = scrape(reg, src, "sqlite", "column")
    logger.info("central catalog (T1): {n} rows", n=len(prev))

    # the source drifts — no modify_date, no PK to help us
    with src.begin() as c:
        c.execute(text("ALTER TABLE person ADD COLUMN age INTEGER"))   # new column
        c.execute(text("CREATE TABLE org (title TEXT)"))                # new table
        c.execute(text("DROP TABLE gone"))                             # dropped table
    cur, _ = scrape(reg, src, "sqlite", "column")

    diff = cd.except_diff(cur, prev, cols)
    logger.info("columns: {cols}", cols=cols)
    logger.info("ADDED (new-or-changed, source EXCEPT central):")
    for r in diff["added"]:
        logger.info("  + {r}", r=r)
    logger.info("REMOVED (gone-or-changed, central EXCEPT source):")
    for r in diff["removed"]:
        logger.info("  - {r}", r=r)


if __name__ == "__main__":
    main()
