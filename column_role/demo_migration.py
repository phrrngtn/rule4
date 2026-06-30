"""Schema migration DDL from a column-collection changeset.

Three revisions of one table (in practice each is ``ColumnCollection.from_column_role(
registry, server, db, table, when=T_n)`` off the schema time-series; here built by hand to
stay self-contained). The changeset between any two is interpreted as ALTER TABLE DDL —
forward (n-1 -> n), rollback (n -> n-3), and rendered for several target dialects.

Run from column_role/:  uv run python demo_migration.py
"""
from loguru import logger

from column_collection import Col, ColumnCollection


def rev(*cols):
    return ColumnCollection("dbo", "cust", [Col(n, t, dialect="sqlserver") for n, t in cols],
                            key="id", dialect="sqlserver")


# r1 (n-3) -> r2 (n-1) -> r3 (n): widen, add, retype, drop across the series
r1 = rev(("id", "int"), ("name", "nvarchar(50)"), ("region", "nvarchar(50)"))
r2 = rev(("id", "int"), ("name", "nvarchar(100)"), ("region", "nvarchar(50)"),
         ("signup_date", "date"))
r3 = rev(("id", "int"), ("name", "nvarchar(100)"), ("signup_date", "datetime2"),
         ("loyalty_tier", "int"))


def show(title, ddl):
    body = "\n".join("  " + s for s in ddl) or "  (no change)"
    logger.info("{title}\n{body}", title=title, body=body)


# forward, step by step
show("r1 -> r2 (forward):", r1.migration_to(r2))
show("r2 -> r3 (forward):", r2.migration_to(r3))

# rollback across two revisions: n -> n-3
show("r3 -> r1 (rollback, n -> n-3):", r3.migration_to(r1))

# the same r2 -> r3 changeset rendered for other engines (note SQLite can't retype in place)
for dialect in ("postgresql", "duckdb", "sqlite"):
    show(f"r2 -> r3 rendered for {dialect}:", r2.migration_to(r3, dialect=dialect))
