"""catalog_source — the meta-schema for catalog scraping, as DATA.

Records, per (dialect, essence), how to produce that essence from the *native* catalog: the
FROM (the joined catalog tables), the per-attribute expression mapping, an optional WHERE, and
the ``change_signal`` column (the modify_date/version that drives cheap tailing). The mappings
are **data** — they live in ``catalog_seed.json`` and are loaded into the registry tables
(``catalog_source`` / ``catalog_attr``); this module holds only the generic engine, no metadata
literals. Adding an essence for a dialect is an edit to the data file, not to code.

``generate_projection`` *retrieves* the fragments from the registry (SA Core) and assembles the
projection as a **SQLAlchemy expression-language ``select()``** — ``literal_column`` for the
attr expressions, ``text`` for the dialect-specific FROM/WHERE fragments, ``bindparam`` for the
tailing watermark. No string concatenation; the result is a real SA construct, executed on a SA
connection to the source. The registry lives in SQLite for the prototype; its natural home is
the same DuckLake time-series it drives (schema-as-data, self-hosting).

The split the design predicts: **curatorial** = the seed data (deciding an essence's
catalog-table mapping); **deterministic** = this engine.
"""
import glob
import json
import os

from sqlalchemy import (Column, Integer, MetaData, String, Table, and_, bindparam,
                        literal_column, select, text)

_MD = MetaData()
CATALOG_SOURCE = Table(
    "catalog_source", _MD,
    Column("dialect", String), Column("essence", String),
    Column("from_sql", String), Column("where_sql", String), Column("change_signal", String))
CATALOG_ATTR = Table(
    "catalog_attr", _MD,
    Column("dialect", String), Column("essence", String),
    Column("ord", Integer), Column("attr", String), Column("expr", String))

_SEED_GLOB = os.path.join(os.path.dirname(__file__), "catalog_seed*.json")


def create_registry(conn):
    """Create the catalog_source / catalog_attr tables (SA Core, dialect-independent)."""
    _MD.create_all(conn)


def load(conn, paths=None):
    """Populate the registry from the seed data files — the metadata is DATA, not code. Default
    is every ``catalog_seed*.json`` beside this module (a base file plus per-dialect additions),
    so **adding a dialect is a new data file, no code change**. Bulk SA Core inserts."""
    for path in sorted(paths or glob.glob(_SEED_GLOB)):
        with open(path) as fh:
            data = json.load(fh)
        for tbl, key in ((CATALOG_SOURCE, "catalog_source"), (CATALOG_ATTR, "catalog_attr")):
            if data.get(key):
                conn.execute(tbl.insert(), data[key])


def generate_projection(conn, dialect, essence, *, tail=False):
    """Assemble the dialect-specific projection for one essence as a SA ``select()``, driven by
    the fragments *retrieved from the registry*. ``literal_column``/``text`` carry the
    dialect-specific bits; ``tail=True`` adds ``{change_signal} > :hwm`` (a bound param) — run it
    with ``.execute(stmt, {"hwm": watermark})``. Raises if the essence has no change_signal."""
    src = conn.execute(
        select(CATALOG_SOURCE.c.from_sql, CATALOG_SOURCE.c.where_sql, CATALOG_SOURCE.c.change_signal)
        .where(and_(CATALOG_SOURCE.c.dialect == dialect, CATALOG_SOURCE.c.essence == essence))).one()
    attrs = conn.execute(
        select(CATALOG_ATTR.c.expr, CATALOG_ATTR.c.attr)
        .where(and_(CATALOG_ATTR.c.dialect == dialect, CATALOG_ATTR.c.essence == essence))
        .order_by(CATALOG_ATTR.c.ord)).all()
    stmt = select(*[literal_column(expr).label(attr) for expr, attr in attrs]).select_from(
        text(src.from_sql))
    if src.where_sql:
        stmt = stmt.where(text(src.where_sql))
    if tail:
        if not src.change_signal:
            raise ValueError(f"essence {essence!r} for {dialect!r} has no change_signal — cannot tail")
        stmt = stmt.where(literal_column(src.change_signal) > bindparam("hwm"))
    return stmt
