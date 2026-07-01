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
                        column as sa_column, literal_column, select, table as sa_table, text)

_MD = MetaData()
CATALOG_SOURCE = Table(
    "catalog_source", _MD,
    Column("dialect", String), Column("essence", String),
    Column("from_sql", String), Column("where_sql", String), Column("change_signal", String))
CATALOG_ATTR = Table(
    "catalog_attr", _MD,
    Column("dialect", String), Column("essence", String),
    Column("ord", Integer), Column("attr", String), Column("expr", String))

# The JOIN as *relational data* (not a stored SQL blob): the catalog tables an essence draws
# from (aliases + exprs, one root), and the key-column edges between them. The FROM/JOIN is
# GENERATED from a query against these -- identifiers only, no embedded SQL.
CATALOG_TABLE = Table(
    "catalog_table", _MD,
    Column("dialect", String), Column("essence", String),
    Column("alias", String), Column("expr", String), Column("is_root", Integer))
CATALOG_JOIN = Table(
    "catalog_join", _MD,
    Column("dialect", String), Column("essence", String),
    Column("alias", String), Column("to_alias", String),
    Column("child_col", String), Column("parent_col", String))

_SEED_GLOB = os.path.join(os.path.dirname(__file__), "catalog_seed*.json")


def create_registry(conn):
    """Create the catalog_source / catalog_attr tables (SA Core, dialect-independent)."""
    _MD.create_all(conn)


def load(conn, paths=None):
    """Populate the registry from the seed data files — the metadata is DATA, not code. Default
    is every ``catalog_seed*.json`` beside this module (a base file plus per-dialect additions),
    so **adding a dialect is a new data file, no code change**. Bulk SA Core inserts."""
    tables = ((CATALOG_SOURCE, "catalog_source"), (CATALOG_ATTR, "catalog_attr"),
              (CATALOG_TABLE, "catalog_table"), (CATALOG_JOIN, "catalog_join"))
    for path in sorted(paths or glob.glob(_SEED_GLOB)):
        with open(path) as fh:
            data = json.load(fh)
        for tbl, key in tables:
            if data.get(key):
                conn.execute(tbl.insert(), data[key])


def from_clause(conn, dialect, essence):
    """Build the FROM/JOIN for an essence from **join metadata in the registry** — the catalog
    tables (aliases + exprs, one root) and their key-column edges — as a **SQLAlchemy Core Join
    construct**, not a string. Each catalog table becomes a lightweight ``table().alias()`` and
    the edges become real ``==`` on-clauses (AND-ed for compound keys), walked from the root.
    Returns None when no join metadata is registered (caller falls back to
    ``catalog_source.from_sql`` for essences whose joins aren't decomposable — LATERAL unnest,
    correlated TVFs)."""
    rows = conn.execute(
        select(CATALOG_TABLE.c.alias, CATALOG_TABLE.c.expr, CATALOG_TABLE.c.is_root)
        .where(and_(CATALOG_TABLE.c.dialect == dialect, CATALOG_TABLE.c.essence == essence))).all()
    if not rows:
        return None
    edges = {}
    for j in conn.execute(select(CATALOG_JOIN.c.alias, CATALOG_JOIN.c.to_alias,
                                 CATALOG_JOIN.c.child_col, CATALOG_JOIN.c.parent_col)
                          .where(and_(CATALOG_JOIN.c.dialect == dialect,
                                      CATALOG_JOIN.c.essence == essence))):
        edges.setdefault(j.alias, []).append((j.child_col, j.to_alias, j.parent_col))
    # columns each alias participates in (child on its side, parent on the target's) -> declare
    # them on the lightweight tables so the on-clauses can reference them.
    used = {}
    for a, elist in edges.items():
        for (cc, to, pc) in elist:
            used.setdefault(a, set()).add(cc)
            used.setdefault(to, set()).add(pc)

    def make(alias, expr):
        schema, name = expr.rsplit(".", 1) if "." in expr else (None, expr)
        return sa_table(name, *[sa_column(c) for c in sorted(used.get(alias, ()))],
                        schema=schema).alias(alias)

    tbls = {a: make(a, e) for (a, e, _r) in rows}
    root = next(a for (a, _e, r) in rows if r)
    ordered, placed, remaining = [root], {root}, [a for (a, _e, _r) in rows if a != root]
    while remaining:
        nxt = [a for a in remaining if {to for (_c, to, _p) in edges.get(a, [])} <= placed]
        if not nxt:
            raise ValueError(f"unresolved join order for {dialect}/{essence}: {remaining}")
        for a in nxt:
            ordered.append(a); placed.add(a); remaining.remove(a)
    frm = tbls[root]
    for a in ordered[1:]:
        frm = frm.join(tbls[a], and_(*[tbls[a].c[cc] == tbls[to].c[pc]
                                       for (cc, to, pc) in edges[a]]))
    return frm


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
    # a SA Join construct built from join metadata if present, else the stored from_sql (text)
    from_obj = from_clause(conn, dialect, essence)
    stmt = select(*[literal_column(expr).label(attr) for expr, attr in attrs]).select_from(
        from_obj if from_obj is not None else text(src.from_sql))
    if src.where_sql:
        stmt = stmt.where(text(src.where_sql))
    if tail:
        if not src.change_signal:
            raise ValueError(f"essence {essence!r} for {dialect!r} has no change_signal — cannot tail")
        stmt = stmt.where(literal_column(src.change_signal) > bindparam("hwm"))
    return stmt
