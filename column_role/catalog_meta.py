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
# each catalog table's identity key columns (DRY reference data, shared across essences) —
# the convention that lets join columns be INFERRED rather than recorded.
CATALOG_KEY = Table(
    "catalog_key", _MD,
    Column("dialect", String), Column("table_name", String),
    Column("ord", Integer), Column("col", String))
# join edges, oriented child (references) -> parent (referenced). The ON columns are INFERRED
# from the parent's identity key (same-named, the catalog convention); child_col/parent_col are
# NULL for those. They're filled only where the catalog breaks the convention (differently-named
# FK columns like kc.parent_object_id -> o.object_id) — exceptions as data.
CATALOG_JOIN = Table(
    "catalog_join", _MD,
    Column("dialect", String), Column("essence", String),
    Column("child", String), Column("parent", String),
    Column("child_col", String), Column("parent_col", String),
    Column("is_outer", Integer))   # 1 = LEFT OUTER (optional side), else INNER

_SEED_GLOB = os.path.join(os.path.dirname(__file__), "catalog_seed*.json")


def create_registry(conn):
    """Create the catalog_source / catalog_attr tables (SA Core, dialect-independent)."""
    _MD.create_all(conn)


def load(conn, paths=None):
    """Populate the registry from the seed data files — the metadata is DATA, not code. Default
    is every ``catalog_seed*.json`` beside this module (a base file plus per-dialect additions),
    so **adding a dialect is a new data file, no code change**. Bulk SA Core inserts."""
    tables = ((CATALOG_SOURCE, "catalog_source"), (CATALOG_ATTR, "catalog_attr"),
              (CATALOG_TABLE, "catalog_table"), (CATALOG_JOIN, "catalog_join"),
              (CATALOG_KEY, "catalog_key"))
    for path in sorted(paths or glob.glob(_SEED_GLOB)):
        with open(path) as fh:
            data = json.load(fh)
        for tbl, key in tables:
            batch = data.get(key)
            if batch:
                cols = set().union(*(row.keys() for row in batch))   # normalize optional keys
                conn.execute(tbl.insert(), [{c: row.get(c) for c in cols} for row in batch])


def from_clause(conn, dialect, essence):
    """Build the FROM/JOIN for an essence from **join metadata in the registry** as a
    **SQLAlchemy Core Join construct** (not a string). The edges (child -> parent) are essence
    data; the ON *columns* are **inferred from the parent's identity key** (``catalog_key``, the
    catalog convention that an FK column shares the referenced PK's name), and only overridden
    (``child_col``/``parent_col``) where the catalog breaks that convention. So ``catalog_join``
    is mostly pure graph structure. Returns None when no join metadata is registered (caller
    falls back to ``catalog_source.from_sql`` — LATERAL unnest, correlated TVFs)."""
    rows = conn.execute(
        select(CATALOG_TABLE.c.alias, CATALOG_TABLE.c.expr, CATALOG_TABLE.c.is_root)
        .where(and_(CATALOG_TABLE.c.dialect == dialect, CATALOG_TABLE.c.essence == essence))).all()
    if not rows:
        return None
    expr_of = {a: e for (a, e, _r) in rows}
    keys = {}   # table expr -> [identity columns], the DRY convention
    for k in conn.execute(select(CATALOG_KEY.c.table_name, CATALOG_KEY.c.col)
                          .where(CATALOG_KEY.c.dialect == dialect).order_by(CATALOG_KEY.c.ord)):
        keys.setdefault(k.table_name, []).append(k.col)
    # edge (child, parent) -> [(child_col, parent_col), …]: inferred from parent identity when
    # the recorded columns are NULL, else the explicit override pair(s).
    edge_cols, edge_outer = {}, {}
    for j in conn.execute(select(CATALOG_JOIN.c.child, CATALOG_JOIN.c.parent, CATALOG_JOIN.c.child_col,
                                 CATALOG_JOIN.c.parent_col, CATALOG_JOIN.c.is_outer)
                          .where(and_(CATALOG_JOIN.c.dialect == dialect,
                                      CATALOG_JOIN.c.essence == essence))):
        edge_outer[(j.child, j.parent)] = bool(j.is_outer)
        if j.child_col is None:
            edge_cols[(j.child, j.parent)] = [(c, c) for c in keys[expr_of[j.parent]]]
        else:
            edge_cols.setdefault((j.child, j.parent), []).append((j.child_col, j.parent_col))
    used = {}
    for (child, parent), pairs in edge_cols.items():
        for (cc, pc) in pairs:
            used.setdefault(child, set()).add(cc)
            used.setdefault(parent, set()).add(pc)

    def make(alias):
        expr = expr_of[alias]
        schema, name = expr.rsplit(".", 1) if "." in expr else (None, expr)
        return sa_table(name, *[sa_column(c) for c in sorted(used.get(alias, ()))],
                        schema=schema).alias(alias)

    tbls = {a: make(a) for a in expr_of}
    root = next(a for (a, _e, r) in rows if r)
    placed, ordered = {root}, []
    while len(placed) < len(expr_of):
        added = False
        for (child, parent), pairs in edge_cols.items():
            nc = (parent if child in placed and parent not in placed
                  else child if parent in placed and child not in placed else None)
            if nc is not None:
                on = and_(*[tbls[child].c[cc] == tbls[parent].c[pc] for (cc, pc) in pairs])
                ordered.append((nc, on, edge_outer[(child, parent)])); placed.add(nc); added = True
        if not added:
            raise ValueError(f"disconnected join graph for {dialect}/{essence}")
    frm = tbls[root]
    for (alias, on, outer) in ordered:
        frm = frm.join(tbls[alias], on, isouter=outer)
    return frm


# --- self-hosting: the registry's durable home is DuckLake — schema-as-data in the same
#     temporal store the scraper produces (versioned, time-travelable, diffable by the same
#     machinery). JSON seeds become a bootstrap; the lake is the source of truth. ---
_LAKE_DDL = {
    "catalog_source": [("dialect", "varchar"), ("essence", "varchar"), ("from_sql", "varchar"),
                       ("where_sql", "varchar"), ("change_signal", "varchar")],
    "catalog_attr": [("dialect", "varchar"), ("essence", "varchar"), ("ord", "int64"),
                     ("attr", "varchar"), ("expr", "varchar")],
    "catalog_table": [("dialect", "varchar"), ("essence", "varchar"), ("alias", "varchar"),
                      ("expr", "varchar"), ("is_root", "int64")],
    "catalog_join": [("dialect", "varchar"), ("essence", "varchar"), ("child", "varchar"),
                     ("parent", "varchar"), ("child_col", "varchar"), ("parent_col", "varchar"),
                     ("is_outer", "int64")],
    "catalog_key": [("dialect", "varchar"), ("table_name", "varchar"), ("ord", "int64"),
                    ("col", "varchar")],
}
_REG_TABLES = {"catalog_source": CATALOG_SOURCE, "catalog_attr": CATALOG_ATTR,
               "catalog_table": CATALOG_TABLE, "catalog_join": CATALOG_JOIN, "catalog_key": CATALOG_KEY}


def to_lake(reg_conn, writer, data_path, *, sample_time=None):
    """Write the working registry into DuckLake — its durable, versioned home. Each registry
    table becomes a DuckLake table; one snapshot per call, so the config itself time-travels."""
    import ducklake_oob_writer as dl
    existing = {t["table_name"] for t in writer.current_tables()}
    for name, ddl in _LAKE_DDL.items():
        if name not in existing:
            writer.create_table("main", name, ddl)
        os.makedirs(os.path.join(data_path, "main", name), exist_ok=True)
        rows = [tuple(r) for r in reg_conn.execute(select(_REG_TABLES[name]))]
        if not rows:
            continue
        stamp = sample_time.strftime("%Y%m%dT%H%M%S") if sample_time else "seed"
        pq = os.path.join(data_path, "main", name, f"{name}_{stamp}.parquet")
        dl.write_rows_parquet(ddl, rows, pq)
        writer.register_parquet(name, pq, rel_path=os.path.basename(pq), snapshot_time=sample_time)


def load_from_lake(reg_conn, catalog, data_path):
    """Materialize the working registry from its DuckLake home (the inverse of to_lake) — SA
    Core reads through the duckdb-engine lake_reader, bulk-inserted into the working registry."""
    import ducklake_oob_writer as dl
    with dl.lake_reader(catalog, data_path) as lc:
        for name, tbl in _REG_TABLES.items():
            rows = [dict(r._mapping) for r in lc.execute(select(text("*")).select_from(text(f"lake.{name}")))]
            if rows:
                reg_conn.execute(tbl.insert(), rows)


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
