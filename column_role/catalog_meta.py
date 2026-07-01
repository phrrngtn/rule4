"""catalog_source — the meta-schema for catalog scraping, expressed as data.

Records, per (dialect, essence), how to produce that essence from the *native* catalog: the
FROM (the joined catalog tables), the per-attribute expression mapping, an optional WHERE, and
the ``change_signal`` column (the modify_date/version that drives cheap tailing). A **generic**
generator reads this registry and emits the dialect-specific projection SQL — so adding an
essence for a dialect is *data entry* (INSERTs), not code.

The split the design predicts: **curatorial** (the seed — deciding that SQL Server's column
essence is ``sys.columns ⋈ sys.objects ⋈ sys.types`` with these attr mappings) vs
**deterministic** (the generation). Everything here is SQLAlchemy Core against the registry
(dialect-independent, bound values); the *output* is dialect-specific SQL (catalog
identifiers/expressions, no user values) that the caller runs via ``text()`` on a SA connection
to that dialect's source. The registry lives here in SQLite for the prototype; its natural home
is the same DuckLake time-series it drives (schema-as-data, self-hosting, versioned).
"""
from sqlalchemy import Column, Integer, MetaData, String, Table, and_, select

_MD = MetaData()
CATALOG_SOURCE = Table(
    "catalog_source", _MD,
    Column("dialect", String), Column("essence", String),
    Column("from_sql", String), Column("where_sql", String), Column("change_signal", String))
CATALOG_ATTR = Table(
    "catalog_attr", _MD,
    Column("dialect", String), Column("essence", String),
    Column("ord", Integer), Column("attr", String), Column("expr", String))


# --- the CURATORIAL layer: the essence -> catalog-tables mapping, per dialect (data, not code).
#     'column' essence, reverse-engineered from the hand-written sql/*.sql column branches. ---
_SOURCES = [
    # dialect, essence, from_sql, where_sql, change_signal
    ("sqlserver", "column",
     "sys.objects AS o "
     "JOIN sys.schemas AS s ON s.schema_id = o.schema_id "
     "JOIN sys.columns AS c ON c.object_id = o.object_id "
     "JOIN sys.types AS ty ON ty.user_type_id = c.user_type_id",
     "o.type IN ('U', 'V')", "o.modify_date"),
    ("sqlite", "column",
     "sqlite_master AS m, pragma_table_info(m.name) AS ti",
     "m.type IN ('table', 'view')", None),
    ("duckdb", "column",
     "information_schema.columns AS c",
     None, None),
]
_ATTRS = {
    "sqlserver": [("schema_name", "s.name"), ("object_name", "o.name"), ("member_name", "c.name"),
                  ("ordinal", "c.column_id"), ("data_type", "ty.name"), ("is_nullable", "c.is_nullable")],
    "sqlite": [("schema_name", "'main'"), ("object_name", "m.name"), ("member_name", "ti.name"),
               ("ordinal", "ti.cid"), ("data_type", "ti.type"), ("is_nullable", 'NOT ti."notnull"')],
    "duckdb": [("schema_name", "c.table_schema"), ("object_name", "c.table_name"),
               ("member_name", "c.column_name"), ("ordinal", "c.ordinal_position"),
               ("data_type", "c.data_type"), ("is_nullable", "(c.is_nullable = 'YES')")],
}


def create_registry(conn):
    """Create the catalog_source / catalog_attr tables (SA Core, dialect-independent)."""
    _MD.create_all(conn)


def seed(conn):
    """Load the curated column-essence mappings — the config is data (SA Core inserts)."""
    for dialect, essence, from_sql, where_sql, change in _SOURCES:
        conn.execute(CATALOG_SOURCE.insert().values(
            dialect=dialect, essence=essence, from_sql=from_sql, where_sql=where_sql,
            change_signal=change))
        for i, (attr, expr) in enumerate(_ATTRS[dialect]):
            conn.execute(CATALOG_ATTR.insert().values(
                dialect=dialect, essence=essence, ord=i, attr=attr, expr=expr))


def generate_projection(conn, dialect, essence):
    """Read the registry and assemble the dialect-specific projection for one essence — the
    deterministic half. Returns dialect-specific SQL; run it via ``text()`` on a SA connection
    to that dialect's source."""
    src = conn.execute(
        select(CATALOG_SOURCE.c.from_sql, CATALOG_SOURCE.c.where_sql)
        .where(and_(CATALOG_SOURCE.c.dialect == dialect, CATALOG_SOURCE.c.essence == essence))).one()
    attrs = conn.execute(
        select(CATALOG_ATTR.c.expr, CATALOG_ATTR.c.attr)
        .where(and_(CATALOG_ATTR.c.dialect == dialect, CATALOG_ATTR.c.essence == essence))
        .order_by(CATALOG_ATTR.c.ord)).all()
    select_list = ", ".join(f"{expr} AS {attr}" for expr, attr in attrs)
    sql = f"SELECT {select_list} FROM {src.from_sql}"
    if src.where_sql:
        sql += f" WHERE {src.where_sql}"
    return sql
