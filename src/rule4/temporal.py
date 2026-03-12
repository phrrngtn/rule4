"""
rule4.temporal — Dialect-independent TTST (transaction-time state table) support.

Given a SQLAlchemy Table, produces a temporalized version (with tt_start/tt_end)
and provides TTST upsert logic that works across PostgreSQL, SQLite, DuckDB,
and SQL Server.

The upsert takes a JSON blob (single bind parameter) containing an array of row
objects and expands it to a relation via a CTE. The JSON-to-tabular expansion
is dialect-specific (custom @compiles clauses on JsonSource and JsonField);
the TTST close/insert logic is pure SA expression API — no string SQL.

Usage:

    from rule4.temporal import temporalize, ttst_sync

    # Temporalize a table definition
    tt_table = temporalize(base_table)
    tt_table.create(engine, checkfirst=True)

    # Sync a batch of rows (JSON array string)
    result = ttst_sync(engine, tt_table, payload_json, business_keys=[":id"])
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy import (
    Column,
    MetaData,
    String,
    Table,
    and_,
    or_,
    literal,
    select,
    func,
    bindparam,
)
from sqlalchemy.sql import ColumnElement
from sqlalchemy.sql.expression import FromClause, BinaryExpression
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.types import NullType

TT_END_SENTINEL = "9999-12-31T00:00:00+00:00"


# ── Temporalize ─────────────────────────────────────────────────────────


def temporalize(table, schema=None):
    """Add tt_start and tt_end columns to a SQLAlchemy Table.

    Returns a new Table with all original columns plus:
      tt_start  TEXT  NOT NULL   (ISO 8601 UTC string)
      tt_end    TEXT  NOT NULL   (ISO 8601 UTC string, sentinel = 9999-12-31...)

    The original table is not modified.
    """
    meta = MetaData(schema=schema or table.schema)
    columns = [Column(c.name, c.type) for c in table.columns]
    columns.append(Column("tt_start", String, nullable=False))
    columns.append(Column("tt_end", String, nullable=False))
    return Table(table.name, meta, *columns)


# ── Dialect-specific JSON expansion elements ────────────────────────────


class JsonSource(FromClause):
    """FROM clause that expands a JSON array bind parameter into rows.

    Compiles to dialect-specific table-valued JSON expansion:

    PostgreSQL:   jsonb_array_elements(:payload::jsonb) AS elem
    SQLite:       json_each(:payload) AS elem
    DuckDB:       (SELECT unnest(from_json(:payload, '["json"]')) AS elem) AS _src
    SQL Server:   OPENJSON(:payload) AS elem   (returns key/value/type columns)
    """
    inherit_cache = True
    named_with_column = True

    def __init__(self, param_name="payload"):
        super().__init__()
        self.param_name = param_name
        self.name = "_json_source"
        self._param = bindparam(param_name)

    @property
    def _from_objects(self):
        return [self]


class JsonField(ColumnElement):
    """Extract a text field from a JSON array element using ->> operator.

    Compiles to dialect-specific arrow extraction:

    PostgreSQL:   elem ->> ':field_name'              (direct key, no $ prefix)
    SQLite:       elem.value ->> '$.:field_name'      (JSONPath, from json_each)
    DuckDB:       elem ->> '$.:field_name'            (JSONPath, from unnest)
    SQL Server:   JSON_VALUE(elem.value, '$.:field_name')  (from OPENJSON)
    """
    type = String()
    inherit_cache = True

    def __init__(self, field_name):
        super().__init__()
        self.field_name = field_name


def _json_path(name):
    """Build a JSONPath expression for a field name.

    Uses double-quoted notation for keys with special characters
    (required by SQL Server, works on SQLite and DuckDB too).
    Plain dot notation for simple keys.
    """
    if any(c in name for c in ':. "[]'):
        escaped = name.replace('"', '\\"')
        return f'$."{escaped}"'
    return f'$.{name}'


class NullSafeNE(ColumnElement):
    """NULL-safe inequality: True when values differ, including NULL vs non-NULL.

    Equivalent to IS DISTINCT FROM (which SQL Server lacks).

    PostgreSQL/SQLite/DuckDB: compiles to IS DISTINCT FROM / IS NOT
    SQL Server:               (a <> b OR (a IS NULL AND b IS NOT NULL)
                                       OR (a IS NOT NULL AND b IS NULL))
    """
    type = NullType()
    inherit_cache = True

    def __init__(self, left, right):
        super().__init__()
        self.left = left
        self.right = right


@compiles(NullSafeNE, "mssql")
def _mssql_null_safe_ne(element, compiler, **kw):
    l = compiler.process(element.left, **kw)
    r = compiler.process(element.right, **kw)
    return (f"({l} <> {r} OR ({l} IS NULL AND {r} IS NOT NULL) "
            f"OR ({l} IS NOT NULL AND {r} IS NULL))")


@compiles(NullSafeNE)
def _default_null_safe_ne(element, compiler, **kw):
    l = compiler.process(element.left, **kw)
    r = compiler.process(element.right, **kw)
    # SA's is_distinct_from works on PG, SQLite (IS NOT), DuckDB
    # but we compile directly to avoid any dialect gaps
    dialect = compiler.dialect.name
    if dialect == "sqlite":
        return f"({l} IS NOT {r})"
    return f"({l} IS DISTINCT FROM {r})"


# ── @compiles: JsonSource ───────────────────────────────────────────────


@compiles(JsonSource, "postgresql")
def _pg_source(element, compiler, **kw):
    param = compiler.process(element._param, **kw)
    return f"jsonb_array_elements({param}::jsonb) AS elem"


@compiles(JsonSource, "sqlite")
def _sqlite_source(element, compiler, **kw):
    param = compiler.process(element._param, **kw)
    return f"json_each({param}) AS elem"


@compiles(JsonSource, "mssql")
def _mssql_source(element, compiler, **kw):
    param = compiler.process(element._param, **kw)
    return f"OPENJSON({param}) AS elem"


@compiles(JsonSource, "default")  # DuckDB and others
def _duckdb_source(element, compiler, **kw):
    param = compiler.process(element._param, **kw)
    return f"(SELECT unnest(from_json({param}, '[\"json\"]')) AS elem) AS _src"


# ── @compiles: JsonField ───────────────────────────────────────────────


@compiles(JsonField, "postgresql")
def _pg_field(element, compiler, **kw):
    key = element.field_name.replace("'", "''")
    return f"elem ->> '{key}'"


@compiles(JsonField, "sqlite")
def _sqlite_field(element, compiler, **kw):
    path = _json_path(element.field_name)
    return f"elem.value ->> '{path}'"


@compiles(JsonField, "mssql")
def _mssql_field(element, compiler, **kw):
    path = _json_path(element.field_name)
    return f"JSON_VALUE(elem.value, '{path}')"


@compiles(JsonField, "default")  # DuckDB and others
def _duckdb_field(element, compiler, **kw):
    path = _json_path(element.field_name)
    return f"elem ->> '{path}'"


# ── Staged CTE builder ─────────────────────────────────────────────────


def staged_cte(column_names, param_name="payload"):
    """Build a STAGED CTE that expands a JSON array into typed columns.

    Returns a SA CTE object with .c[col_name] accessors for use in
    update().where(), insert().from_select(), and other SA expressions.

    The inner SELECT is compiled per-dialect via JsonSource and JsonField.
    """
    source = JsonSource(param_name)
    cols = [JsonField(name).label(name) for name in column_names]
    return select(*cols).select_from(source).cte("STAGED")


# ── TTST sync ───────────────────────────────────────────────────────────


def ttst_sync(engine, tt_table, payload_json, business_keys,
              timestamp_column=":updated_at"):
    """Perform a set-based TTST sync: stage → close changed → insert new/changed.

    Uses pure SQLAlchemy expression API. The only dialect-specific bits are
    JsonSource and JsonField, handled via @compiles.

    Args:
        engine: SQLAlchemy engine
        tt_table: Temporalized table (with tt_start, tt_end columns)
        payload_json: JSON string — array of row objects
        business_keys: Column names forming the business key (e.g., [":id"])
        timestamp_column: Column to use as tt_start source (default ":updated_at")

    Returns:
        dict with counts: {"staged", "closed", "inserted"}
    """
    rows = json.loads(payload_json)
    if not rows:
        return {"staged": 0, "closed": 0, "inserted": 0}

    # Normalize: serialize nested objects to JSON strings
    for row in rows:
        for k, v in row.items():
            if isinstance(v, (dict, list)):
                row[k] = json.dumps(v)

    payload = json.dumps(rows)
    now_utc = datetime.now(timezone.utc).isoformat()

    data_col_names = [c.name for c in tt_table.columns
                      if c.name not in ("tt_start", "tt_end")]
    value_col_names = [n for n in data_col_names
                       if n not in business_keys and n != timestamp_column]

    staged = staged_cte(data_col_names)

    # Business key join: tt_table.bk == staged.bk for each key column
    bk_match = and_(
        *[tt_table.c[k] == staged.c[k] for k in business_keys]
    )

    ts_expr = func.coalesce(staged.c[timestamp_column], literal(now_utc))

    with engine.begin() as conn:
        # ── 1. CLOSE changed rows ──────────────────────────────────
        # UPDATE tt_table SET tt_end = ... FROM STAGED
        # WHERE bk matches AND current row AND values differ
        n_closed = 0
        if value_col_names:
            value_changed = or_(
                *[NullSafeNE(tt_table.c[c], staged.c[c])
                  for c in value_col_names]
            )
            close_stmt = (
                tt_table.update()
                .values(tt_end=ts_expr)
                .where(bk_match)
                .where(tt_table.c.tt_end == TT_END_SENTINEL)
                .where(value_changed)
            )
            result = conn.execute(close_stmt, {"payload": payload})
            n_closed = result.rowcount

        # ── 2. INSERT new and changed rows ─────────────────────────
        # INSERT INTO tt_table SELECT staged.*, ts, sentinel
        # FROM STAGED LEFT JOIN tt_table ON bk AND current
        # WHERE tt_table.bk IS NULL  (new or just-closed)
        join = staged.outerjoin(
            tt_table,
            and_(bk_match, tt_table.c.tt_end == TT_END_SENTINEL),
        )

        insert_select = (
            select(
                *[staged.c[c] for c in data_col_names],
                ts_expr.label("tt_start"),
                literal(TT_END_SENTINEL).label("tt_end"),
            )
            .select_from(join)
            .where(tt_table.c[business_keys[0]].is_(None))
        )

        insert_stmt = tt_table.insert().from_select(
            data_col_names + ["tt_start", "tt_end"],
            insert_select,
        )

        result = conn.execute(insert_stmt, {"payload": payload})
        n_inserted = result.rowcount

    return {"staged": len(rows), "closed": n_closed, "inserted": n_inserted}
