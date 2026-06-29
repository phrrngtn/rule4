"""Type-aware extraction — CAST/CONVERT funky source values to transit-safe forms on the
way *out* of the source, driven by the column_role type information.

The schema-as-data doesn't only build the replica's DDL (see ``schema_evolution``); the
*same* type information shapes the value *extraction*. A binary / sql_variant /
datetimeoffset / spatial column can't ride a generic result set safely, so the tailing
query selects a ``CAST``/``CONVERT`` of it (0x-hex, ISO text, WKT, …) — the textual form
that lands in the DuckLake ``varchar`` the DDL mapping chose for it. One type fact, both
directions: schema out *and* values out.

Mirrors this project's SQL Server conventions: binary via ``CONVERT(…, 1)`` hex,
``sql_variant`` inspected/cast to text, LOB / ``NVARCHAR(MAX)`` columns ordered **last**
(the ODBC driver fetches large objects after all fixed-width columns).
"""

# SQL Server source types that need a CAST/CONVERT on the way out → transit-safe text.
# {c} is the (bracket-quoted) column reference.
_SQLSERVER_FUNKY = {
    "binary": "CONVERT(VARCHAR(MAX), {c}, 1)",        # 0x-prefixed hex
    "varbinary": "CONVERT(VARCHAR(MAX), {c}, 1)",
    "image": "CONVERT(VARCHAR(MAX), {c}, 1)",
    "timestamp": "CONVERT(VARCHAR(MAX), {c}, 1)",     # rowversion
    "rowversion": "CONVERT(VARCHAR(MAX), {c}, 1)",
    "uniqueidentifier": "CAST({c} AS VARCHAR(36))",
    "datetimeoffset": "CONVERT(VARCHAR(34), {c}, 127)",   # ISO 8601
    "sql_variant": "CAST({c} AS NVARCHAR(MAX))",
    "xml": "CAST({c} AS NVARCHAR(MAX))",
    "hierarchyid": "{c}.ToString()",
    "geography": "{c}.STAsText()",
    "geometry": "{c}.STAsText()",
    "money": "CONVERT(VARCHAR(MAX), {c})",
    "smallmoney": "CONVERT(VARCHAR(MAX), {c})",
}
# large-object types → select LAST (ODBC fetches LOBs after fixed-width columns)
_LOB_BASE = {"text", "ntext", "image", "xml", "sql_variant", "geography", "geometry"}


def _base(t):
    return (t or "").lower().split("(", 1)[0].strip()


def is_lob(source_type):
    return _base(source_type) in _LOB_BASE or "(max)" in (source_type or "").lower()


def extraction_expr(column_name, source_type, dialect, *, table_alias=None):
    """The source-side SELECT expression for one column: a CAST/CONVERT for a funky type,
    else the bare column. ``table_alias`` qualifies the reference (e.g. the base-table
    alias in a CHANGETABLE join)."""
    if dialect == "sqlserver":
        ref = f"[{column_name}]" if not table_alias else f"{table_alias}.[{column_name}]"
        tmpl = _SQLSERVER_FUNKY.get(_base(source_type))
        expr = tmpl.format(c=ref) if tmpl else ref
        return f"{expr} AS [{column_name}]"
    ref = f'"{column_name}"' if not table_alias else f'{table_alias}."{column_name}"'
    return ref  # sqlite / pg: passthrough for now (extend per dialect as needed)


def tailing_projection(columns, dialect, *, table_alias=None):
    """The ordered SELECT list for a tailing query: funky types CAST to transit-safe text,
    LOB columns last. ``columns`` = ``[(name, source_type), …]`` (e.g. straight from a
    column_role capture)."""
    fixed = [(n, t) for n, t in columns if not is_lob(t)]
    lobs = [(n, t) for n, t in columns if is_lob(t)]
    return ", ".join(extraction_expr(n, t, dialect, table_alias=table_alias)
                     for n, t in fixed + lobs)
