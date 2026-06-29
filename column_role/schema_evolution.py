"""Drive DuckLake schema evolution from the column_role registry.

column_role is the schema-as-data registry: ``schema_as_of(server, db, T)`` is a source's
columns as they were at T. This maps those source types to DuckLake column types and
reconciles a DuckLake table to match (additive). So the same registry that *records*
schema drift also *drives* a replica's schema evolution — one diff, applied through
``DuckLakeWriter.reconcile_columns``. The capture instance / ``column_role`` row is the
source of truth for "what columns exist, as of when"; the reconcile is the apply.
"""

# source data_type -> DuckLake type (the common scalars across SQLite/PG/SQL Server;
# widen per source dialect as needed — unknown types fall back to varchar).
_DUCKLAKE = {
    "integer": "int64", "int": "int64", "bigint": "int64", "smallint": "int64",
    "tinyint": "int64", "serial": "int64", "bigserial": "int64",
    "text": "varchar", "varchar": "varchar", "nvarchar": "varchar", "char": "varchar",
    "nchar": "varchar", "character varying": "varchar", "character": "varchar", "string": "varchar",
    "real": "float64", "double": "float64", "double precision": "float64", "float": "float64",
    "float4": "float32", "float8": "float64",
    "numeric": "float64", "decimal": "float64", "money": "float64",
    "boolean": "boolean", "bool": "boolean", "bit": "boolean",
    "date": "date", "timestamp": "timestamp", "datetime": "timestamp", "datetime2": "timestamp",
    "timestamp without time zone": "timestamp",
}


def ducklake_type(source_type):
    base = (source_type or "").lower().split("(", 1)[0].strip()
    return _DUCKLAKE.get(base, "varchar")


def desired_columns(registry, dataserver, database, table, when, *, schema="main",
                    grouping_kind="table"):
    """The DuckLake ``(name, type)`` column list for ``table`` as column_role knew it at
    ``when`` — ordered, types mapped from the source dialect."""
    return [(member_name, ducklake_type(data_type))
            for (sname, oname, gkind, member_name, ordinal, data_type, _ro, _rm)
            in registry.schema_as_of(dataserver, database, when)
            if gkind == grouping_kind and oname == table and sname == schema]


def reconcile_from_column_role(writer, ducklake_table, registry, dataserver, database,
                               source_table, when, *, schema="main", snapshot_time=None,
                               grouping_kind="table"):
    """Evolve ``ducklake_table`` so its columns match the source table's schema as
    column_role knew it at ``when`` — the diff->evolve step. Additive (adds new columns;
    never drops/renames). Returns the list of columns added."""
    desired = desired_columns(registry, dataserver, database, source_table, when,
                              schema=schema, grouping_kind=grouping_kind)
    return writer.reconcile_columns(ducklake_table, desired, schema_name=schema,
                                    snapshot_time=snapshot_time)
