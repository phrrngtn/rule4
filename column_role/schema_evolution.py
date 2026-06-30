"""Drive DuckLake schema evolution from the column_role registry.

column_role is the schema-as-data registry: ``schema_as_of(server, db, T)`` is a source's
columns as they were at T. This maps those source types to DuckLake column types and
reconciles a DuckLake table to match (additive). So the same registry that *records*
schema drift also *drives* a replica's schema evolution — one diff, applied through
``DuckLakeWriter.reconcile_columns``. The capture instance / ``column_role`` row is the
source of truth for "what columns exist, as of when"; the reconcile is the apply.


Types come from the single source of truth — the ``type_reference`` reference data (see
``type_reference.TYPES``), resolved by ``(dialect, source_type)``. No type dict here.
"""
from type_reference import TYPES


def ducklake_type(source_type, dialect="sqlserver"):
    """The DuckLake type for a source type, via the type_reference resolver (no dict)."""
    return TYPES.resolve(dialect, source_type).ducklake_type


def desired_columns(registry, dataserver, database, table, when, *, schema="main",
                    grouping_kind="table", dialect="sqlserver"):
    """The DuckLake ``(name, type)`` column list for ``table`` as column_role knew it at
    ``when`` — ordered, types resolved from ``type_reference`` for the source dialect."""
    return [(member_name, ducklake_type(data_type, dialect))
            for (sname, oname, gkind, member_name, ordinal, data_type, _ro, _rm)
            in registry.schema_as_of(dataserver, database, when)
            if gkind == grouping_kind and oname == table and sname == schema]


def reconcile_from_column_role(writer, ducklake_table, registry, dataserver, database,
                               source_table, when, *, schema="main", snapshot_time=None,
                               grouping_kind="table", dialect="sqlserver"):
    """Evolve ``ducklake_table`` so its columns match the source table's schema as
    column_role knew it at ``when`` — the diff->evolve step. Additive (adds new columns;
    never drops/renames). Returns the list of columns added."""
    desired = desired_columns(registry, dataserver, database, source_table, when,
                              schema=schema, grouping_kind=grouping_kind, dialect=dialect)
    return writer.reconcile_columns(ducklake_table, desired, schema_name=schema,
                                    snapshot_time=snapshot_time)
