"""Type mapping as **reference data**, not Python.

The correspondence between a dialect's type names, the universal ODBC SQL type codes, and
the SQLAlchemy / DuckLake types is *data* — a reference table you JOIN against, never a
hardcoded dict. (ODBC even hands you half of it: ``SQLGetTypeInfo`` is the driver's own
type catalog, ``TYPE_NAME -> DATA_TYPE`` = the ODBC code.) Resolving a captured column's
SA type / DuckLake type / extraction transform then becomes:

    column_role  ⋈  connections (for the dialect)  ⋈  type_reference

— pure relational, no per-type code. The ``transform`` value names a ``@compiles``
construct (``to_hex`` / ``to_wkt`` / ``to_iso``); the ``sa_type`` value is a SQLAlchemy
type class name (resolve with ``getattr(sqlalchemy, …)``). One denormalised table — we
only read it for tracking, so normalisation doesn't earn its keep.

Columns: dialect, type_name, odbc_code, odbc_name, sa_type, ducklake_type, transform, is_lob.
"""
import os

import duckdb

import ducklake_oob_writer as dl

_DDL = [("dialect", "varchar"), ("type_name", "varchar"), ("odbc_code", "int64"),
        ("odbc_name", "varchar"), ("sa_type", "varchar"), ("ducklake_type", "varchar"),
        ("transform", "varchar"), ("is_lob", "boolean")]
_PARQUET_DDL = ("dialect VARCHAR, type_name VARCHAR, odbc_code BIGINT, odbc_name VARCHAR, "
                "sa_type VARCHAR, ducklake_type VARCHAR, transform VARCHAR, is_lob BOOLEAN")

# (dialect, type_name, odbc_code, odbc_name, sa_type, ducklake_type, transform, is_lob)
SEED = [
    # --- SQL Server ---
    ("sqlserver", "int", 4, "SQL_INTEGER", "Integer", "int32", None, False),
    ("sqlserver", "bigint", -5, "SQL_BIGINT", "BigInteger", "int64", None, False),
    ("sqlserver", "smallint", 5, "SQL_SMALLINT", "SmallInteger", "int16", None, False),
    ("sqlserver", "tinyint", -6, "SQL_TINYINT", "SmallInteger", "int16", None, False),
    ("sqlserver", "bit", -7, "SQL_BIT", "Boolean", "boolean", None, False),
    ("sqlserver", "decimal", 3, "SQL_DECIMAL", "Numeric", "float64", None, False),
    ("sqlserver", "numeric", 2, "SQL_NUMERIC", "Numeric", "float64", None, False),
    ("sqlserver", "money", 3, "SQL_DECIMAL", "Numeric", "varchar", "to_text", False),
    ("sqlserver", "real", 7, "SQL_REAL", "Float", "float32", None, False),
    ("sqlserver", "float", 8, "SQL_DOUBLE", "Float", "float64", None, False),
    ("sqlserver", "varchar", 12, "SQL_VARCHAR", "String", "varchar", None, False),
    ("sqlserver", "char", 1, "SQL_CHAR", "String", "varchar", None, False),
    ("sqlserver", "nvarchar", -9, "SQL_WVARCHAR", "Unicode", "varchar", None, False),
    ("sqlserver", "nchar", -8, "SQL_WCHAR", "Unicode", "varchar", None, False),
    ("sqlserver", "text", -1, "SQL_LONGVARCHAR", "Text", "varchar", None, True),
    ("sqlserver", "ntext", -10, "SQL_WLONGVARCHAR", "UnicodeText", "varchar", None, True),
    ("sqlserver", "binary", -2, "SQL_BINARY", "LargeBinary", "varchar", "to_hex", False),
    ("sqlserver", "varbinary", -3, "SQL_VARBINARY", "LargeBinary", "varchar", "to_hex", False),
    ("sqlserver", "image", -4, "SQL_LONGVARBINARY", "LargeBinary", "varchar", "to_hex", True),
    ("sqlserver", "timestamp", -2, "SQL_BINARY", "LargeBinary", "varchar", "to_hex", False),
    ("sqlserver", "uniqueidentifier", -11, "SQL_GUID", "Uuid", "varchar", "to_text", False),
    ("sqlserver", "date", 91, "SQL_TYPE_DATE", "Date", "date", None, False),
    ("sqlserver", "time", 92, "SQL_TYPE_TIME", "Time", "time", None, False),
    ("sqlserver", "datetime", 93, "SQL_TYPE_TIMESTAMP", "DateTime", "timestamp", None, False),
    ("sqlserver", "datetime2", 93, "SQL_TYPE_TIMESTAMP", "DateTime", "timestamp", None, False),
    ("sqlserver", "datetimeoffset", -155, "SQL_SS_TIMESTAMPOFFSET", "DateTime", "varchar", "to_iso", False),
    ("sqlserver", "geography", -151, "SQL_SS_UDT", "LargeBinary", "varchar", "to_wkt", True),
    ("sqlserver", "geometry", -151, "SQL_SS_UDT", "LargeBinary", "varchar", "to_wkt", True),
    ("sqlserver", "xml", -152, "SQL_SS_XML", "UnicodeText", "varchar", None, True),
    ("sqlserver", "sql_variant", -150, "SQL_SS_VARIANT", "String", "varchar", "to_text", True),
    # --- SQLite (declared types, uppercased by the projection) ---
    ("sqlite", "INTEGER", 4, "SQL_INTEGER", "BigInteger", "int64", None, False),
    ("sqlite", "TEXT", 12, "SQL_VARCHAR", "String", "varchar", None, False),
    ("sqlite", "REAL", 8, "SQL_DOUBLE", "Float", "float64", None, False),
    ("sqlite", "NUMERIC", 2, "SQL_NUMERIC", "Numeric", "float64", None, False),
    ("sqlite", "BLOB", -3, "SQL_VARBINARY", "LargeBinary", "varchar", "to_hex", False),
    # --- PostgreSQL ---
    ("postgresql", "integer", 4, "SQL_INTEGER", "Integer", "int32", None, False),
    ("postgresql", "bigint", -5, "SQL_BIGINT", "BigInteger", "int64", None, False),
    ("postgresql", "smallint", 5, "SQL_SMALLINT", "SmallInteger", "int16", None, False),
    ("postgresql", "boolean", -7, "SQL_BIT", "Boolean", "boolean", None, False),
    ("postgresql", "real", 7, "SQL_REAL", "Float", "float32", None, False),
    ("postgresql", "double precision", 8, "SQL_DOUBLE", "Float", "float64", None, False),
    ("postgresql", "numeric", 2, "SQL_NUMERIC", "Numeric", "float64", None, False),
    ("postgresql", "character varying", 12, "SQL_VARCHAR", "String", "varchar", None, False),
    ("postgresql", "text", -1, "SQL_LONGVARCHAR", "Text", "varchar", None, True),
    ("postgresql", "bytea", -3, "SQL_VARBINARY", "LargeBinary", "varchar", "to_hex", False),
    ("postgresql", "uuid", -11, "SQL_GUID", "Uuid", "varchar", "to_text", False),
    ("postgresql", "date", 91, "SQL_TYPE_DATE", "Date", "date", None, False),
    ("postgresql", "timestamp without time zone", 93, "SQL_TYPE_TIMESTAMP", "DateTime", "timestamp", None, False),
    ("postgresql", "timestamp with time zone", 93, "SQL_TYPE_TIMESTAMP", "DateTime", "varchar", "to_iso", False),
]


def seed_into(writer, data_path, sample_time, *, schema_name="main"):
    """Materialise the reference table in a DuckLake catalog (idempotent-ish: one snapshot)."""
    writer.create_table(schema_name, "type_reference", _DDL)
    tdir = os.path.join(data_path, schema_name, "type_reference")
    os.makedirs(tdir, exist_ok=True)
    pq = os.path.join(tdir, "seed.parquet")
    d = duckdb.connect()
    d.execute(f"CREATE TABLE r ({_PARQUET_DDL})")
    d.executemany(f"INSERT INTO r VALUES ({','.join('?' * len(_DDL))})", SEED)
    d.execute(f"COPY r TO '{pq}' (FORMAT PARQUET)")
    d.close()
    writer.register_parquet("type_reference", pq, rel_path="seed.parquet", snapshot_time=sample_time)


# The resolution is a JOIN — no per-type Python. Captured columns ⋈ type_reference.
RESOLVE_SQL = """
SELECT cr.object_name, cr.member_name, cr.data_type,
       tr.odbc_name, tr.sa_type, tr.ducklake_type, tr.transform, tr.is_lob
FROM lake.column_role AS cr
JOIN lake.type_reference AS tr
  ON tr.dialect = ? AND upper(tr.type_name) = upper(cr.data_type)
WHERE cr.dataserver = ? AND cr.database = ? AND cr.grouping_kind = 'table'
ORDER BY cr.object_name, cr.ordinal
"""
