"""Construct 'tailing' queries against a source from the schema-as-data metadata.

Given a source's columns+types (from the column_role schema time-series) and a stored
connection, build the CDC/CT/backlog SELECT that samples the payload since a watermark —
with the **type-aware projection** (``extraction``), so funky values are CAST on the way
out. For now the query is built and run through SQLAlchemy/pyodbc in Python; the *same*
SQL would later federate through DuckDB's nanodbc/ODBC extension. The connection is
metadata too — its ODBC components live in the metadatabase, no secrets (integrated
security by default).
"""
from extraction import tailing_projection


def odbc_connection_string(driver, server, database, *, trusted=True, extra=""):
    """Assemble an ODBC connection string from the components stored in the metadatabase.
    Integrated security by default, so no password ever lives in the catalog."""
    parts = [f"DRIVER={{{driver}}}", f"SERVER={server}", f"DATABASE={database}"]
    if trusted:
        parts.append("Trusted_Connection=yes")
    if extra:
        parts.append(extra)
    return ";".join(parts)


def ct_tailing_query(schema_name, table, columns, dialect, *, key):
    """A Change-Tracking *net-changes-since-watermark* query, type-aware. ``columns`` =
    ``[(name, source_type), …]`` from a column_role capture. The watermark binds at the
    single ``?`` (the CT version). For non-CT sources, a backlog snapshot with the same
    projection is returned."""
    if dialect == "sqlserver":
        proj = tailing_projection(columns, dialect, table_alias="b")
        return (f"SELECT ct.SYS_CHANGE_OPERATION AS __op, ct.[{key}] AS __key, {proj} "
                f"FROM CHANGETABLE(CHANGES [{schema_name}].[{table}], ?) AS ct "
                f"LEFT JOIN [{schema_name}].[{table}] AS b ON b.[{key}] = ct.[{key}]")
    # backlog fallback (no CT): a full snapshot carrying the same type-aware projection
    return f"SELECT {tailing_projection(columns, dialect)} FROM {table}"
