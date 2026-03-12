"""
rule4.catalog — Universal schema registry backed by DuckLake.

Reads from rule4_table / rule4_column in a DuckLake database and
emits standard SQLAlchemy MetaData / Table / Column objects that can
target any dialect.

Usage:

    from rule4.catalog import open_catalog

    catalog = open_catalog("sql/ducklake/schema_registry/schema_catalog.duckdb")

    # Full MetaData for a catalog (set-based, two queries)
    meta = catalog.metadata("data.cityofnewyork.us")
    meta.create_all(postgres_engine)

    # Single table, lazy
    table = catalog.table("data.cityofnewyork.us", "main", "ic3t-wcy2")
    print(table.columns.keys())

    # PIT: schema as of snapshot 4
    meta = catalog.metadata("data.cityofnewyork.us", snapshot_id=4)

    # List all catalogs
    catalog.catalogs()

    # Self-description
    meta = catalog.metadata("rule4")
"""

from __future__ import annotations

from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

import duckdb
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    String,
    Integer,
    BigInteger,
    Numeric,
    Float,
    Boolean,
    DateTime,
    Date,
    Text,
)


# ── Type mapping from YAML ────────────────────────────────────────────

_SA_TYPES = {
    "String": String,
    "Integer": Integer,
    "BigInteger": BigInteger,
    "Numeric": Numeric,
    "Float": Float,
    "Boolean": Boolean,
    "DateTime": DateTime,
    "Date": Date,
    "Text": Text,
}


def _load_type_map() -> dict[str, type]:
    """Load type_map.yaml and build a flat lookup from all sources."""
    import yaml
    yaml_path = Path(__file__).parent / "type_map.yaml"
    with open(yaml_path) as f:
        data = yaml.safe_load(f)

    flat = {}
    for source_name, mappings in data.get("sources", {}).items():
        for native_type, sa_name in mappings.items():
            sa_cls = _SA_TYPES.get(sa_name, String)
            # Exact match takes priority; don't overwrite
            if native_type not in flat:
                flat[native_type] = sa_cls
            # Also store uppercase for fallback lookup
            upper = native_type.upper()
            if upper not in flat:
                flat[upper] = sa_cls

    return flat


def _load_type_families() -> dict[str, str]:
    """Load type_map.yaml and build type_name -> family_name lookup."""
    import yaml
    yaml_path = Path(__file__).parent / "type_map.yaml"
    with open(yaml_path) as f:
        data = yaml.safe_load(f)

    lookup = {}
    for family, members in data.get("type_families", {}).items():
        for member in members:
            lookup[member] = family
            lookup[member.upper()] = family
    return lookup


_TYPE_MAP: dict[str, type] = _load_type_map()
_TYPE_FAMILIES: dict[str, str] = _load_type_families()


def _sa_type(type_name: str) -> type:
    """Map a source-native type name to a SQLAlchemy type."""
    if type_name is None:
        return String
    # Try exact match first
    if type_name in _TYPE_MAP:
        return _TYPE_MAP[type_name]
    # Handle parameterized types like VARCHAR(255), DECIMAL(10,2)
    base = type_name.split("(")[0].strip().upper()
    return _TYPE_MAP.get(base, String)


def type_family(type_name: str) -> str:
    """Map a type name (SA class name or physical) to its family."""
    if type_name in _TYPE_FAMILIES:
        return _TYPE_FAMILIES[type_name]
    base = type_name.split("(")[0].strip().upper()
    if base in _TYPE_FAMILIES:
        return _TYPE_FAMILIES[base]
    return base.lower()


# ── Catalog connection ─────────────────────────────────────────────────


class Rule4Catalog:
    """
    A read-only handle to the rule4 schema registry in DuckLake.

    Provides set-based reads of rule4_table and rule4_column,
    emitting SQLAlchemy MetaData / Table / Column objects.
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection, lake_alias: str = "lake"):
        self._conn = conn
        self._lake = lake_alias

    # ── Public API ─────────────────────────────────────────────────────

    def catalogs(self) -> list[dict]:
        """List all catalogs with table and column counts."""
        rows = self._conn.execute(f"""
            SELECT
                t.catalog_name,
                t.source_type,
                count(DISTINCT t.table_name) AS tables,
                count(c.column_name) AS columns
            FROM {self._lake}.rule4_table AS t
            LEFT JOIN {self._lake}.rule4_column AS c
              USING (catalog_name, schema_name, table_name)
            GROUP BY ALL
            ORDER BY tables DESC
        """).fetchall()
        return [
            {"catalog_name": r[0], "source_type": r[1], "tables": r[2], "columns": r[3]}
            for r in rows
        ]

    def metadata(
        self,
        catalog_name: str,
        schema: Optional[str] = None,
        snapshot_id: Optional[int] = None,
    ) -> MetaData:
        """
        Build a SQLAlchemy MetaData populated from the catalog.

        Two queries: one for tables, one for columns. Set-based.

        Args:
            catalog_name: Which catalog to load (e.g. 'data.cityofnewyork.us')
            schema: Target schema for emitted Table objects (for DDL generation)
            snapshot_id: DuckLake snapshot for PIT query (None = current)
        """
        meta = MetaData(schema=schema)

        tables = self._query_tables(catalog_name, snapshot_id)
        columns = self._query_columns(catalog_name, snapshot_id)

        # Group columns by (schema_name, table_name)
        cols_by_table: dict[tuple[str, str], list[dict]] = defaultdict(list)
        for col in columns:
            key = (col["schema_name"], col["table_name"])
            cols_by_table[key].append(col)

        for tbl in tables:
            key = (tbl["schema_name"], tbl["table_name"])
            tbl_cols = cols_by_table.get(key, [])
            sa_columns = [
                Column(
                    c["column_name"],
                    _sa_type(c["data_type"]),
                    comment=c["description"],
                )
                for c in sorted(tbl_cols, key=lambda c: c["ordinal_position"] or 0)
            ]
            Table(
                tbl["table_name"],
                meta,
                *sa_columns,
                comment=tbl["description"],
            )

        return meta

    def table(
        self,
        catalog_name: str,
        schema_name: str,
        table_name: str,
        target_schema: Optional[str] = None,
        snapshot_id: Optional[int] = None,
    ) -> Table:
        """Load a single table definition as a SQLAlchemy Table."""
        meta = MetaData(schema=target_schema)
        cols = self._query_columns(
            catalog_name, snapshot_id,
            schema_name=schema_name, table_name=table_name,
        )
        tbl_info = self._query_tables(
            catalog_name, snapshot_id,
            schema_name=schema_name, table_name=table_name,
        )
        comment = tbl_info[0]["description"] if tbl_info else None

        sa_columns = [
            Column(
                c["column_name"],
                _sa_type(c["data_type"]),
                comment=c["description"],
            )
            for c in sorted(cols, key=lambda c: c["ordinal_position"] or 0)
        ]
        return Table(table_name, meta, *sa_columns, comment=comment)

    # ── Internal queries ───────────────────────────────────────────────

    def _table_source(self, snapshot_id: Optional[int], table_name: str) -> str:
        """Return the FROM clause — current table or PIT via ducklake_table_insertions."""
        if snapshot_id is None:
            return f"{self._lake}.{table_name}"
        return (
            f"ducklake_table_insertions('{self._lake}', 'main', "
            f"'{table_name}', 0::BIGINT, {snapshot_id}::BIGINT)"
        )

    def _query_tables(
        self,
        catalog_name: str,
        snapshot_id: Optional[int] = None,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
    ) -> list[dict]:
        src = self._table_source(snapshot_id, "rule4_table")
        where = ["catalog_name = $1"]
        params = [catalog_name]
        if schema_name is not None:
            where.append(f"schema_name = ${len(params) + 1}")
            params.append(schema_name)
        if table_name is not None:
            where.append(f"table_name = ${len(params) + 1}")
            params.append(table_name)

        sql = f"""
            SELECT catalog_name, schema_name, table_name,
                   source_type, table_type, description
            FROM {src}
            WHERE {' AND '.join(where)}
        """
        rows = self._conn.execute(sql, params).fetchall()
        return [
            {
                "catalog_name": r[0], "schema_name": r[1], "table_name": r[2],
                "source_type": r[3], "table_type": r[4], "description": r[5],
            }
            for r in rows
        ]

    def _query_columns(
        self,
        catalog_name: str,
        snapshot_id: Optional[int] = None,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
    ) -> list[dict]:
        src = self._table_source(snapshot_id, "rule4_column")
        where = ["catalog_name = $1"]
        params = [catalog_name]
        if schema_name is not None:
            where.append(f"schema_name = ${len(params) + 1}")
            params.append(schema_name)
        if table_name is not None:
            where.append(f"table_name = ${len(params) + 1}")
            params.append(table_name)

        sql = f"""
            SELECT catalog_name, schema_name, table_name,
                   column_name, ordinal_position, data_type, description
            FROM {src}
            WHERE {' AND '.join(where)}
            ORDER BY table_name, ordinal_position
        """
        rows = self._conn.execute(sql, params).fetchall()
        return [
            {
                "catalog_name": r[0], "schema_name": r[1], "table_name": r[2],
                "column_name": r[3], "ordinal_position": r[4],
                "data_type": r[5], "description": r[6],
            }
            for r in rows
        ]


# ── Public entry point ─────────────────────────────────────────────────


def open_catalog(
    catalog_db: str = "schema_catalog.duckdb",
    data_path: str = "data/",
    lake_alias: str = "lake",
) -> Rule4Catalog:
    """
    Open a DuckLake-backed rule4 catalog.

    Args:
        catalog_db: Path to the DuckLake catalog DuckDB file.
        data_path: DATA_PATH for the DuckLake attachment.
        lake_alias: DuckDB alias for the attached DuckLake database.

    Returns:
        A Rule4Catalog instance ready for queries.
    """
    conn = duckdb.connect()
    conn.execute("INSTALL ducklake; LOAD ducklake;")
    conn.execute(
        f"ATTACH 'ducklake:{catalog_db}' AS {lake_alias} "
        f"(DATA_PATH '{data_path}', AUTOMATIC_MIGRATION TRUE)"
    )
    return Rule4Catalog(conn, lake_alias)
