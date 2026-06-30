"""Temporal schema registry — column_role captured over time into a DuckLake.

Repeatedly sample a source's structure (run the dialect `column_role` projection),
*widen* each capture with its provenance — `(dataserver, database, sample_time)`,
constant across the result-set — and record it into one DuckLake registry **partitioned
on (dataserver, database)** (the rewrite-style dimension encoding: the constant context
lifted to pruning columns — "transitive data"). `schema_as_of(server, db, T)` returns
the schema as the latest capture `<= T` (the implicit transaction-time interval).
Append-only: never updated, so freely denormalized.

This composes the three pieces: `column_role` (schema-as-data) + `ducklake_oob_writer`
(the OOB apply layer) + the constant-column partition encoding.
"""
from __future__ import annotations

import datetime as _dt
import os
import re

from sqlalchemy import (BigInteger, Column, DateTime, MetaData, String, Table, and_, func,
                        select, text)

import ducklake_oob_writer as dl

_SQLDIR = os.path.join(os.path.dirname(__file__), "sql")

# lake.column_role as a SA Core table — for reads through the duckdb-engine lake_reader
_LAKE = MetaData()
_CR = Table("column_role", _LAKE,
            Column("dataserver", String), Column("database", String),
            Column("sample_time", DateTime), Column("schema_name", String),
            Column("object_name", String), Column("grouping_kind", String),
            Column("member_name", String), Column("ordinal", BigInteger),
            Column("data_type", String), Column("referenced_object", String),
            Column("referenced_member", String), schema="lake")

# the column_role columns we keep in the registry (a useful subset of the 28), in order
_SUBSET = ["schema_name", "object_name", "grouping_kind", "member_name",
           "ordinal", "data_type", "referenced_object", "referenced_member"]
_CONTEXT = ["dataserver", "database", "sample_time"]
_COLS = _CONTEXT + _SUBSET
# DuckLake column types for create_table (context first, then the subset)
_DDL = ([("dataserver", "varchar"), ("database", "varchar"), ("sample_time", "timestamp")]
        + [("schema_name", "varchar"), ("object_name", "varchar"), ("grouping_kind", "varchar"),
           ("member_name", "varchar"), ("ordinal", "int64"), ("data_type", "varchar"),
           ("referenced_object", "varchar"), ("referenced_member", "varchar")])
_PARQUET_DDL = ("dataserver VARCHAR, database VARCHAR, sample_time TIMESTAMP, "
                "schema_name VARCHAR, object_name VARCHAR, grouping_kind VARCHAR, "
                "member_name VARCHAR, ordinal BIGINT, data_type VARCHAR, "
                "referenced_object VARCHAR, referenced_member VARCHAR")


def projection_body(dialect: str) -> str:
    """The SELECT body of the dialect's column_role view (strip the CREATE/comments/GO)."""
    raw = re.sub(r"--.*", "", open(os.path.join(_SQLDIR, f"{dialect}.sql")).read())
    after = re.split(r"VIEW\s+\S*column_role\s+AS", raw, flags=re.I)[1]
    return re.split(r"\bGO\b", after)[0].strip().rstrip(";")


def capture(cursor, dialect: str, dataserver: str, database: str, sample_time) -> list[tuple]:
    """Run the column_role projection on `cursor`'s source and widen each row with
    (dataserver, database, sample_time). Returns rows in `_COLS` order."""
    sel = ", ".join(_SUBSET)
    rows = cursor.execute(f"SELECT {sel} FROM ( {projection_body(dialect)} ) t").fetchall()
    return [(dataserver, database, sample_time) + tuple(r) for r in rows]


class Registry:
    """A DuckLake schema registry, partitioned on (dataserver, database)."""

    def __init__(self, catalog_path: str, data_path: str):
        from sqlalchemy import create_engine
        self.catalog_path, self.data_path = catalog_path, data_path
        self._eng = create_engine(f"sqlite:///{catalog_path}")
        dl.create_catalog(self._eng)
        w = dl.DuckLakeWriter(self._eng, dl.DUCKLAKE_METADATA)
        w.init_catalog(data_path=data_path)
        w.create_table("main", "column_role", _DDL)
        w.set_partitioning("column_role", ["dataserver", "database"])
        self._w = w
        os.makedirs(os.path.join(data_path, "main", "column_role"), exist_ok=True)

    def record(self, rows: list[tuple], sample_time):
        """Write one capture (constant dataserver/database → partition values via min==max)."""
        if not rows:
            return
        tag = f"{rows[0][0]}__{rows[0][1]}__{sample_time:%Y%m%dT%H%M%S}".replace("/", "_")
        pq = os.path.join(self.data_path, "main", "column_role", f"{tag}.parquet")
        dl.write_rows_parquet(_DDL, rows, pq)
        self._w.register_parquet("column_role", pq, rel_path=f"{tag}.parquet", snapshot_time=sample_time)

    def dispose(self):
        self._eng.dispose()

    def query(self, sql: str):
        with dl.lake_reader(f"sqlite:{self.catalog_path}", self.data_path) as conn:
            return conn.execute(text(sql)).fetchall()

    def schema_as_of(self, dataserver: str, database: str, when):
        """The schema for (dataserver, database) as the latest capture <= `when` — SA Core
        through the duckdb-engine lake_reader."""
        cr = _CR
        latest = (select(func.max(cr.c.sample_time))
                  .where(and_(cr.c.dataserver == dataserver, cr.c.database == database,
                              cr.c.sample_time <= when)).scalar_subquery())
        stmt = (select(cr.c.schema_name, cr.c.object_name, cr.c.grouping_kind, cr.c.member_name,
                       cr.c.ordinal, cr.c.data_type, cr.c.referenced_object, cr.c.referenced_member)
                .where(and_(cr.c.dataserver == dataserver, cr.c.database == database,
                            cr.c.sample_time == latest))
                .order_by(cr.c.schema_name, cr.c.object_name, cr.c.grouping_kind, cr.c.ordinal))
        with dl.lake_reader(f"sqlite:{self.catalog_path}", self.data_path) as conn:
            return conn.execute(stmt).fetchall()
