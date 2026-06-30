"""ColumnCollection — the schema-as-data object for one table, assembled from columns.

A table's columns are *data* (a column_role capture, a catalog read, anywhere) — no
SQLAlchemy mapper required; ``from_column_role`` just assembles the objects. One
ColumnCollection then carries the whole round trip:

  (1) sqlalchemy_table()        — a SA model (``Table``) for the source object;
  (2) record_in_ducklake(w)     — the DuckLake DDL (create the replica, or evolve it);
  (3) tail_query_base(engine)   — a composable, type-aware tail SELECT (just the
                                  projection); callers add the WHERE / CHANGETABLE FROM;
  (4) populate_replica(rep, rs) — merge tail results into a current-state replica;
  (5) populate_ducklake(w, rs)  — append tail results to DuckLake as a payload snapshot.

The funky-value transforms in (3) are ``@compiles`` constructs rendering per the source
dialect — the legitimate dialect compiler, since the *source* is a real portability
surface (unlike the monomorphic DuckDB-qua-Parquet tool, which stays raw).
"""
from sqlalchemy import (BigInteger, Boolean, Column as SACol, Date, DateTime, Float, Integer,
                        MetaData, Numeric, String, Table, column as sqlcolumn, select,
                        table as sqltable)
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import ColumnElement

from schema_evolution import ducklake_type

# source base type -> SQLAlchemy type for the source-side model (funky -> String/text)
_SA_TYPE = {
    "int": BigInteger, "integer": BigInteger, "bigint": BigInteger, "smallint": Integer,
    "tinyint": Integer, "serial": BigInteger,
    "real": Float, "float": Float, "double": Float, "double precision": Float,
    "numeric": Numeric, "decimal": Numeric, "money": Numeric,
    "boolean": Boolean, "bool": Boolean, "bit": Boolean,
    "date": Date, "timestamp": DateTime, "datetime": DateTime, "datetime2": DateTime,
}


def _sa_type(source_type):
    return _SA_TYPE.get((source_type or "").lower().split("(", 1)[0].strip(), String)


class Col:
    """One column: source name + source type; the DuckLake type is derived."""

    def __init__(self, name, source_type, ordinal=None):
        self.name, self.source_type, self.ordinal = name, source_type, ordinal

    @property
    def ducklake_type(self):
        return ducklake_type(self.source_type)


class ColumnCollection:
    def __init__(self, schema, name, columns, *, key=None):
        self.schema, self.name, self.columns, self.key = schema, name, columns, key

    @classmethod
    def from_column_role(cls, registry, dataserver, database, table, when, *,
                         schema="main", key=None, grouping_kind="table"):
        """Assemble a ColumnCollection from a column_role capture (the schema time-series)."""
        cols = [Col(member_name, data_type, ordinal)
                for (sname, oname, gkind, member_name, ordinal, data_type, _ro, _rm)
                in registry.schema_as_of(dataserver, database, when)
                if gkind == grouping_kind and oname == table and sname == schema]
        return cls(schema, table, cols, key=key)

    # (1) a SQLAlchemy model (Table) for the source object
    def sqlalchemy_table(self, metadata=None, *, schema=None):
        md = metadata if metadata is not None else MetaData()
        return Table(self.name, md, *[SACol(c.name, _sa_type(c.source_type)) for c in self.columns],
                     schema=schema)

    # (2) record the object in DuckLake speak — create the replica table (or evolve it)
    def record_in_ducklake(self, writer, *, schema_name="main", snapshot_time=None):
        desired = [(c.name, c.ducklake_type) for c in self.columns]
        if self.name in {t["table_name"] for t in writer.current_tables()}:
            return {"evolved": writer.reconcile_columns(self.name, desired, schema_name=schema_name,
                                                        snapshot_time=snapshot_time)}
        return writer.create_table(schema_name, self.name, desired, snapshot_time=snapshot_time)

    # (3) an incremental tail query base — composable, type-aware projection
    def tail_query_base(self, engine, *, source_schema=None):
        """A composable Core ``select()`` of the type-aware projection against the source
        table. The funky transforms render per ``engine``'s dialect at compile time. Add
        ``.where(...)`` (the watermark predicate) / swap the FROM for ``CHANGETABLE``."""
        src = sqltable(self.name, *[sqlcolumn(c.name) for c in self.columns], schema=source_schema)
        return select(*[_extract(src.c[c.name], c.source_type) for c in self.columns]).select_from(src)

    # (4) populate a current-state replica from tail results (merge)
    def populate_replica(self, replica, rows, *, snapshot_time=None):
        """``rows`` = result dicts; an optional ``__op`` ('I'/'U'/'D') routes the merge
        (absent -> all upserts). Returns Replica.apply's counts."""
        upserts = [{c.name: r.get(c.name) for c in self.columns}
                   for r in rows if r.get("__op", "U") in ("I", "U")]
        deletes = [r[self.key] for r in rows if r.get("__op") == "D"]
        return replica.apply(upserts=upserts, deletes=deletes, snapshot_time=snapshot_time)

    # (5) populate DuckLake from tail results — append as a payload snapshot
    def populate_ducklake(self, writer, rows, *, schema_name="main", snapshot_time=None):
        """Append the tail results as inlined rows (a payload snapshot)."""
        return writer.inline_rows(self.name, [{c.name: r.get(c.name) for c in self.columns}
                                              for r in rows if r.get("__op", "I") != "D"],
                                  schema_name=schema_name, snapshot_time=snapshot_time)


# --- dialect-specific value transforms for funky source types (source is polymorphic ->
#     @compiles constructs, compiled per the source dialect) ---
class _Transform(ColumnElement):
    inherit_cache = True

    def __init__(self, col):
        self.col = col
        self.name = getattr(col, "name", None)
        super().__init__()


class to_hex(_Transform):
    """binary/varbinary/rowversion -> hex text."""


class to_wkt(_Transform):
    """geography/geometry -> Well-Known Text."""


class to_iso(_Transform):
    """datetimeoffset -> ISO 8601 text."""


@compiles(to_hex, "mssql")
def _hex_mssql(el, c, **kw):
    return f"CONVERT(VARCHAR(MAX), {c.process(el.col, **kw)}, 1)"


@compiles(to_hex, "postgresql")
def _hex_pg(el, c, **kw):
    return f"encode({c.process(el.col, **kw)}, 'hex')"


@compiles(to_hex)
def _hex_default(el, c, **kw):
    return f"hex({c.process(el.col, **kw)})"


@compiles(to_wkt, "mssql")
def _wkt_mssql(el, c, **kw):
    return f"{c.process(el.col, **kw)}.STAsText()"


@compiles(to_wkt, "postgresql")
def _wkt_pg(el, c, **kw):
    return f"ST_AsText({c.process(el.col, **kw)})"


@compiles(to_wkt)
def _wkt_default(el, c, **kw):
    return c.process(el.col, **kw)


@compiles(to_iso, "mssql")
def _iso_mssql(el, c, **kw):
    return f"CONVERT(VARCHAR(34), {c.process(el.col, **kw)}, 127)"


@compiles(to_iso)
def _iso_default(el, c, **kw):
    return f"CAST({c.process(el.col, **kw)} AS VARCHAR)"


_FUNKY = {"binary": to_hex, "varbinary": to_hex, "image": to_hex, "timestamp": to_hex,
          "rowversion": to_hex, "geography": to_wkt, "geometry": to_wkt, "datetimeoffset": to_iso}


def _extract(col, source_type):
    if source_type:
        ctor = _FUNKY.get(source_type.lower().split("(", 1)[0].strip())
        if ctor:
            return ctor(col).label(col.name)
    return col
