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

Every type fact (DuckLake type, SA type, extraction transform) comes from the single source
of truth — the ``type_reference`` reference data (``TYPES``) — resolved by ``(dialect,
source_type)``. No type dict here. The funky-value transforms in (3) are ``@compiles``
constructs rendering per the source dialect (the *source* is a real portability surface).
"""
import datetime as _dt
from itertools import groupby

import sqlalchemy
from sqlalchemy import (Column as SACol, MetaData, String, Table, case, cast,
                        column as sqlcolumn, literal, select, table as sqltable, text)
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import ColumnElement

from extraction import tailing_projection
from type_reference import TYPES


def _as_dt(v):
    """Coerce a transaction-time value to a datetime for DuckLake snapshot_time."""
    if isinstance(v, _dt.datetime) or v is None:
        return v
    try:
        return _dt.datetime.fromisoformat(str(v))
    except ValueError:
        return v


class Col:
    """One column. Every type fact is resolved from ``type_reference`` (no dict)."""

    def __init__(self, name, source_type, ordinal=None, dialect="sqlserver"):
        self.name, self.source_type, self.ordinal, self.dialect = name, source_type, ordinal, dialect

    @property
    def _r(self):
        return TYPES.resolve(self.dialect, self.source_type)

    @property
    def ducklake_type(self):
        return self._r.ducklake_type

    @property
    def sa_type(self):
        return getattr(sqlalchemy, self._r.sa_type, String)

    @property
    def transform(self):
        return self._r.transform


class ColumnCollection:
    def __init__(self, schema, name, columns, *, key=None, dialect="sqlserver"):
        self.schema, self.name, self.columns, self.key, self.dialect = schema, name, columns, key, dialect

    @classmethod
    def from_column_role(cls, registry, dataserver, database, table, when, *,
                         schema="main", key=None, grouping_kind="table", dialect="sqlserver"):
        """Assemble a ColumnCollection from a column_role capture (the schema time-series)."""
        cols = [Col(member_name, data_type, ordinal, dialect)
                for (sname, oname, gkind, member_name, ordinal, data_type, _ro, _rm)
                in registry.schema_as_of(dataserver, database, when)
                if gkind == grouping_kind and oname == table and sname == schema]
        return cls(schema, table, cols, key=key, dialect=dialect)

    # migrate this revision's schema to another revision's — schema-only ALTER TABLE DDL
    def migration_to(self, other, *, dialect=None):
        """The ``ALTER TABLE`` statements taking *this* revision's schema to ``other``'s
        (both directions supported by swapping the call). See ``migration.migration_ddl``."""
        from migration import migration_ddl
        return migration_ddl(self, other, dialect=dialect)

    # (1) a SQLAlchemy model (Table) for the source object
    def sqlalchemy_table(self, metadata=None, *, schema=None):
        md = metadata if metadata is not None else MetaData()
        return Table(self.name, md, *[SACol(c.name, c.sa_type) for c in self.columns], schema=schema)

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
        return select(*[_extract(src.c[c.name], c.transform) for c in self.columns]).select_from(src)

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

    # --- the acquisition seam: poll the source for changes since a watermark ---
    def changed_since(self, conn, watermark, driver, *, source_schema=None):
        """Poll the source for changes since ``watermark`` using ``driver`` — the per-model
        *acquisition* strategy (CT / user-column / backlog). Returns ``(changes,
        next_watermark)``; each change is a dict with ``__op`` / ``__key`` / ``__tt`` (its
        own transaction-time) plus the type-aware column values. Only ``driver`` is
        model-specific; the apply side below is generic."""
        rows = [dict(r._mapping) for r in conn.execute(
            driver.query(self, watermark, source_schema=source_schema))]
        return rows, driver.next_watermark(rows, watermark)

    def sync(self, conn, watermark, driver, replica, *, source_schema=None):
        """One poll → apply. Poll via ``driver``, then **staple rows by transaction-time**:
        group the changes by ``__tt`` and apply each group as one snapshot (so each distinct
        source transaction-time becomes one DuckLake snapshot). ``replica`` is a
        ``HistoryReplica`` (or ``Replica``). Returns the new watermark. Assumes the source's
        transaction-time is monotonic."""
        rows, nw = self.changed_since(conn, watermark, driver, source_schema=source_schema)
        # __tt is the staple (the thing that orders/groups commits); it need not be a
        # timestamp — a driver whose staple is a logical version (CT/CDC LSN) supplies its
        # own __tt -> datetime mapping via snapshot_time(); the default coerces ISO text.
        to_time = getattr(driver, "snapshot_time", _as_dt)
        rows.sort(key=lambda r: r["__tt"])
        for tt, grp in groupby(rows, key=lambda r: r["__tt"]):
            ops = [{"op": r.get("__op", "U"), "key": r["__key"],
                    "row": {c.name: r[c.name] for c in self.columns}} for r in grp]
            replica.apply_commit(ops, snapshot_time=to_time(tt))
        return nw


# --- dialect-specific value transforms for funky source types (source is polymorphic ->
#     @compiles constructs, compiled per the source dialect; named by type_reference.transform) ---
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


_TRANSFORMS = {"to_hex": to_hex, "to_wkt": to_wkt, "to_iso": to_iso}


def _extract(col, transform):
    """The projected expression for a column, by the transform name from type_reference:
    a dialect-aware construct, a plain CAST-to-text, or the bare column."""
    if not transform:
        return col
    if transform == "to_text":
        return cast(col, String).label(col.name)
    ctor = _TRANSFORMS.get(transform)
    return ctor(col).label(col.name) if ctor else col


# --- acquisition drivers (the model-specific seam fed to ColumnCollection.changed_since) ---
class UserColumnDriver:
    """Acquisition by a **user-modeled transaction-time column** — the Socrata ``:updated_at``
    pattern. Poll ``WHERE tt_col > watermark ORDER BY tt_col``; each row is an upsert stamped
    with its own ``tt_col`` as transaction-time; the watermark advances to ``max(tt_col)``.
    Upsert-only unless ``deleted_col`` (a boolean tombstone) marks deletes. The query is SA
    Core over the source dialect, reusing the same type-aware projection as the rest of the
    ColumnCollection (so funky source values are CAST on the way out)."""

    def __init__(self, tt_column, *, key, deleted_col=None):
        self.tt_column, self.key, self.deleted_col = tt_column, key, deleted_col

    def query(self, cc, watermark, *, source_schema=None):
        extra = [sqlcolumn(self.tt_column)]
        if self.deleted_col:
            extra.append(sqlcolumn(self.deleted_col))
        src = sqltable(cc.name, *[sqlcolumn(c.name) for c in cc.columns], *extra,
                       schema=source_schema)
        op = (case((src.c[self.deleted_col].is_(True), "D"), else_="U")
              if self.deleted_col else literal("U"))
        return (select(*[_extract(src.c[c.name], c.transform) for c in cc.columns],
                       op.label("__op"),
                       src.c[self.key].label("__key"),
                       src.c[self.tt_column].label("__tt"))
                .where(src.c[self.tt_column] > watermark)
                .order_by(src.c[self.tt_column]))

    def next_watermark(self, rows, prev):
        return max((r["__tt"] for r in rows), default=prev)


class ChangeTrackingDriver:
    """Acquisition via SQL Server **Change Tracking**. Three things make it differ from the
    user-column driver — the seam absorbs each:

    * **The staple is a logical version, not a data column.** ``CHANGETABLE(CHANGES t, @v)``
      returns the *net* changes since version ``@v``; each row carries ``SYS_CHANGE_VERSION``
      (the version at which that key last changed) — that is ``__tt``. Grouping by it yields
      one DuckLake snapshot per distinct change-version (finer than one-per-poll). CT has no
      commit *timestamp* (unlike CDC's ``lsn_time_mapping``), so ``snapshot_time`` maps the
      integer version to a synthetic monotonic datetime (``epoch + version·unit``) — enough
      for version-addressable ``AT (TIMESTAMP)`` time-travel; a CDC subclass would override it
      with the real commit time.
    * **The query is not SA-Core-expressible.** ``CHANGETABLE`` is a T-SQL TVF with bespoke
      syntax, so ``query`` returns ``text()`` (raw, dialect-specific by necessity — the
      sanctioned fallback) carrying the type-aware projection from ``extraction``.
    * **The watermark is the server's current version, not max-over-rows.** The query selects
      ``CHANGE_TRACKING_CURRENT_VERSION()`` as ``__hwm`` on every row so the next watermark is
      read from the server, not inferred from the changes (which would miss the tail).

    ``__op`` is CT's ``SYS_CHANGE_OPERATION`` (I/U/D) — fed straight to ``apply_commit``."""

    def __init__(self, key, *, schema="dbo", epoch=_dt.datetime(2000, 1, 1), unit="seconds"):
        self.key, self.schema, self.epoch, self.unit = key, schema, epoch, unit

    def query(self, cc, watermark, *, source_schema=None):
        sch = source_schema or self.schema
        proj = tailing_projection([(c.name, c.source_type) for c in cc.columns],
                                  "sqlserver", table_alias="b")
        return text(
            f"SELECT ct.SYS_CHANGE_OPERATION AS __op, ct.[{self.key}] AS __key, "
            f"ct.SYS_CHANGE_VERSION AS __tt, CHANGE_TRACKING_CURRENT_VERSION() AS __hwm, {proj} "
            f"FROM CHANGETABLE(CHANGES [{sch}].[{cc.name}], :wm) AS ct "
            f"LEFT JOIN [{sch}].[{cc.name}] AS b ON b.[{self.key}] = ct.[{self.key}] "
            f"ORDER BY ct.SYS_CHANGE_VERSION").bindparams(wm=watermark)

    def next_watermark(self, rows, prev):
        return max((r["__hwm"] for r in rows), default=prev)

    def snapshot_time(self, tt):
        return self.epoch + _dt.timedelta(**{self.unit: int(tt)})
