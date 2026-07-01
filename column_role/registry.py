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

from sqlalchemy import (BigInteger, Column, DateTime, Float, MetaData, String, Table, and_,
                        case, func, select, text)

import ducklake_oob_writer as dl

_SQLDIR = os.path.join(os.path.dirname(__file__), "sql")

# lake.column_role as a SA Core table — for reads through the duckdb-engine lake_reader
_LAKE = MetaData()
_CR = Table("column_role", _LAKE,
            Column("dataserver", String), Column("database", String),
            Column("sample_time", DateTime), Column("schema_name", String),
            Column("object_name", String), Column("object_id", BigInteger),
            Column("grouping_kind", String),
            Column("member_name", String), Column("ordinal", BigInteger),
            Column("data_type", String), Column("referenced_object", String),
            Column("referenced_member", String), schema="lake")

# the column_role columns we keep in the registry (a useful subset of the 28), in order
_SUBSET = ["schema_name", "object_name", "object_id", "grouping_kind", "member_name",
           "ordinal", "data_type", "referenced_object", "referenced_member"]
_CONTEXT = ["dataserver", "database", "sample_time"]
_COLS = _CONTEXT + _SUBSET
# DuckLake column types for create_table (context first, then the subset)
_DDL = ([("dataserver", "varchar"), ("database", "varchar"), ("sample_time", "timestamp")]
        + [("schema_name", "varchar"), ("object_name", "varchar"), ("object_id", "int64"),
           ("grouping_kind", "varchar"), ("member_name", "varchar"), ("ordinal", "int64"),
           ("data_type", "varchar"), ("referenced_object", "varchar"),
           ("referenced_member", "varchar")])
_PARQUET_DDL = ("dataserver VARCHAR, database VARCHAR, sample_time TIMESTAMP, "
                "schema_name VARCHAR, object_name VARCHAR, object_id BIGINT, "
                "grouping_kind VARCHAR, member_name VARCHAR, ordinal BIGINT, data_type VARCHAR, "
                "referenced_object VARCHAR, referenced_member VARCHAR")


# --- Read 1: the schema-identity probe (the cheap LT change-feed) ---
# One row per object: object_id (the identity LT — drop-recreate) + create/modify_date (the
# change markers). Poll this often; do the wide column sample (Read 2, `capture`) only for
# objects the probe shows changed. object_id is a true logical identity; the dates are
# wallclock (fine for equality/change-detection, not trusted for ordering).
_IDENTITY_SQL = {
    "sqlserver": "SELECT s.name AS schema_name, o.name AS object_name, "
                 "o.object_id AS object_id, o.create_date AS create_date, "
                 "o.modify_date AS modify_date "
                 "FROM sys.objects AS o JOIN sys.schemas AS s ON s.schema_id = o.schema_id "
                 "WHERE o.type IN ('U', 'V')",
    "postgresql": "SELECT n.nspname AS schema_name, c.relname AS object_name, "
                  "c.oid::bigint AS object_id, NULL::timestamp AS create_date, "
                  "NULL::timestamp AS modify_date "
                  "FROM pg_class AS c JOIN pg_namespace AS n ON n.oid = c.relnamespace "
                  "WHERE c.relkind IN ('r', 'v') AND n.nspname NOT LIKE 'pg\\_%'",
}
_ID_DDL = [("dataserver", "varchar"), ("database", "varchar"), ("sample_time", "timestamp"),
           ("schema_name", "varchar"), ("object_name", "varchar"), ("object_id", "int64"),
           ("create_date", "timestamp"), ("modify_date", "timestamp")]
_SI = Table("schema_identity", MetaData(),
            Column("dataserver", String), Column("database", String),
            Column("sample_time", DateTime), Column("schema_name", String),
            Column("object_name", String), Column("object_id", BigInteger),
            Column("create_date", DateTime), Column("modify_date", DateTime), schema="lake")


def capture_identity(cursor, dialect: str, dataserver: str, database: str, sample_time) -> list[tuple]:
    """Read 1 — the cheap per-object identity/LT probe. Returns rows in `_ID_DDL` order:
    (dataserver, database, sample_time, schema_name, object_name, object_id,
    create_date, modify_date)."""
    rows = cursor.execute(_IDENTITY_SQL[dialect]).fetchall()
    return [(dataserver, database, sample_time) + tuple(r) for r in rows]


# --- per-sample environment probe: remote clock-skew estimate + database identity ---
# One row per *sample* (not per object). Carries (a) skew — measure our trusted local UTC,
# ask the remote its UTC, difference is the gross skew (RTT ~ms is noise vs minute-scale
# skew); a *jump* in the skew series between samples is the "remote clock stepped" signal.
# Also the server-local now (to derive the server's TZ, since create/modify_date are
# server-local) and (b) the database create_date — the DB identity that survives dbid reuse,
# so a changed db_create_date ⇒ the database was dropped and recreated.
_ENV_SQL = {
    "sqlserver": "SELECT SYSUTCDATETIME() AS remote_utc, SYSDATETIME() AS remote_local, "
                 "(SELECT create_date FROM sys.databases WHERE database_id = DB_ID()) "
                 "AS db_create_date",
    "postgresql": "SELECT (now() AT TIME ZONE 'UTC') AS remote_utc, "
                  "localtimestamp AS remote_local, "
                  "(pg_postmaster_start_time() AT TIME ZONE 'UTC') AS db_create_date",
}
_CLK_DDL = [("dataserver", "varchar"), ("database", "varchar"), ("sample_time", "timestamp"),
            ("local_utc", "timestamp"), ("remote_utc", "timestamp"),
            ("remote_local", "timestamp"), ("db_create_date", "timestamp"),
            ("skew_seconds", "float64")]
_SC = Table("sample_clock", MetaData(),
            Column("dataserver", String), Column("database", String),
            Column("sample_time", DateTime), Column("local_utc", DateTime),
            Column("remote_utc", DateTime), Column("remote_local", DateTime),
            Column("db_create_date", DateTime), Column("skew_seconds", Float), schema="lake")


def measure_env(cursor, dialect: str, dataserver: str, database: str, sample_time, local_utc):
    """The per-sample environment probe. ``local_utc`` = our trusted UTC captured around the
    call. Returns one `_CLK_DDL`-shaped tuple; ``skew_seconds`` = remote_utc − local_utc."""
    remote_utc, remote_local, db_create = cursor.execute(_ENV_SQL[dialect]).fetchone()
    skew = (remote_utc - local_utc).total_seconds() if remote_utc and local_utc else None
    return (dataserver, database, sample_time, local_utc, remote_utc, remote_local,
            db_create, skew)


def projection_body(dialect: str) -> str:
    """The SELECT body of the dialect's column_role view (strip the CREATE/comments/GO)."""
    raw = re.sub(r"--.*", "", open(os.path.join(_SQLDIR, f"{dialect}.sql")).read())
    after = re.split(r"VIEW\s+\S*column_role\s+AS", raw, flags=re.I)[1]
    return re.split(r"\bGO\b", after)[0].strip().rstrip(";")


def capture(cursor, dialect: str, dataserver: str, database: str, sample_time,
            *, only=None) -> list[tuple]:
    """Read 2 — the full column sample. Run the column_role projection on `cursor`'s source
    and widen each row with (dataserver, database, sample_time); returns rows in `_COLS` order.

    ``only`` (a collection of object_names) **prunes** the projection to just those objects —
    the HWM/dirty-set prune: transport the wide column detail only for objects the identity
    probe (Read 1) showed changed. The IN-list is bound (qmark), so the values are parameters."""
    sel = ", ".join(_SUBSET)
    sql = f"SELECT {sel} FROM ( {projection_body(dialect)} ) AS t"
    if only is not None:
        only = list(only)
        if not only:
            return []
        sql += f" WHERE t.object_name IN ({', '.join('?' for _ in only)})"
        rows = cursor.execute(sql, only).fetchall()
    else:
        rows = cursor.execute(sql).fetchall()
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
        w.create_table("main", "schema_identity", _ID_DDL)
        w.set_partitioning("schema_identity", ["dataserver", "database"])
        w.create_table("main", "sample_clock", _CLK_DDL)
        w.set_partitioning("sample_clock", ["dataserver", "database"])
        self._w = w
        for t in ("column_role", "schema_identity", "sample_clock"):
            os.makedirs(os.path.join(data_path, "main", t), exist_ok=True)

    def record(self, rows: list[tuple], sample_time):
        """Write one capture (constant dataserver/database → partition values via min==max)."""
        if not rows:
            return
        tag = f"{rows[0][0]}__{rows[0][1]}__{sample_time:%Y%m%dT%H%M%S}".replace("/", "_")
        pq = os.path.join(self.data_path, "main", "column_role", f"{tag}.parquet")
        dl.write_rows_parquet(_DDL, rows, pq)
        self._w.register_parquet("column_role", pq, rel_path=f"{tag}.parquet", snapshot_time=sample_time)

    def record_identity(self, rows: list[tuple], sample_time):
        """Write one identity probe (Read 1) into the schema_identity time-series."""
        if not rows:
            return
        tag = f"{rows[0][0]}__{rows[0][1]}__{sample_time:%Y%m%dT%H%M%S}id".replace("/", "_")
        pq = os.path.join(self.data_path, "main", "schema_identity", f"{tag}.parquet")
        dl.write_rows_parquet(_ID_DDL, rows, pq)
        self._w.register_parquet("schema_identity", pq, rel_path=f"{tag}.parquet",
                                 snapshot_time=sample_time)

    def schema_anomalies(self, dataserver: str, database: str):
        """Detect schema events over the identity probe time-series — **order by TT
        (sample_time), detect via the object_id LT**. Per object (schema, name): a changed
        object_id => ``recreated`` (drop+create); a changed modify_date, same object_id =>
        ``altered``; first sighting => ``initial``. Returns (schema_name, object_name,
        sample_time, prev_object_id, object_id, event) for the non-``unchanged`` rows."""
        si = _SI
        part = [si.c.schema_name, si.c.object_name]
        prev_oid = func.lag(si.c.object_id).over(partition_by=part, order_by=si.c.sample_time)
        prev_mod = func.lag(si.c.modify_date).over(partition_by=part, order_by=si.c.sample_time)
        base = (select(si.c.schema_name, si.c.object_name, si.c.sample_time, si.c.object_id,
                       prev_oid.label("prev_oid"), si.c.modify_date, prev_mod.label("prev_mod"))
                .where(and_(si.c.dataserver == dataserver, si.c.database == database)).subquery())
        event = case((base.c.prev_oid.is_(None), "initial"),
                     (base.c.object_id != base.c.prev_oid, "recreated"),
                     (base.c.modify_date != base.c.prev_mod, "altered"),
                     else_="unchanged")
        ranked = select(base.c.schema_name, base.c.object_name, base.c.sample_time,
                        base.c.prev_oid, base.c.object_id, event.label("event")).subquery()
        stmt = (select(ranked.c.schema_name, ranked.c.object_name, ranked.c.sample_time,
                       ranked.c.prev_oid, ranked.c.object_id, ranked.c.event)
                .where(ranked.c.event != "unchanged")
                .order_by(ranked.c.sample_time, ranked.c.object_name))
        with dl.lake_reader(f"sqlite:{self.catalog_path}", self.data_path) as conn:
            return conn.execute(stmt).fetchall()

    def record_clock(self, env_row: tuple, sample_time):
        """Record one per-sample environment probe (skew + DB identity) — see measure_env."""
        tag = f"{env_row[0]}__{env_row[1]}__{sample_time:%Y%m%dT%H%M%S}clk".replace("/", "_")
        pq = os.path.join(self.data_path, "main", "sample_clock", f"{tag}.parquet")
        dl.write_rows_parquet(_CLK_DDL, [env_row], pq)
        self._w.register_parquet("sample_clock", pq, rel_path=f"{tag}.parquet",
                                 snapshot_time=sample_time)

    def clock_events(self, dataserver: str, database: str, *, step_threshold=120.0):
        """Environment anomalies over the sample_clock series (order by TT): a database
        drop-recreate (db_create_date changed) and a remote clock step (skew jumped by more
        than ``step_threshold`` seconds between samples). Returns (sample_time, skew_seconds,
        db_recreated, clock_stepped)."""
        sc = _SC
        order = sc.c.sample_time
        prev_dbc = func.lag(sc.c.db_create_date).over(order_by=order)
        prev_skew = func.lag(sc.c.skew_seconds).over(order_by=order)
        base = (select(sc.c.sample_time, sc.c.skew_seconds, sc.c.db_create_date,
                       prev_dbc.label("prev_dbc"), prev_skew.label("prev_skew"))
                .where(and_(sc.c.dataserver == dataserver, sc.c.database == database)).subquery())
        db_recreated = and_(base.c.prev_dbc.isnot(None), base.c.db_create_date != base.c.prev_dbc)
        clock_stepped = and_(base.c.prev_skew.isnot(None),
                             func.abs(base.c.skew_seconds - base.c.prev_skew) > step_threshold)
        stmt = (select(base.c.sample_time, base.c.skew_seconds,
                       db_recreated.label("db_recreated"), clock_stepped.label("clock_stepped"))
                .order_by(base.c.sample_time))
        with dl.lake_reader(f"sqlite:{self.catalog_path}", self.data_path) as conn:
            return conn.execute(stmt).fetchall()

    def hwm(self, dataserver: str, database: str):
        """The Read-2 pruning high-water mark: the latest modify_date seen for
        (dataserver, database). Read 2 need only re-sample objects whose create/modify_date
        exceeds this (or whose object_id is new — see schema_anomalies)."""
        si = _SI
        stmt = select(func.max(si.c.modify_date)).where(
            and_(si.c.dataserver == dataserver, si.c.database == database))
        with dl.lake_reader(f"sqlite:{self.catalog_path}", self.data_path) as conn:
            return conn.execute(stmt).scalar()

    def dirty_objects(self, dataserver: str, database: str):
        """The Read-2 prune input: object_names to re-sample = the latest identity probe
        (Read 1) diffed against the latest full capture (Read 2) by **object_id**
        (clock-independent) — objects that are new or whose object_id changed (recreated).
        In-place ALTERs (same object_id) are the modify_date signal — union in the
        schema_anomalies 'altered' set if you re-sample those too."""
        si, cr = _SI, _CR
        lp = (select(func.max(si.c.sample_time)).where(
            and_(si.c.dataserver == dataserver, si.c.database == database)).scalar_subquery())
        lc = (select(func.max(cr.c.sample_time)).where(
            and_(cr.c.dataserver == dataserver, cr.c.database == database)).scalar_subquery())
        probe_q = select(si.c.object_name, si.c.object_id).where(and_(
            si.c.dataserver == dataserver, si.c.database == database, si.c.sample_time == lp))
        full_q = select(cr.c.object_name, cr.c.object_id).where(and_(
            cr.c.dataserver == dataserver, cr.c.database == database,
            cr.c.sample_time == lc)).distinct()
        with dl.lake_reader(f"sqlite:{self.catalog_path}", self.data_path) as conn:
            probe = {n: oid for n, oid in conn.execute(probe_q).fetchall()}
            full = {n: oid for n, oid in conn.execute(full_q).fetchall()}
        return sorted(n for n, oid in probe.items() if full.get(n) != oid)

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
        stmt = (select(cr.c.schema_name, cr.c.object_name, cr.c.object_id, cr.c.grouping_kind,
                       cr.c.member_name, cr.c.ordinal, cr.c.data_type, cr.c.referenced_object,
                       cr.c.referenced_member)
                .where(and_(cr.c.dataserver == dataserver, cr.c.database == database,
                            cr.c.sample_time == latest))
                .order_by(cr.c.schema_name, cr.c.object_name, cr.c.grouping_kind, cr.c.ordinal))
        with dl.lake_reader(f"sqlite:{self.catalog_path}", self.data_path) as conn:
            return conn.execute(stmt).fetchall()
