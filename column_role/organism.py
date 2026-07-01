"""The replica organism — schema + payload + change-detection wired into one flow.

Point at a **read-only** source; render it as a local append-only DuckLake TTST, kept fresh by
polling. The source is only ever *read*: its schema (the ``column`` essence, via ``catalog_meta``)
shapes and evolves the replica table, and a driver streams its payload into a ``HistoryReplica``.
One ``Replica.sync()`` is one poll -> apply cycle.

**All control/status — the watermark, last-sync, the schema fingerprint — lives in a pluggable
CONTROL store, never in the source.** Two backends:

- ``ReplicaControl`` — the common case: state lives in the replica's own DuckLake, appended per
  sync (so the watermark trail is itself bitemporal history) and read latest-wins.
- ``RegistryControl`` — a third-party registry, for when the replica target is administratively
  locked down (you're replicating a *subset* into a schema you can't extend with a control table).

Either way the source carries no bookkeeping and is never written.
"""
import hashlib
import json
import os

from sqlalchemy import (Column, DateTime, MetaData, String, Table, and_, select, text)

import catalog_meta as cm
import ducklake_oob_writer as dl
from column_collection import Col, ColumnCollection

_CTL = MetaData()
_SYNC_STATE = Table(
    "sync_state", _CTL,
    Column("source", String), Column("database", String), Column("schema_name", String),
    Column("table_name", String), Column("watermark", String),
    Column("col_fingerprint", String), Column("last_sync", DateTime))


class RegistryControl:
    """Sync state in a separate registry DB — for when the replica target is locked down."""

    def __init__(self, engine):
        self.eng = engine
        _CTL.create_all(engine)

    def _where(self, s, d, sc, t):
        c = _SYNC_STATE.c
        return and_(c.source == s, c.database == d, c.schema_name == sc, c.table_name == t)

    def get_state(self, s, d, sc, t):
        with self.eng.connect() as c:
            row = c.execute(select(_SYNC_STATE.c.watermark, _SYNC_STATE.c.col_fingerprint)
                            .where(self._where(s, d, sc, t))).fetchone()
        return (row.watermark, row.col_fingerprint) if row else (None, None)

    def set_state(self, s, d, sc, t, watermark, fp, now):
        with self.eng.begin() as c:
            c.execute(_SYNC_STATE.delete().where(self._where(s, d, sc, t)))
            c.execute(_SYNC_STATE.insert().values(source=s, database=d, schema_name=sc,
                      table_name=t, watermark=str(watermark), col_fingerprint=fp, last_sync=now))


class ReplicaControl:
    """Sync state in the replica's own DuckLake (the common case). Appended per sync — so the
    watermark trail is bitemporal history — and read latest-wins by last_sync."""

    _DDL = [("source", "varchar"), ("database", "varchar"), ("schema_name", "varchar"),
            ("table_name", "varchar"), ("watermark", "varchar"), ("col_fingerprint", "varchar"),
            ("last_sync", "varchar")]

    def __init__(self, writer, catalog, data_path):
        self.w, self.catalog, self.data_path, self._n = writer, catalog, data_path, 0
        if "_sync_state" not in {t["table_name"] for t in writer.current_tables()}:
            writer.create_table("main", "_sync_state", self._DDL)
        os.makedirs(os.path.join(data_path, "main", "_sync_state"), exist_ok=True)

    def get_state(self, s, d, sc, t):
        with dl.lake_reader(self.catalog, self.data_path) as lc:
            row = lc.execute(
                text("SELECT watermark, col_fingerprint FROM lake._sync_state "
                     "WHERE source = :s AND database = :d AND schema_name = :sc AND table_name = :t "
                     "ORDER BY last_sync DESC LIMIT 1"),
                {"s": s, "d": d, "sc": sc, "t": t}).fetchone()
        return (row.watermark, row.col_fingerprint) if row else (None, None)

    def set_state(self, s, d, sc, t, watermark, fp, now):
        self._n += 1
        pq = os.path.join(self.data_path, "main", "_sync_state", f"_sync_state_{self._n}.parquet")
        dl.write_rows_parquet(self._DDL, [(s, d, sc, t, str(watermark), fp, str(now))], pq)
        self.w.register_parquet("_sync_state", pq, rel_path=os.path.basename(pq), snapshot_time=now)


def _fingerprint(cc):
    return hashlib.sha256(
        json.dumps([(c.name, c.source_type) for c in cc.columns], sort_keys=True).encode()
    ).hexdigest()[:16]


def scrape_schema(source_conn, reg_conn, dialect, schema, table, key, exclude=()):
    """Read the source's current column essence (the generated projection) and assemble a
    ColumnCollection — the schema that shapes the replica. ``exclude`` drops driver-owned control
    columns (a user-modeled tt column becomes the snapshot's transaction-time, not a data column).
    Read-only on the source."""
    stmt = cm.generate_projection(reg_conn, dialect, "column")
    rows = sorted((r for r in source_conn.execute(stmt)
                   if r.schema_name == schema and r.object_name == table
                   and r.member_name not in exclude),
                  key=lambda r: r.ordinal)
    return ColumnCollection(schema, table,
                            [Col(r.member_name, r.data_type, r.ordinal, dialect) for r in rows],
                            key=key, dialect=dialect)


def _facet_matches(m, schema, table):
    if m.get("object_name") != table:
        return False
    sc = m.get("object_schema", m.get("schema_name"))
    return sc is None or sc == schema


def capture_facet(writer, reg_conn, source_conn, dialect, schema, table, facet, now):
    """Scrape a facet essence (extended_property, stats_histogram, …) for the replicated object
    and append it as a snapshot into a ``{table}__{facet}`` DuckLake table — metadata rendered as
    data, stamped ``captured_at`` and bitemporal (each sync is a new version). Read-only on the
    source; a full snapshot per sync (no watermark — facets are current-state metadata)."""
    stmt = cm.generate_projection(reg_conn, dialect, facet)
    cols = [c.name for c in stmt.selected_columns]
    rows = [dict(r._mapping) for r in source_conn.execute(stmt)
            if _facet_matches(r._mapping, schema, table)]
    ftable = f"{table}__{facet}"
    if ftable not in {t["table_name"] for t in writer.current_tables()}:
        writer.create_table("main", ftable, [(c, "varchar") for c in cols] + [("captured_at", "varchar")])
    payload = [{**{c: (None if r.get(c) is None else str(r.get(c))) for c in cols},
                "captured_at": str(now)} for r in rows]
    if payload:
        writer.inline_rows(ftable, payload, schema_name="main", snapshot_time=now)
    return len(payload)


class Replica:
    """One replicated table: read-only source -> DuckLake TTST, state in a pluggable ControlStore.
    ``facets`` are essence names captured as metadata snapshots into the replica alongside the
    payload each sync (extended properties, stats histograms, …)."""

    def __init__(self, control, reg_engine, source_engine, lake_writer, *, source, database,
                 dialect, schema, table, key, driver, initial_watermark, facets=()):
        self.control, self.reg, self.src = control, reg_engine, source_engine
        self.w, self.driver, self.initial_watermark = lake_writer, driver, initial_watermark
        self.source, self.database, self.dialect = source, database, dialect
        self.schema, self.table, self.key, self.facets = schema, table, key, facets

    def sync(self, now):
        """One poll -> apply cycle. Reads the watermark from control, evolves the replica table to
        the current source schema, streams the payload, advances the watermark in control."""
        k = (self.source, self.database, self.schema, self.table)
        exclude = {getattr(self.driver, "tt_column", None),
                   getattr(self.driver, "deleted_col", None)} - {None}
        with self.src.connect() as sconn, self.reg.connect() as rconn:
            cc = scrape_schema(sconn, rconn, self.dialect, self.schema, self.table, self.key, exclude)
            fp = _fingerprint(cc)
            watermark, prev_fp = self.control.get_state(*k)
            cc.record_in_ducklake(self.w, snapshot_time=now)   # create the replica table, or evolve it
            rep = dl.HistoryReplica(self.w, self.table, self.key)
            new_wm = cc.sync(sconn, watermark if watermark is not None else self.initial_watermark,
                             self.driver, rep, source_schema=self.schema)
            facets = {f: capture_facet(self.w, rconn, sconn, self.dialect, self.schema, self.table,
                                       f, now) for f in self.facets}
        self.control.set_state(*k, new_wm, fp, now)
        return {"watermark": new_wm, "first": prev_fp is None,
                "evolved": prev_fp is not None and fp != prev_fp, "facets": facets}
