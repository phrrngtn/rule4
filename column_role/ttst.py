"""TTST projector — DuckLake tt-history -> a relational transaction-time state table (db').

The **outbound** mirror of the scrapers: where a driver fills DuckLake from a source, this
drains DuckLake into a transaction-time state table you own (SQL Server / PG / SQLite / …), by
the same **poll-since-HWM** pattern reversed. DuckLake's inline MVCC already stores
``(begin_snapshot, end_snapshot)`` — Snodgrass tt-intervals — so projection copies intervals
out and applies them to the TTST: INSERT new opens, UPDATE to close intervals as they close
(distinct rows, so no key-conflict UPSERT is needed — plain SA Core insert()/update()).

Everything is **SQLAlchemy expression language**, so it renders for whatever the destination
dialect is. The one genuinely dialect-specific thing — the *partial index* backing as-of-latest
(``WHERE tt_end IS NULL``) — is expressed by peeking at the connection's dialect and using its
``<dialect>_where`` option. The **tt is the DuckLake snapshot_id** (the logical clock), so the
HWM ``greatest(max(tt_start), max(tt_end))`` is skew-immune; the snapshot_time is carried too
(``tt_start_ts`` / ``tt_end_ts``) for point-in-time-by-time queries.
"""
from sqlalchemy import (BigInteger, Column, DateTime, Index, MetaData, Table, and_, func,
                        or_, select, text)

from ducklake_oob_writer import inlined
from ducklake_oob_writer.catalog import DUCKLAKE_METADATA


def ttst_table(name, data_cols, *, schema=None, metadata=None):
    """A SQLAlchemy Core ``Table`` for the TTST: the data columns + the tt-interval
    bookkeeping. ``data_cols`` = [(name, sa_type), …]."""
    md = metadata if metadata is not None else MetaData()
    cols = [Column(n, t) for n, t in data_cols] + [
        Column("tt_start", BigInteger, nullable=False), Column("tt_start_ts", DateTime),
        Column("tt_end", BigInteger), Column("tt_end_ts", DateTime)]
    return Table(name, md, *cols, schema=schema)


def create_ttst(conn, tbl, key):
    """Create the TTST + its two serving indexes on ``conn``. Dialect-aware via SA: the
    as-of-latest index is a *partial* index over the open intervals, expressed with the
    connection dialect's ``<dialect>_where`` option; the PIT index ranges the interval bounds."""
    tbl.create(conn, checkfirst=True)
    dialect = conn.dialect.name
    partial = ({f"{dialect}_where": text("tt_end IS NULL")}
               if dialect in ("mssql", "postgresql", "sqlite") else {})
    Index(f"ix_{tbl.name}_current", tbl.c[key], **partial).create(conn)   # as-of-latest
    Index(f"ix_{tbl.name}_pit", tbl.c.tt_start, tbl.c.tt_end).create(conn)  # PIT range


def ttst_hwm(conn, tbl):
    """The watermark read from the TTST: greatest(max(tt_start), max(tt_end)); 0 when empty."""
    a, b = conn.execute(select(func.coalesce(func.max(tbl.c.tt_start), 0),
                               func.coalesce(func.max(tbl.c.tt_end), 0))).one()
    return max(a, b)


def extract_intervals(catalog_engine, table_name, data_cols, since):
    """Read DuckLake's tt-intervals for ``table_name`` whose bound crossed ``since`` (the HWM):
    new opens (begin_snapshot > since) and newly-closed (end_snapshot > since). ``data_cols`` =
    [(name, ducklake_type), …]. Returns dicts of the data columns plus __begin/__end (snapshot
    ids) and __begin_ts/__end_ts (snapshot times)."""
    tbl = DUCKLAKE_METADATA.tables["ducklake_table"]
    snap = DUCKLAKE_METADATA.tables["ducklake_snapshot"]
    reg, names = inlined.REGISTRY, [c for c, _ in data_cols]
    out = []
    with catalog_engine.connect() as conn:
        tid = conn.execute(select(tbl.c.table_id).where(and_(
            tbl.c.table_name == table_name, tbl.c.end_snapshot.is_(None)))).scalar()
        for inline_name in conn.execute(select(reg.c.table_name).where(reg.c.table_id == tid)).scalars():
            idata = inlined.data_table(inline_name, data_cols)
            b, e = snap.alias("b"), snap.alias("e")
            q = (select(*[idata.c[n] for n in names],
                        idata.c.begin_snapshot, idata.c.end_snapshot,
                        b.c.snapshot_time.label("begin_ts"), e.c.snapshot_time.label("end_ts"))
                 .select_from(idata.join(b, b.c.snapshot_id == idata.c.begin_snapshot)
                              .outerjoin(e, e.c.snapshot_id == idata.c.end_snapshot))
                 .where(or_(idata.c.begin_snapshot > since, idata.c.end_snapshot > since)))
            out += [{**{n: r[n] for n in names}, "__begin": r["begin_snapshot"],
                     "__end": r["end_snapshot"], "__begin_ts": r["begin_ts"],
                     "__end_ts": r["end_ts"]} for r in conn.execute(q).mappings()]
    return out


def project(conn, tbl, key, names, intervals, hwm):
    """Apply the intervals to the TTST via SA Core (dialect-independent): begin > hwm ⇒ a new
    interval to INSERT (tt_end may be open or already closed); begin ≤ hwm < end ⇒ close the
    matching open interval. Returns (inserted, closed)."""
    inserted = closed = 0
    for iv in intervals:
        if iv["__begin"] > hwm:
            conn.execute(tbl.insert().values(
                **{n: iv[n] for n in names}, tt_start=iv["__begin"], tt_start_ts=iv["__begin_ts"],
                tt_end=iv["__end"], tt_end_ts=iv["__end_ts"]))
            inserted += 1
        else:
            conn.execute(tbl.update().where(and_(
                tbl.c[key] == iv[key], tbl.c.tt_start == iv["__begin"], tbl.c.tt_end.is_(None)))
                .values(tt_end=iv["__end"], tt_end_ts=iv["__end_ts"]))
            closed += 1
    return inserted, closed


def sync(conn, catalog_engine, table_name, tbl, key, data_cols_dl):
    """One outbound sync on ``conn``: read the TTST HWM, extract DuckLake intervals past it,
    apply. ``data_cols_dl`` = the DuckLake-typed data columns (for the inline read)."""
    hwm = ttst_hwm(conn, tbl)
    intervals = extract_intervals(catalog_engine, table_name, data_cols_dl, hwm)
    ins, cl = project(conn, tbl, key, [c for c, _ in data_cols_dl], intervals, hwm)
    return {"hwm": hwm, "inserted": ins, "closed": cl}
