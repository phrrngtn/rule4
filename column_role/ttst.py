"""TTST projector — DuckLake tt-history -> a relational transaction-time state table (db').

The **outbound** mirror of the scrapers: where a driver fills DuckLake from a source, this
drains DuckLake into a transaction-time state table you own (SQL Server / PG / anything), by
the same **poll-since-HWM** pattern reversed. DuckLake's inline MVCC already stores
``(begin_snapshot, end_snapshot)`` — Snodgrass tt-intervals — so projection is literally
copying intervals out and MERGE-ing them into the TTST, closing open intervals as they close.

The **tt is the logical clock** (the DuckLake snapshot_id), not the wallclock — so the HWM
``greatest(max(tt_start), max(tt_end))`` is skew-immune. The snapshot_time is carried too
(``tt_start_ts`` / ``tt_end_ts``) for human point-in-time-by-time queries. Loose about PK/FK;
indexed for the two use-cases: **as-of-latest** (filtered index on the open intervals) and
**PIT reconstruction** (range index on the interval bounds).
"""
from sqlalchemy import and_, or_, select

import ducklake_oob_writer as dl
from ducklake_oob_writer import inlined
from ducklake_oob_writer.catalog import DUCKLAKE_METADATA


def create_ttst_ddl(ttst, data_cols, key):
    """SQL Server DDL for the TTST: data columns + the tt-interval bookkeeping, plus the two
    serving indexes. ``data_cols`` = [(name, sqlserver_type), …]."""
    cols = ",\n  ".join(f"[{n}] {t}" for n, t in data_cols)
    t = ttst.split(".")[-1]
    return [
        f"CREATE TABLE {ttst} (\n  {cols},\n"
        "  tt_start BIGINT NOT NULL, tt_start_ts DATETIME2 NULL,\n"
        "  tt_end BIGINT NULL, tt_end_ts DATETIME2 NULL)",
        # as-of-latest: the current state is the open intervals
        f"CREATE INDEX ix_{t}_current ON {ttst} ([{key}]) WHERE tt_end IS NULL",
        # PIT reconstruction: WHERE tt_start <= @T AND (tt_end > @T OR tt_end IS NULL)
        f"CREATE INDEX ix_{t}_pit ON {ttst} (tt_start, tt_end) INCLUDE ([{key}])",
    ]


def ttst_hwm(cur, ttst):
    """The watermark read from the TTST: greatest(max(tt_start), max(tt_end)). Open intervals
    (tt_end NULL) don't advance the end side. 0 for an empty TTST."""
    row = cur.execute(f"SELECT COALESCE(MAX(tt_start), 0), COALESCE(MAX(tt_end), 0) FROM {ttst}").fetchone()
    return max(row[0], row[1])


def extract_intervals(catalog_engine, table_name, data_cols, since):
    """Read DuckLake's tt-intervals for ``table_name`` whose bound crossed ``since`` (the HWM):
    new opens (begin_snapshot > since) and newly-closed (end_snapshot > since). ``data_cols`` =
    [(name, ducklake_type), …]. Returns dicts with the data columns plus __begin/__end (snapshot
    ids) and __begin_ts/__end_ts (snapshot times)."""
    tbl = DUCKLAKE_METADATA.tables["ducklake_table"]
    snap = DUCKLAKE_METADATA.tables["ducklake_snapshot"]
    reg = inlined.REGISTRY
    names = [c for c, _ in data_cols]
    with catalog_engine.connect() as conn:
        tid = conn.execute(select(tbl.c.table_id).where(and_(
            tbl.c.table_name == table_name, tbl.c.end_snapshot.is_(None)))).scalar()
        out = []
        for inline_name in conn.execute(select(reg.c.table_name).where(reg.c.table_id == tid)).scalars():
            idata = inlined.data_table(inline_name, data_cols)
            b, e = snap.alias("b"), snap.alias("e")
            q = (select(*[idata.c[n] for n in names],
                        idata.c.begin_snapshot, idata.c.end_snapshot,
                        b.c.snapshot_time.label("begin_ts"), e.c.snapshot_time.label("end_ts"))
                 .select_from(idata.join(b, b.c.snapshot_id == idata.c.begin_snapshot)
                              .outerjoin(e, e.c.snapshot_id == idata.c.end_snapshot))
                 .where(or_(idata.c.begin_snapshot > since, idata.c.end_snapshot > since)))
            for r in conn.execute(q).mappings():
                out.append({**{n: r[n] for n in names}, "__begin": r["begin_snapshot"],
                            "__end": r["end_snapshot"], "__begin_ts": r["begin_ts"],
                            "__end_ts": r["end_ts"]})
    return out


def project(cur, ttst, data_cols, key, intervals, hwm):
    """MERGE the extracted intervals into the TTST (bound values). begin > hwm ⇒ a new interval
    to INSERT (its tt_end may be open or already closed); begin ≤ hwm < end ⇒ close the
    matching open interval. Returns (inserted, closed)."""
    names = [c for c, _ in data_cols]
    cols_sql = ", ".join(f"[{n}]" for n in names)
    ph = ", ".join("?" for _ in names)
    inserted = closed = 0
    for iv in intervals:
        if iv["__begin"] > hwm:
            cur.execute(
                f"INSERT INTO {ttst} ({cols_sql}, tt_start, tt_start_ts, tt_end, tt_end_ts) "
                f"VALUES ({ph}, ?, ?, ?, ?)",
                [iv[n] for n in names] + [iv["__begin"], iv["__begin_ts"], iv["__end"], iv["__end_ts"]])
            inserted += 1
        else:   # begin <= hwm < end: close the interval opened in a prior sync
            cur.execute(
                f"UPDATE {ttst} SET tt_end = ?, tt_end_ts = ? "
                f"WHERE [{key}] = ? AND tt_start = ? AND tt_end IS NULL",
                [iv["__end"], iv["__end_ts"], iv[key], iv["__begin"]])
            closed += 1
    return inserted, closed


def sync(cur, catalog_engine, table_name, ttst, data_cols_dl, key):
    """One outbound sync: read the TTST HWM, extract DuckLake intervals past it, MERGE."""
    hwm = ttst_hwm(cur, ttst)
    intervals = extract_intervals(catalog_engine, table_name, data_cols_dl, hwm)
    return {"hwm": hwm, **dict(zip(("inserted", "closed"),
                                   project(cur, ttst, data_cols_dl, key, intervals, hwm)))}
