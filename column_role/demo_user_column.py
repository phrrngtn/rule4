"""User-modeled transaction-time → DuckLake payload time-series (the Socrata pattern).

A source table carries its *own* transaction-time column (`updated_at`) — no CDC/CT. The
UserColumnDriver polls `WHERE updated_at > watermark`, ColumnCollection.sync staples the
rows by their `updated_at` (one DuckLake snapshot per distinct transaction-time), and the
inline replica builds the payload time-series. The acquisition seam is the only new piece;
the apply side (HistoryReplica) is unchanged.

Run from column_role/:  uv run python demo_user_column.py
"""
import os
import shutil
import sqlite3

from loguru import logger
from sqlalchemy import create_engine, text

import ducklake_oob_writer as dl
from column_collection import Col, ColumnCollection, UserColumnDriver

BASE = "/tmp/usercol"
shutil.rmtree(BASE, ignore_errors=True)
os.makedirs(BASE)
T1, T2, T3 = "2026-06-30 10:00:00", "2026-06-30 11:00:00", "2026-06-30 12:00:00"

# --- source with a user-modeled transaction-time column ---
src = sqlite3.connect(f"{BASE}/source.sqlite")
src.execute("CREATE TABLE item (id INTEGER PRIMARY KEY, name TEXT, updated_at TEXT)")
src.executemany("INSERT INTO item VALUES (?,?,?)", [(1, "a", T1), (2, "b", T1), (3, "c", T1)])
src.commit()
src_eng = create_engine(f"sqlite:///{BASE}/source.sqlite")

# the column-collection: DATA columns only — updated_at is the transaction-time (the driver's)
# Col types are the *source* dialect types (resolved via type_reference); from_column_role
# fills these from a capture — here we build them by hand for a self-contained demo.
cc = ColumnCollection("main", "item", [Col("id", "INTEGER"), Col("name", "TEXT")],
                      key="id", dialect="sqlite")
driver = UserColumnDriver("updated_at", key="id")

# --- DuckLake replica built from the column-collection ---
eng = create_engine(f"sqlite:///{BASE}/lake.sqlite")
dl.create_catalog(eng)
w = dl.DuckLakeWriter(eng, dl.DUCKLAKE_METADATA)
w.init_catalog(data_path=f"{BASE}/data")
cc.record_in_ducklake(w)
rep = dl.HistoryReplica(w, "item", "id")

# poll 1 — initial load (everything since the epoch)
with src_eng.connect() as conn:
    wm = cc.sync(conn, "2000-01-01 00:00:00", driver, rep)
logger.info("poll 1 -> watermark = {wm}", wm=wm)

# source evolves at its own pace: update id=2 (T2), insert id=4 (T3)
src.execute("UPDATE item SET name='b2', updated_at=? WHERE id=2", [T2])
src.execute("INSERT INTO item VALUES (4,'d',?)", [T3])
src.commit()

# poll 2 — incremental (only rows with updated_at > watermark)
with src_eng.connect() as conn:
    wm = cc.sync(conn, wm, driver, rep)
logger.info("poll 2 -> watermark = {wm}", wm=wm)
eng.dispose()

# --- the payload time-series in DuckLake ---
with dl.lake_reader(f"sqlite:{BASE}/lake.sqlite", f"{BASE}/data") as conn:
    def state(at=None):
        if at is None:
            stmt = text("SELECT id, name FROM lake.item ORDER BY id")
        else:
            stmt = text("SELECT id, name FROM lake.item AT (TIMESTAMP => :ts) ORDER BY id"
                        ).bindparams(ts=at)
        return dict(conn.execute(stmt).fetchall())
    logger.info("current      : {state}", state=state())
    logger.info("AT 10:30 (T1): {state}", state=state("2026-06-30 10:30:00"))
    logger.info("AT 11:30 (T2): {state}", state=state("2026-06-30 11:30:00"))
    logger.info("AT 12:30 (T3): {state}", state=state("2026-06-30 12:30:00"))
src.close()
