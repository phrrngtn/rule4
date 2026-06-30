"""Trigger-maintained backlog -> DuckLake payload time-series (the after-image backlog).

A real SQLite source with AFTER INSERT/UPDATE/DELETE triggers that append every after-image
(plus its op and transaction-time) to a backlog table — Snodgrass's backlog relation. The
BacklogDriver polls it; ColumnCollection.sync staples by ts (one DuckLake snapshot per
distinct transaction-time); HistoryReplica reconstructs.

The point of a backlog over a user-column / net poll: it keeps **intermediate versions**.
Here id=2 goes b -> b2 -> b3; a current-state poll would only ever see b3, but the backlog
(and therefore the DuckLake time-series) retains b2. The triggers stamp ts from a one-row
`clock` table bumped before each change, so the transaction-times are real and distinct.

Run from column_role/:  uv run python demo_backlog.py
"""
import os
import shutil
import sqlite3

from loguru import logger
from sqlalchemy import create_engine, text

import ducklake_oob_writer as dl
from column_collection import BacklogDriver, Col, ColumnCollection

BASE = "/tmp/backlog"
shutil.rmtree(BASE, ignore_errors=True)
os.makedirs(BASE)
T1, T2, T3, T4 = ("2026-06-30 10:00:00", "2026-06-30 11:00:00",
                  "2026-06-30 12:00:00", "2026-06-30 13:00:00")

# --- source: live table + trigger-maintained after-image backlog ---
src = sqlite3.connect(f"{BASE}/source.sqlite")
src.executescript("""
CREATE TABLE clock (now TEXT);
INSERT INTO clock VALUES ('1970-01-01 00:00:00');
CREATE TABLE item (id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE item_log (seq INTEGER PRIMARY KEY AUTOINCREMENT,
                       id INTEGER, name TEXT, op TEXT, ts TEXT);
CREATE TRIGGER item_ai AFTER INSERT ON item BEGIN
  INSERT INTO item_log (id, name, op, ts) SELECT NEW.id, NEW.name, 'I', now FROM clock; END;
CREATE TRIGGER item_au AFTER UPDATE ON item BEGIN
  INSERT INTO item_log (id, name, op, ts) SELECT NEW.id, NEW.name, 'U', now FROM clock; END;
CREATE TRIGGER item_ad AFTER DELETE ON item BEGIN
  INSERT INTO item_log (id, name, op, ts) SELECT OLD.id, OLD.name, 'D', now FROM clock; END;
""")


def at(ts, *stmts):
    src.execute("UPDATE clock SET now = ?", [ts])
    for s in stmts:
        src.execute(s)
    src.commit()


at(T1, "INSERT INTO item VALUES (1,'a'),(2,'b'),(3,'c')")
at(T2, "UPDATE item SET name='b2' WHERE id=2")
at(T3, "UPDATE item SET name='b3' WHERE id=2")          # intermediate version b2 -> b3
at(T4, "DELETE FROM item WHERE id=3", "INSERT INTO item VALUES (4,'d')")

# --- the column-collection (data columns only) + the backlog driver ---
cc = ColumnCollection("main", "item", [Col("id", "INTEGER"), Col("name", "TEXT")],
                      key="id", dialect="sqlite")
driver = BacklogDriver("item_log", "ts", "op", key="id")

# --- DuckLake history replica ---
eng = create_engine(f"sqlite:///{BASE}/lake.sqlite")
dl.create_catalog(eng)
w = dl.DuckLakeWriter(eng, dl.DUCKLAKE_METADATA)
w.init_catalog(data_path=f"{BASE}/data")
cc.record_in_ducklake(w)
rep = dl.HistoryReplica(w, "item", "id")

src_eng = create_engine(f"sqlite:///{BASE}/source.sqlite")
with src_eng.connect() as conn:
    wm = cc.sync(conn, "1970-01-01 00:00:00", driver, rep)
logger.info("replayed backlog -> watermark = {wm}", wm=wm)
src_eng.dispose()
eng.dispose()

# --- the full history, intermediate versions included ---
with dl.lake_reader(f"sqlite:{BASE}/lake.sqlite", f"{BASE}/data") as conn:
    def state(at=None):
        if at is None:
            stmt = text("SELECT id, name FROM lake.item ORDER BY id")
        else:
            stmt = text("SELECT id, name FROM lake.item AT (TIMESTAMP => :ts) ORDER BY id"
                        ).bindparams(ts=at)
        return dict(conn.execute(stmt).fetchall())
    logger.info("current      : {state}", state=state())
    logger.info("AT T1        : {state}", state=state(T1))
    logger.info("AT T2        : {state}", state=state(T2))
    logger.info("AT T3 (b3)   : {state}  <- b2 at T2 retained, not skipped", state=state(T3))
    logger.info("AT T4        : {state}", state=state(T4))
src.close()
