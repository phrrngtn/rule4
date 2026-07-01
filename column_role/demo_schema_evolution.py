"""column_role-driven DuckLake schema evolution — end to end.

A SQLite source table is captured into the column_role registry; a DuckLake replica is
built from that captured schema. The source then drifts (ALTER ADD COLUMN), is re-captured,
and the replica is reconciled FROM column_role — so the registry that *records* the drift
*drives* the evolution. No DDL is mirrored by hand; the schema-as-data does it.

Run from column_role/:  uv run python demo_schema_evolution.py
"""
import datetime as dt
import os
import shutil
import sqlite3

from sqlalchemy import create_engine

import ducklake_oob_writer as dl
from registry import Registry, capture
from schema_evolution import desired_columns, reconcile_from_column_role

BASE = "/tmp/colrole_evo"
shutil.rmtree(BASE, ignore_errors=True)
os.makedirs(BASE)
SRV, DB = "localhost", "shop"

# --- SQLite source: widget(id, name) ---
src = sqlite3.connect(f"{BASE}/source.sqlite")
src.execute("CREATE TABLE widget (id INTEGER PRIMARY KEY, name TEXT)")
src.execute("INSERT INTO widget VALUES (1, 'sprocket')")
src.commit()
src_eng = create_engine(f"sqlite:///{BASE}/source.sqlite")
sconn = src_eng.connect()

# --- column_role registry + the DuckLake replica ---
reg = Registry(f"{BASE}/reg_cat.sqlite", f"{BASE}/reg_data")
rep_eng = create_engine(f"sqlite:///{BASE}/rep_cat.sqlite")
dl.create_catalog(rep_eng)
w = dl.DuckLakeWriter(rep_eng, dl.DUCKLAKE_METADATA)
w.init_catalog(data_path=f"{BASE}/rep_data")

# T1: capture the source schema, build the replica from it
T1 = dt.datetime(2026, 6, 29, 10)
reg.record(capture(sconn, "sqlite", SRV, DB, T1), T1)
cols_t1 = desired_columns(reg, SRV, DB, "widget", T1, dialect="sqlite")
print(f"T1  column_role says widget = {cols_t1}")
w.create_table("main", "widget", cols_t1)
print(f"    replica built : {[c['column_name'] for c in w.current_columns('widget')]}")

# the source schema drifts: ADD COLUMN price
src.execute("ALTER TABLE widget ADD COLUMN price REAL")
src.execute("INSERT INTO widget VALUES (2, 'gadget', 9.99)")
src.commit()

# T2: re-capture, then reconcile the replica FROM column_role
T2 = dt.datetime(2026, 6, 29, 11)
reg.record(capture(sconn, "sqlite", SRV, DB, T2), T2)
cols_t2 = desired_columns(reg, SRV, DB, "widget", T2, dialect="sqlite")
print(f"\nT2  column_role says widget = {cols_t2}")
added = reconcile_from_column_role(w, "widget", reg, SRV, DB, "widget", T2, snapshot_time=T2, dialect="sqlite")
print(f"    reconcile added: {added}")
print(f"    replica now    : {[c['column_name'] for c in w.current_columns('widget')]}")

sconn.close()
src_eng.dispose()
reg.dispose()
rep_eng.dispose()
src.close()
