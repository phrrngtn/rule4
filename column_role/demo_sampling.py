"""Self-hosted DuckLake replicas: a sampling table provisions replicas from column_role.

Three applications of DuckLake, stacked:
  (1) column_role  — a time-series of *schema* (the columns of a source, over time).
  (2) sampling     — the control plane: which sources become which replicas (also a
                     DuckLake table; lives in the same catalog as the replicas).
  (3) the replica  — a time-series of *payload* (the rows of the source, polled from
                     CDC/CT/backlog).
provision() reads (1)+(2) to build (3)'s *structure*; Replica.apply() fills (3)'s payload.
The sampling table and the replicas it creates share ONE DuckLake catalog — self-hosting.

Run from column_role/:  uv run python demo_sampling.py
"""
import datetime as dt
import os
import shutil
import sqlite3

from sqlalchemy import create_engine

import ducklake_oob_writer as dl
from registry import Registry, capture
from sampling import SamplingPlan, provision

BASE = "/tmp/colrole_sampling"
shutil.rmtree(BASE, ignore_errors=True)
os.makedirs(BASE)
SRV, DB = "localhost", "shop"
T = dt.datetime(2026, 6, 29, 10)

# --- the source ---
src = sqlite3.connect(f"{BASE}/source.sqlite")
src.execute("CREATE TABLE widget (id INTEGER PRIMARY KEY, name TEXT, price REAL)")
src.executemany("INSERT INTO widget VALUES (?,?,?)",
                [(1, "sprocket", 1.5), (2, "gadget", 9.99)])
src.commit()

# --- (1) column_role: the schema time-series ---
reg = Registry(f"{BASE}/reg_cat.sqlite", f"{BASE}/reg_data")
src_eng = create_engine(f"sqlite:///{BASE}/source.sqlite")
with src_eng.connect() as sconn:
    reg.record(capture(sconn, "sqlite", SRV, DB, T), T)

# --- (2) sampling: the control plane (a DuckLake table that hosts its own replicas) ---
plan = SamplingPlan(f"{BASE}/lake_cat.sqlite", f"{BASE}/lake_data")
plan.declare([{"dataserver": SRV, "database": DB, "source_object": "widget",
               "key_column": "id", "mode": "net"}], T)
print("sampling plan :", plan.specs())

# provision: read (1)+(2) -> build (3)'s structure, self-hosted in the plan's catalog
print("provisioned   :", provision(plan, reg, T, dialect="sqlite"))
print("catalog tables:", [t["table_name"] for t in plan.writer.current_tables()])
print("widget columns:", [c["column_name"] for c in plan.writer.current_columns("widget")])

# --- (3) payload: fill the provisioned replica from the source (the Replica apply) ---
rep = dl.Replica(plan.writer, "widget", "id")
rows = src.execute("SELECT id, name, price FROM widget").fetchall()
rep.apply(upserts=[{"id": r[0], "name": r[1], "price": r[2]} for r in rows], snapshot_time=T)

from sqlalchemy import column, select, table  # noqa: E402

_widget = table("widget", column("id"), column("name"), column("price"), schema="lake")
_samp = table("sampling", column("source_object"), column("target_table"), column("mode"), schema="lake")
with dl.lake_reader(f"sqlite:{BASE}/lake_cat.sqlite", f"{BASE}/lake_data") as conn:
    print("\nreplica payload (3)  :",
          conn.execute(select(_widget).order_by(_widget.c.id)).fetchall())
    print("sampling table  (2)  : same catalog ->",
          conn.execute(select(_samp)).fetchall())

src_eng.dispose()
reg.dispose()
plan.dispose()
src.close()
