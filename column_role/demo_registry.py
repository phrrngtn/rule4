"""Capstone demo: a multi-source, temporal schema registry built from column_role.

Captures three live sources (SQLite — with a schema change between two samples — DuckDB,
and the live PostgreSQL rule4_test) into ONE DuckLake registry partitioned on
(dataserver, database), then demonstrates: the sources present, transaction-time
`schema_as_of`, source-pruning, and partition pruning.

Run from the rule4 repo:  uv run python column_role/demo_registry.py
"""
import datetime as dt
import os
import shutil
import sqlite3

import duckdb

import ducklake_oob_writer as dl
from registry import Registry, capture

BASE = "/tmp/cr_registry"
shutil.rmtree(BASE, ignore_errors=True)
os.makedirs(BASE)
reg = Registry(f"{BASE}/registry.sqlite", f"{BASE}/data")

T1 = dt.datetime(2026, 6, 29, 10, 0, 0)
T2 = dt.datetime(2026, 6, 29, 11, 0, 0)

# --- SQLite source: capture, evolve the schema, capture again (the temporal axis) ---
sq = sqlite3.connect(f"{BASE}/src.sqlite")
sq.executescript("CREATE TABLE region(id INTEGER PRIMARY KEY, name TEXT);"
                 "CREATE TABLE customer(id INTEGER PRIMARY KEY, name TEXT NOT NULL,"
                 " region_id INTEGER REFERENCES region(id));")
reg.record(capture(sq, "sqlite", "sqlite_local", "src.sqlite", T1), T1)
sq.execute("ALTER TABLE customer ADD COLUMN email TEXT")               # schema evolves
reg.record(capture(sq, "sqlite", "sqlite_local", "src.sqlite", T2), T2)
sq.close()

# --- DuckDB source: a different schema (another source in the same registry) ---
dd = duckdb.connect(f"{BASE}/src.duckdb")
dd.execute("CREATE TABLE sales(id BIGINT PRIMARY KEY, amount DECIMAL(10,2), ts TIMESTAMP)")
reg.record(capture(dd, "duckdb", "duckdb_local", "src.duckdb", T1), T1)
dd.close()

# --- live PostgreSQL (the real heterogeneous source) ---
try:
    import pyodbc
    pg = pyodbc.connect("DSN=rule4_test", timeout=10)
    rows = capture(pg, "postgresql", "pg_localhost", "rule4_test", T1)
    reg.record(rows, T1)
    pg.close()
    print(f"captured live PostgreSQL rule4_test: {len(rows):,} column_role rows")
except Exception as e:
    print("PG capture skipped:", str(e)[:90])

# === demonstrate ===
print("\n== sources in the registry ==")
for ds, db in reg.query("SELECT DISTINCT dataserver, database FROM lake.column_role ORDER BY 1,2"):
    print(f"   {ds} / {db}")

print("\n== TEMPORAL: sqlite_local/src.sqlite customer columns, as-of T1 vs T2 ==")
def customer_cols(T):
    return [r[3] for r in reg.schema_as_of("sqlite_local", "src.sqlite", T)
            if r[1] == "customer" and r[2] == "table"]
print("   as-of T1 (10:00):", customer_cols(T1))
print("   as-of T2 (11:00):", customer_cols(T2), " <- 'email' appeared")

print("\n== SOURCE-PRUNING: schema_as_of(duckdb_local) returns only that source ==")
ddrows = reg.schema_as_of("duckdb_local", "src.duckdb", T2)
print("   objects:", sorted({r[1] for r in ddrows}), "| rows:", len(ddrows))

print("\n== PARTITION PRUNING ('transitive data'): files touched for one source ==")
with dl.attach_lake(f"sqlite:{reg.catalog_path}", reg.data_path) as c:
    plan = c.execute("EXPLAIN ANALYZE SELECT count(*) FROM lake.column_role "
                     "WHERE dataserver = 'duckdb_local'").fetchall()[-1][-1]
import re
m = re.search(r"Total Files Read:\s*\d+", plan)
print("   query WHERE dataserver='duckdb_local' ->", m.group(0) if m else "(n/a)",
      "(of all captures — pruned by the partition column)")
reg.dispose()
