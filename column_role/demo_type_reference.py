"""Type mapping as reference data: resolve a captured column's types via a JOIN, not Python.

Capture a source's schema into column_role, seed the type_reference table into the SAME
metadatabase catalog, then resolve every column's SA type / DuckLake type / extraction
transform with a single JOIN — no per-type code, no dict.

Run from column_role/:  uv run python demo_type_reference.py
"""
import datetime as dt
import os
import shutil
import sqlite3

from sqlalchemy import create_engine

import ducklake_oob_writer as dl
from registry import Registry, capture
from type_reference import resolve_query, seed_into

BASE = "/tmp/typeref"
shutil.rmtree(BASE, ignore_errors=True)
os.makedirs(BASE)
SRV, DB, T = "localhost", "shop", dt.datetime(2026, 6, 29, 10)

# a source with a few telling types (incl. a BLOB that needs a transform)
src = sqlite3.connect(f"{BASE}/source.sqlite")
src.execute("CREATE TABLE widget (id INTEGER PRIMARY KEY, name TEXT, price REAL, payload BLOB)")
src.commit()

# column_role + type_reference in ONE metadatabase catalog
reg = Registry(f"{BASE}/cat.sqlite", f"{BASE}/data")
src_eng = create_engine(f"sqlite:///{BASE}/source.sqlite")
with src_eng.connect() as sconn:
    reg.record(capture(sconn, "sqlite", SRV, DB, T), T)
seed_into(reg._w, reg.data_path, T)

# resolve via a JOIN — column_role ⋈ type_reference, no Python type dict (SA Core)
with dl.lake_reader(f"sqlite:{BASE}/cat.sqlite", f"{BASE}/data") as conn:
    rows = conn.execute(resolve_query("sqlite", SRV, DB)).fetchall()

print("column           data_type  ->  sa_type / ducklake_type / transform   (odbc)")
for obj, col, dtype, odbc, sa, dlt, xf, lob in rows:
    print(f"  {col:<8} {dtype:<9}  ->  {sa} / {dlt} / {xf or '-'}"
          f"   ({odbc}{', LOB' if lob else ''})")

src_eng.dispose()
reg.dispose()
src.close()
