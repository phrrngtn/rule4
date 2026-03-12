"""
Profile the actual PG round-trip for a single large domain to find the bottleneck.
"""
import json
import time
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))

from sqlalchemy import create_engine, inspect, text, MetaData, Table, Column
from rule4.catalog import _sa_type, type_family

PG_URL = "postgresql://localhost/rule4_test"
engine = create_engine(PG_URL)
schema_name = "s_bench_domain"

# Load a big domain from the catalog JSON
with open("raw/all_socrata_catalog.json") as f:
    results = json.load(f)

# Pick datos.gov.co — the one that took 16 minutes
domain_results = [r for r in results if r.get("metadata", {}).get("domain") == "www.datos.gov.co"]
print(f"Testing with {len(domain_results)} tables from www.datos.gov.co")

# Setup
with engine.connect() as conn:
    conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
    conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))
    conn.commit()

timings = {"build_meta": 0, "create": 0, "inspect": 0, "compare": 0}
ok = fail = 0

t_total = time.time()

for i, r in enumerate(domain_results):
    res = r.get("resource", {})
    table_name = res.get("id", "")
    field_names = res.get("columns_field_name") or []
    datatypes = res.get("columns_datatype") or []
    if not table_name or not field_names:
        continue

    # Build SA table
    t0 = time.time()
    schema_meta = MetaData(schema=schema_name)
    columns = []
    for j, fname in enumerate(field_names):
        dtype = datatypes[j] if j < len(datatypes) else "Text"
        columns.append(Column(fname, _sa_type(dtype)))
    tbl = Table(table_name, schema_meta, *columns)
    t1 = time.time()
    timings["build_meta"] += t1 - t0

    # CREATE
    try:
        schema_meta.create_all(engine, tables=[tbl])
    except Exception as e:
        fail += 1
        if fail <= 3:
            print(f"  CREATE fail: {table_name}: {e}")
        continue
    t2 = time.time()
    timings["create"] += t2 - t1

    # INSPECT
    insp = inspect(engine)
    cols_back = insp.get_columns(table_name, schema=schema_name)
    t3 = time.time()
    timings["inspect"] += t3 - t2

    # COMPARE
    readback = {c["name"]: type(c["type"]).__name__ for c in cols_back}
    original_names = {c.name for c in tbl.columns}
    if original_names == set(readback.keys()):
        ok += 1
    else:
        fail += 1
    t4 = time.time()
    timings["compare"] += t4 - t3

    if (i + 1) % 100 == 0:
        elapsed = time.time() - t_total
        print(f"  [{i+1:3d}/{len(domain_results)}] {elapsed:.1f}s elapsed  "
              f"create={timings['create']:.1f}s  inspect={timings['inspect']:.1f}s", flush=True)

elapsed = time.time() - t_total

# Cleanup
with engine.connect() as conn:
    conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
    conn.commit()
engine.dispose()

print(f"\nResults: {ok} OK, {fail} fail, {elapsed:.1f}s total")
print(f"Breakdown:")
for step, t in timings.items():
    print(f"  {step:12s}: {t:.2f}s ({t/elapsed*100:.0f}%)")
