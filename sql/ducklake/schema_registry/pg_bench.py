"""
Diagnose PostgreSQL round-trip performance.
Tests: create schema, create table, inspect, drop — with timing per step.
"""
import time
import sys
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[3] / "src"))

from sqlalchemy import create_engine, inspect, text, MetaData, Table, Column, String, Numeric, DateTime, Boolean

PG_URL = "postgresql://localhost/rule4_test"

engine = create_engine(PG_URL)

# Build a sample table with 30 columns (typical Socrata size)
meta = MetaData(schema="bench_test")
cols = [Column(f"col_{i}", [String, Numeric, DateTime, Boolean][i % 4]) for i in range(30)]
Table("sample_table", meta, *cols)

N = 20  # number of iterations

with engine.connect() as conn:
    conn.execute(text('DROP SCHEMA IF EXISTS bench_test CASCADE'))
    conn.execute(text('CREATE SCHEMA bench_test'))
    conn.commit()

timings = {"create": [], "inspect": [], "drop": []}

for i in range(N):
    tbl_name = f"tbl_{i}"
    schema_meta = MetaData(schema="bench_test")
    tbl_cols = [Column(f"col_{j}", [String, Numeric, DateTime, Boolean][j % 4]) for j in range(30)]
    Table(tbl_name, schema_meta, *tbl_cols)

    # CREATE
    t0 = time.time()
    schema_meta.create_all(engine, tables=[schema_meta.tables[f"bench_test.{tbl_name}"]])
    t1 = time.time()
    timings["create"].append(t1 - t0)

    # INSPECT
    t2 = time.time()
    insp = inspect(engine)
    cols_back = insp.get_columns(tbl_name, schema="bench_test")
    t3 = time.time()
    timings["inspect"].append(t3 - t2)

    # DROP
    t4 = time.time()
    with engine.connect() as conn:
        conn.execute(text(f'DROP TABLE bench_test."{tbl_name}"'))
        conn.commit()
    t5 = time.time()
    timings["drop"].append(t5 - t4)

    total = (t1-t0) + (t3-t2) + (t5-t4)
    print(f"  [{i+1:2d}/{N}] create={t1-t0:.3f}s  inspect={t3-t2:.3f}s  drop={t5-t4:.3f}s  total={total:.3f}s")

# Cleanup
with engine.connect() as conn:
    conn.execute(text('DROP SCHEMA IF EXISTS bench_test CASCADE'))
    conn.commit()

engine.dispose()

print(f"\nAverages over {N} iterations:")
for step, times in timings.items():
    avg = sum(times) / len(times)
    print(f"  {step:10s}: {avg*1000:.1f}ms avg")
print(f"  {'total':10s}: {sum(sum(t) for t in timings.values()) / N * 1000:.1f}ms avg per table")
