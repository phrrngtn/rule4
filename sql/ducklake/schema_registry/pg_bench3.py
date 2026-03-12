"""
Profile PG round-trip: time create vs inspect vs raw SQL readback.
"""
import time
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))

from sqlalchemy import create_engine, inspect, text, MetaData, Table, Column, String, Numeric, DateTime, Boolean

PG_URL = "postgresql://localhost/rule4_test"
engine = create_engine(PG_URL, pool_size=1)

SCHEMA = "bench3"
N = 100

with engine.connect() as conn:
    conn.execute(text(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE'))
    conn.execute(text(f'CREATE SCHEMA "{SCHEMA}"'))
    conn.commit()

t_create = t_inspect_sa = t_inspect_raw = 0

for i in range(N):
    tbl_name = f"tbl_{i}"
    meta = MetaData(schema=SCHEMA)
    cols = [Column(f"col_{j}", [String, Numeric, DateTime, Boolean][j % 4]) for j in range(30)]
    Table(tbl_name, meta, *cols)

    t0 = time.time()
    meta.tables[f"{SCHEMA}.{tbl_name}"].create(engine)
    t1 = time.time()
    t_create += t1 - t0

    # SA inspect
    insp = inspect(engine)
    cols_back = insp.get_columns(tbl_name, schema=SCHEMA)
    t2 = time.time()
    t_inspect_sa += t2 - t1

    # Raw SQL readback
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = '{SCHEMA}' AND table_name = '{tbl_name}'
            ORDER BY ordinal_position
        """)).fetchall()
    t3 = time.time()
    t_inspect_raw += t3 - t2

with engine.connect() as conn:
    conn.execute(text(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE'))
    conn.commit()

engine.dispose()

print(f"Over {N} tables (30 cols each):")
print(f"  create:      {t_create*1000/N:.1f}ms avg")
print(f"  inspect(SA): {t_inspect_sa*1000/N:.1f}ms avg")
print(f"  inspect(SQL):{t_inspect_raw*1000/N:.1f}ms avg")
print(f"  total(SA):   {(t_create+t_inspect_sa)*1000/N:.1f}ms avg")
print(f"  total(SQL):  {(t_create+t_inspect_raw)*1000/N:.1f}ms avg")
