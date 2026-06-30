"""ColumnCollection — one schema-as-data object, five capabilities.

Assemble a table's columns from a column_role capture, then drive the whole round trip:
(1) a SA model, (2) the DuckLake DDL, (3) a type-aware tail query that renders per source
dialect, (4) a current-state replica populated from the tail results, (5) a DuckLake
payload snapshot from the same results.

Run from column_role/:  uv run python demo_column_collection.py
"""
import datetime as dt
import os
import shutil
import sqlite3

from sqlalchemy import create_engine
from sqlalchemy.dialects import mssql, postgresql

import ducklake_oob_writer as dl
from column_collection import Col, ColumnCollection
from registry import Registry, capture

BASE = "/tmp/cc_demo"
shutil.rmtree(BASE, ignore_errors=True)
os.makedirs(BASE)
SRV, DB, T = "localhost", "shop", dt.datetime(2026, 6, 29, 10)

# --- source + column_role capture -> assemble the ColumnCollection ---
src = sqlite3.connect(f"{BASE}/source.sqlite")
src.execute("CREATE TABLE widget (id INTEGER PRIMARY KEY, name TEXT, price REAL)")
src.executemany("INSERT INTO widget VALUES (?,?,?)", [(1, "sprocket", 1.5), (2, "gadget", 9.99)])
src.commit()
src_eng = create_engine(f"sqlite:///{BASE}/source.sqlite")

reg = Registry(f"{BASE}/reg_cat.sqlite", f"{BASE}/reg_data")
reg.record(capture(src.cursor(), "sqlite", SRV, DB, T), T)
cc = ColumnCollection.from_column_role(reg, SRV, DB, "widget", T, key="id")
print("ColumnCollection:", cc.name, [(c.name, c.source_type, c.ducklake_type) for c in cc.columns])

# (1) a SQLAlchemy model
print("\n(1) SA model    :", [(c.name, str(c.type)) for c in cc.sqlalchemy_table().columns])

# (2) record in DuckLake speak
eng = create_engine(f"sqlite:///{BASE}/lake_cat.sqlite")
dl.create_catalog(eng)
w = dl.DuckLakeWriter(eng, dl.DUCKLAKE_METADATA)
w.init_catalog(data_path=f"{BASE}/lake_data")
cc.record_in_ducklake(w)
print("(2) ducklake DDL:", [c["column_name"] for c in w.current_columns("widget")])

# (3) tail query base — one object, renders per source dialect
print("(3) tail (sqlite):", str(cc.tail_query_base(src_eng).compile(src_eng)).replace("\n", " "))
funky = ColumnCollection("dbo", "evt", [Col("id", "int"), Col("blob", "varbinary"),
                                        Col("geom", "geography")], key="id")
print("    tail (mssql) :", str(funky.tail_query_base(src_eng).compile(dialect=mssql.dialect())).replace("\n", " "))
print("    tail (pg)    :", str(funky.tail_query_base(src_eng).compile(dialect=postgresql.dialect())).replace("\n", " "))

# run the tail query against the source
with src_eng.connect() as conn:
    results = [dict(r._mapping) for r in conn.execute(cc.tail_query_base(src_eng))]
print("\ntail results    :", results)

# (4) populate a current-state replica
rep = dl.Replica(w, "widget", "id")
print("(4) replica     :", cc.populate_replica(rep, results, snapshot_time=T))

# (5) populate DuckLake as a payload snapshot (a second table)
ColumnCollection("main", "widget_log", cc.columns, key="id").record_in_ducklake(w)
log = ColumnCollection("main", "widget_log", cc.columns, key="id")
print("(5) ducklake    :", log.populate_ducklake(w, results, snapshot_time=T))

with dl.attach_lake(f"sqlite:{BASE}/lake_cat.sqlite", f"{BASE}/lake_data") as c:
    print("    replica rows:", c.execute("SELECT id, name, price FROM lake.widget ORDER BY id").fetchall())
    print("    log rows    :", c.execute("SELECT id, name, price FROM lake.widget_log ORDER BY id").fetchall())

reg.dispose()
eng.dispose()
src.close()
