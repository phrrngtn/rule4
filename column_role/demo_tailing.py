"""Metadata-driven tailing: build a type-aware tailing query + ODBC connection from the
catalog alone — no live source needed.

The schema-as-data (column_role types) plus a stored connection are enough to CONSTRUCT
the tailing query (funky values CAST on the way out) and the ODBC connection string. The
same SQL then runs via pyodbc — or, later, federates through DuckDB's ODBC extension.

Run from column_role/:  uv run python demo_tailing.py
"""
import datetime as dt
import os
import shutil

from sampling import SamplingPlan
from tailing import ct_tailing_query

BASE = "/tmp/colrole_tailing"
shutil.rmtree(BASE, ignore_errors=True)
os.makedirs(BASE)
T = dt.datetime(2026, 6, 29, 10)

plan = SamplingPlan(f"{BASE}/cat.sqlite", f"{BASE}/data")

# connection components live in the metadatabase (no password — integrated security)
plan.declare_connection([{
    "dataserver": "gfe", "database": "shop", "dialect": "sqlserver",
    "odbc_driver": "ODBC Driver 18 for SQL Server", "odbc_server": "gfe.phrrngtn.arpa",
    "odbc_database": "shop", "extra": "Encrypt=yes;TrustServerCertificate=yes"}], T)
print("connection string (from the catalog):\n ", plan.connection_string("gfe", "shop"))

# a SQL-Server-shaped schema, exactly as column_role would have captured it: (name, type)
schema = [("id", "int"), ("name", "nvarchar(100)"), ("payload", "varbinary"),
          ("rowguid", "uniqueidentifier"), ("amount", "money"), ("notes", "nvarchar(max)")]
print("\ntype-aware tailing query (built from column_role types):\n ",
      ct_tailing_query("dbo", "widget", schema, "sqlserver", key="id"))

plan.dispose()
