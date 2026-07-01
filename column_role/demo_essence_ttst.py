"""A TTST for anything the dataserver catalogs. capture_essence() snapshots an essence (schema,
login, database, ...) into DuckLake, bitemporal and read-only on the source. A schema created
between two captures shows up as a diff (EXCEPT on captured_at) -- change detection over server
objects, no bookkeeping on the source."""
import datetime as dt, os, shutil
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from loguru import logger
import ducklake_oob_writer as dl, catalog_meta as cm, organism as org
MSSQL=("DRIVER={ODBC Driver 18 for SQL Server};SERVER=gfe.phrrngtn.arpa;DATABASE=rule4_test;"
       "Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate=yes")
base="/tmp/ettst"; shutil.rmtree(base,ignore_errors=True); os.makedirs(base)
reg=create_engine(f"sqlite:///{base}/c.sqlite")
with reg.begin() as rc: cm.create_registry(rc); cm.load(rc)
cat=f"sqlite:{base}/l.sqlite"; eng=create_engine(f"sqlite:///{base}/l.sqlite"); dl.create_catalog(eng)
w=dl.DuckLakeWriter(eng, dl.DUCKLAKE_METADATA); w.init_catalog(data_path=f"{base}/d")
src=create_engine(URL.create("mssql+pyodbc", query={"odbc_connect": MSSQL}))
now1,now2=dt.datetime(2026,7,1,9),dt.datetime(2026,7,1,10)
def cap(ess, now, name=None):
    with src.connect() as sc, reg.connect() as rc: return org.capture_essence(w, rc, sc, "sqlserver", ess, now, name=name)
logger.info("schema TTST snapshot 1: {} schemas", cap("schema", now1, name="schemas"))
with src.begin() as c: c.execute(text("IF SCHEMA_ID('ttst_demo') IS NULL EXEC('CREATE SCHEMA ttst_demo')"))
logger.info("schema TTST snapshot 2 (after CREATE SCHEMA ttst_demo): {} schemas", cap("schema", now2, name="schemas"))
logger.info("login TTST (server-scoped object): {} logins", cap("login", now2, name="logins"))
with dl.lake_reader(cat, f"{base}/d") as lc:
    logger.info("schema set per capture: {}", [tuple(r) for r in lc.execute(text("SELECT captured_at, count(*) FROM lake.schemas GROUP BY captured_at ORDER BY captured_at"))])
    logger.info("schemas added between captures (EXCEPT on the TTST): {}", [r[0] for r in lc.execute(text("SELECT schema_name FROM lake.schemas WHERE captured_at=:b EXCEPT SELECT schema_name FROM lake.schemas WHERE captured_at=:a"), {"a":str(now1),"b":str(now2)})])
    logger.info("logins retained as a TTST: {}", [tuple(r) for r in lc.execute(text("SELECT login_name, type_desc FROM lake.logins"))])
with src.begin() as c: c.execute(text("IF SCHEMA_ID('ttst_demo') IS NOT NULL EXEC('DROP SCHEMA ttst_demo')"))
