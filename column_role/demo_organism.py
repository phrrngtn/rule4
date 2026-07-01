"""The replica organism end-to-end: a read-only SQL Server source (gfe) rendered as a local
DuckLake TTST, kept fresh by polling. Control/status lives IN the replica; the source is never
written. Incremental sync + live schema evolution."""
import datetime as dt, os, shutil
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from loguru import logger
import ducklake_oob_writer as dl
import catalog_meta as cm
import organism as org
from column_collection import UserColumnDriver

MSSQL=("DRIVER={ODBC Driver 18 for SQL Server};SERVER=gfe.phrrngtn.arpa;DATABASE=rule4_test;"
       "Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate=yes")
base="/tmp/organism"; shutil.rmtree(base,ignore_errors=True); os.makedirs(base)

reg=create_engine(f"sqlite:///{base}/catalog.sqlite")
with reg.begin() as rc: cm.create_registry(rc); cm.load(rc)
cat=f"sqlite:{base}/lake.sqlite"
eng=create_engine(f"sqlite:///{base}/lake.sqlite"); dl.create_catalog(eng)
w=dl.DuckLakeWriter(eng, dl.DUCKLAKE_METADATA); w.init_catalog(data_path=f"{base}/data")
control=org.ReplicaControl(w, cat, f"{base}/data")            # control IN the replica

src=create_engine(URL.create("mssql+pyodbc", query={"odbc_connect": MSSQL}))
T1,T2,T3=dt.datetime(2026,6,1),dt.datetime(2026,6,15),dt.datetime(2026,6,20)
with src.begin() as c:
    c.execute(text("IF OBJECT_ID('dbo.wid') IS NOT NULL DROP TABLE dbo.wid"))
    c.execute(text("CREATE TABLE dbo.wid (id INT CONSTRAINT pk_wid PRIMARY KEY, name NVARCHAR(50), updated_at DATETIME2)"))
    for i,n in [(1,'alpha'),(2,'bravo'),(3,'charlie')]:
        c.execute(text("INSERT dbo.wid (id,name,updated_at) VALUES (:i,:n,:t)"),{"i":i,"n":n,"t":T1})
with src.begin() as c:   # give the source object a classification + stats, so the facets have something to capture
    c.execute(text("EXEC sp_addextendedproperty @name=N'survey.classification', @value=N'dimension', @level0type=N'SCHEMA',@level0name=N'dbo',@level1type=N'TABLE',@level1name=N'wid',@level2type=N'COLUMN',@level2name=N'name'"))
    c.execute(text("CREATE STATISTICS st_wid_id ON dbo.wid(id)"))

rep=org.Replica(control, reg, src, w, source="gfe", database="rule4_test", dialect="sqlserver",
                schema="dbo", table="wid", key="id", facets=["extended_property","stats_histogram"],
                driver=UserColumnDriver("updated_at", key="id"), initial_watermark="1900-01-01")
logger.info("sync #1 (initial load): {}", rep.sync(now=dt.datetime(2026,7,1,9)))
with src.begin() as c: c.execute(text("UPDATE dbo.wid SET name='BRAVO', updated_at=:t WHERE id=2"),{"t":T2})
logger.info("sync #2 (one row changed): {}", rep.sync(now=dt.datetime(2026,7,1,10)))
with src.begin() as c:
    c.execute(text("ALTER TABLE dbo.wid ADD region NVARCHAR(20)"))
    c.execute(text("UPDATE dbo.wid SET region='EMEA', updated_at=:t WHERE id=1"),{"t":T3})
logger.info("sync #3 (schema evolved + row changed): {}", rep.sync(now=dt.datetime(2026,7,1,11)))

with dl.lake_reader(cat, f"{base}/data") as lc:
    logger.info("replica TTST — full append-only history (every version kept; id=1 pre/post region):")
    for r in lc.execute(text("SELECT id, name, region FROM lake.wid ORDER BY id, region NULLS FIRST")):
        logger.info("   {}", tuple(r))
    snaps=[tuple(r) for r in lc.execute(text("SELECT snapshot_id, snapshot_time FROM ducklake_snapshots('lake') ORDER BY snapshot_id"))]
    logger.info("snapshots (snapshot_id monotonic; snapshot_time interleaves ingest-July / data-June — the LT/TT anomaly):")
    for sid, st in snaps: logger.info("   v{} @ {}", sid, st)
    logger.info("time-travel AT (VERSION => 3) — the initial load reconstructed on the LOGICAL clock:")
    for r in lc.execute(text("SELECT id, name FROM lake.wid AT (VERSION => 3) ORDER BY id")):
        logger.info("   {}", tuple(r))
    logger.info("control (IN the replica) — the bitemporal watermark trail:")
    for r in lc.execute(text("SELECT table_name, watermark, last_sync FROM lake._sync_state ORDER BY last_sync")): logger.info("   {}", tuple(r))
    logger.info("facet wid__extended_property — classification captured INTO the replica (latest of 3 snapshots):")
    for r in lc.execute(text("SELECT object_type, column_name, property_name, property_value FROM lake.wid__extended_property WHERE captured_at=(SELECT MAX(captured_at) FROM lake.wid__extended_property)")): logger.info("   {}", tuple(r))
    logger.info("facet wid__stats_histogram — data distribution captured INTO the replica (latest snapshot):")
    for r in lc.execute(text("SELECT stats_name, step_number, range_high_key, equal_rows FROM lake.wid__stats_histogram WHERE captured_at=(SELECT MAX(captured_at) FROM lake.wid__stats_histogram) ORDER BY step_number")): logger.info("   {}", tuple(r))
with src.connect() as c:
    cols=[r[0] for r in c.execute(text("SELECT name FROM sys.columns WHERE object_id=OBJECT_ID('dbo.wid') ORDER BY column_id"))]
logger.info("source dbo.wid columns (read-only, NO control columns): {}", cols)
with src.begin() as c: c.execute(text("IF OBJECT_ID('dbo.wid') IS NOT NULL DROP TABLE dbo.wid"))
