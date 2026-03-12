-- ============================================================================
-- 01_init_catalog.sql — Bootstrap the DuckLake catalog database
-- ============================================================================
--
-- Creates ducklake_catalog.duckdb by letting the ducklake extension initialize
-- its metadata tables, then detaches. After this, we populate out-of-band.
--
-- Run from: sql/ducklake/experiment/
--   duckdb -c ".read 01_init_catalog.sql"
-- ============================================================================

INSTALL ducklake;
LOAD ducklake;

-- Let DuckLake create its metadata schema. data/ is where Parquet files live.
ATTACH 'ducklake:ducklake_catalog.duckdb' AS lake (
    DATA_PATH 'data/'
);

-- Verify initialization
.print '--- DuckLake options ---'
SELECT * FROM ducklake_options('lake');

.print ''
.print '--- Initial snapshots ---'
SELECT * FROM ducklake_snapshots('lake');

DETACH lake;

.print ''
.print 'Catalog initialized at ducklake_catalog.duckdb'
.print 'Data directory: data/'
