-- ============================================================================
-- 02b_register_data_files.sql — Register Parquet files in the catalog
-- ============================================================================
--
-- After Parquet files are created (via generated_copy_data.sql), this script
-- registers them in ducklake_data_file with correct metadata (record_count,
-- file_size_bytes, footer_size).
--
-- DuckLake validates footer_size against the actual Parquet file, so we
-- extract it from the files before registration.
--
-- Run from: sql/ducklake/experiment/
--   duckdb -c ".read 02b_register_data_files.sql"
-- ============================================================================

ATTACH 'ducklake_catalog.duckdb' AS cat;

.print '=== Registering data files in catalog ==='

-- Read current counters
CREATE OR REPLACE TEMP TABLE COUNTERS AS
SELECT
    max(snapshot_id) AS last_snapshot,
    max(next_catalog_id) AS next_cat_id,
    max(next_file_id) AS next_file_id,
    max(schema_version) AS schema_ver
FROM cat.ducklake_snapshot;

-- Discover Parquet files and extract file stats.
-- We use a Python script to get footer_size (last 4 bytes before PAR1 magic)
-- and file_size_bytes, then load the results.
.shell python3 extract_parquet_stats.py

CREATE OR REPLACE TEMP TABLE PARQUET_STATS AS
SELECT * FROM read_csv('_parquet_stats.csv');

-- Join with catalog tables to get table_id
CREATE OR REPLACE TEMP TABLE PARQUET_FILES AS
SELECT
    t.table_id,
    t.table_name,
    'data_0.parquet' AS relative_path,
    ps.file_size_bytes,
    ps.footer_size,
    row_number() OVER () AS seq
FROM cat.ducklake_table AS t
JOIN PARQUET_STATS AS ps ON ps.table_name = t.table_name
WHERE t.end_snapshot IS NULL;

SELECT count(*) AS parquet_files_found FROM PARQUET_FILES;

-- One snapshot per data file insert (DuckLake convention)
INSERT INTO cat.ducklake_snapshot
SELECT
    (SELECT last_snapshot FROM COUNTERS) + pf.seq AS snapshot_id,
    now(),
    (SELECT schema_ver FROM COUNTERS),
    (SELECT next_cat_id FROM COUNTERS),
    (SELECT next_file_id FROM COUNTERS) + pf.seq
FROM PARQUET_FILES AS pf;

INSERT INTO cat.ducklake_snapshot_changes
SELECT
    (SELECT last_snapshot FROM COUNTERS) + pf.seq,
    'inserted_into_table:' || pf.table_id,
    'rule4_socrata_sync',
    'Load data sample for ' || pf.table_name,
    '{"source":"socrata","domain":"data.cityofnewyork.us","dataset_id":"' || pf.table_name || '"}'
FROM PARQUET_FILES AS pf;

-- Register Parquet files with correct stats
INSERT INTO cat.ducklake_data_file
SELECT
    (SELECT next_file_id FROM COUNTERS) + pf.seq - 1 AS data_file_id,
    pf.table_id,
    (SELECT last_snapshot FROM COUNTERS) + pf.seq AS begin_snapshot,
    NULL AS end_snapshot,
    NULL AS file_order,
    pf.relative_path AS path,
    true AS path_is_relative,
    'parquet' AS file_format,
    500 AS record_count,
    pf.file_size_bytes,
    pf.footer_size,
    0 AS row_id_start,
    NULL AS partition_id,
    NULL AS encryption_key,
    NULL AS partial_file_info,
    NULL AS mapping_id
FROM PARQUET_FILES AS pf;

.shell rm -f _parquet_stats.csv

.print ''
.print '--- Registered data files ---'
SELECT
    df.data_file_id,
    t.table_name,
    df.path,
    df.record_count,
    df.file_size_bytes,
    df.footer_size
FROM cat.ducklake_data_file AS df
JOIN cat.ducklake_table AS t ON t.table_id = df.table_id;

.print ''
.print '--- Final snapshot state ---'
SELECT snapshot_id, snapshot_time, schema_version FROM cat.ducklake_snapshot;

DETACH cat;

.print ''
.print '=== Data files registered. Ready for ducklake queries. ==='
.print 'Run: duckdb -c ".read 03_query_ducklake.sql"'
