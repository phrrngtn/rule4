-- ============================================================================
-- 02_populate_catalog.sql — Out-of-band metadata population
-- ============================================================================
--
-- Opens the catalog database as a PLAIN DuckDB database (not via ducklake:)
-- and directly inserts metadata rows: tables, columns, snapshots, data files.
--
-- Also generates a script to convert CSV data samples to Parquet with the
-- correct FIELD_IDS so DuckLake can map columns correctly.
--
-- This is the core experiment: can we populate DuckLake's metadata tables
-- ourselves and have the ducklake extension read them correctly?
--
-- Run from: sql/ducklake/experiment/
--   duckdb -c ".read 02_populate_catalog.sql"
--
-- Requires: 00_fetch_socrata.sh and 01_init_catalog.sql already run
-- ============================================================================

.print '=== Loading catalog metadata from Socrata ==='

-- Open the catalog DB directly (NOT as ducklake:)
ATTACH 'ducklake_catalog.duckdb' AS cat;

-- ---------------------------------------------------------------------------
-- Read the Socrata catalog JSON to discover datasets
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE SOCRATA_CATALOG AS
WITH RAW AS (
    SELECT unnest(results) AS r
    FROM read_json_auto('raw/data.cityofnewyork.us/catalog.json')
)
SELECT
    r.resource.id AS dataset_id,
    r.resource.name AS dataset_name,
    r.resource.description AS dataset_description,
    r.resource.data_updated_at AS data_updated_at,
    r.resource.createdAt AS created_at,
    r.resource.columns_name AS columns_name,
    r.resource.columns_field_name AS columns_field_name,
    r.resource.columns_datatype AS columns_datatype
FROM RAW;

SELECT dataset_id, dataset_name, len(columns_field_name) AS num_cols
FROM SOCRATA_CATALOG;

-- ---------------------------------------------------------------------------
-- Read current catalog state to find our starting counters
-- ---------------------------------------------------------------------------
.print ''
.print '--- Current catalog state ---'
SELECT
    max(snapshot_id) AS last_snapshot_id,
    max(next_catalog_id) AS next_catalog_id,
    max(next_file_id) AS next_file_id,
    max(schema_version) AS schema_version
FROM cat.ducklake_snapshot;

-- ---------------------------------------------------------------------------
-- All columns are written as VARCHAR in our Parquet files (all_varchar=true).
-- This is safest: Socrata type metadata is informational but we let consumers
-- cast as needed rather than risk silent coercion failures.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TEMP TABLE DATASETS_TO_PROCESS AS
SELECT
    row_number() OVER () AS seq,
    sc.*
FROM SOCRATA_CATALOG AS sc;

-- Collect all column registrations by unnesting the parallel arrays
CREATE OR REPLACE TEMP TABLE NEW_COLUMNS AS
WITH UNNESTED AS (
    SELECT
        d.seq AS table_seq,
        d.dataset_id,
        unnest(d.columns_field_name) AS field_name,
        unnest(d.columns_name) AS col_name,
        unnest(d.columns_datatype) AS socrata_type,
        generate_subscripts(d.columns_field_name, 1) AS col_order
    FROM DATASETS_TO_PROCESS AS d
)
SELECT
    u.table_seq,
    u.dataset_id,
    u.field_name,
    u.col_name,
    u.socrata_type,
    'varchar' AS duckdb_type,
    u.col_order
FROM UNNESTED AS u;

.print ''
.print '--- Column type distribution (Socrata types, all mapped to varchar) ---'
SELECT socrata_type, count(*) AS cnt
FROM NEW_COLUMNS
GROUP BY ALL;

-- ---------------------------------------------------------------------------
-- Populate catalog tables
-- ---------------------------------------------------------------------------
--
-- DuckLake expects one snapshot per CREATE TABLE, each with a
-- changes_made string like: created_table:"main"."dataset_id"
-- and a corresponding schema_version bump.
--
-- Key invariant discovered experimentally:
--   DuckLake uses Parquet field_id to map columns, NOT column names.
--   field_id must equal column_id from ducklake_column.
--
-- ID assignment:
--   table_id:    next_cat_id + 0..N-1
--   column_id:   next_cat_id + N + 0..M-1
--   snapshot_id: last_snapshot + 1..N  (one per table)

.print ''
.print '--- Populating catalog tables ---'

CREATE OR REPLACE TEMP TABLE COUNTERS AS
SELECT
    coalesce(max(snapshot_id), -1) AS last_snapshot,
    coalesce(max(next_catalog_id), 1) AS next_cat_id,
    coalesce(max(next_file_id), 0) AS next_file_id,
    coalesce(max(schema_version), 0) AS schema_ver
FROM cat.ducklake_snapshot;

-- Assign table IDs — each table gets its own snapshot
CREATE OR REPLACE TEMP TABLE TABLE_IDS AS
SELECT
    d.seq AS table_id_offset,
    d.dataset_id,
    d.dataset_name,
    d.data_updated_at,
    (SELECT next_cat_id FROM COUNTERS) + row_number() OVER () - 1 AS table_id,
    (SELECT last_snapshot FROM COUNTERS) + row_number() OVER () AS snapshot_id,
    (SELECT schema_ver FROM COUNTERS) + row_number() OVER () AS schema_version
FROM DATASETS_TO_PROCESS AS d;

-- Assign column IDs (globally unique, after all table IDs)
CREATE OR REPLACE TEMP TABLE COLUMN_IDS AS
SELECT
    nc.*,
    ti.table_id,
    ti.snapshot_id,
    (SELECT next_cat_id FROM COUNTERS)
        + (SELECT count(*) FROM TABLE_IDS)
        + row_number() OVER () - 1 AS column_id
FROM NEW_COLUMNS AS nc
JOIN TABLE_IDS AS ti ON ti.table_id_offset = nc.table_seq;

-- The next_catalog_id after all IDs are allocated
CREATE OR REPLACE TEMP TABLE FINAL_COUNTERS AS
SELECT
    max(ti.snapshot_id) AS last_snapshot,
    (SELECT max(column_id) + 1 FROM COLUMN_IDS) AS next_cat_id,
    (SELECT next_file_id FROM COUNTERS) AS next_file_id,
    max(ti.schema_version) AS schema_ver
FROM TABLE_IDS AS ti;

-- Insert one snapshot per table (source-authoritative timestamp)
INSERT INTO cat.ducklake_snapshot
SELECT
    ti.snapshot_id,
    ti.data_updated_at::TIMESTAMPTZ,
    ti.schema_version,
    (SELECT next_cat_id FROM FINAL_COUNTERS),
    (SELECT next_file_id FROM FINAL_COUNTERS)
FROM TABLE_IDS AS ti;

-- Schema version entries
INSERT INTO cat.ducklake_schema_versions
SELECT ti.snapshot_id, ti.schema_version
FROM TABLE_IDS AS ti;

-- Insert tables
INSERT INTO cat.ducklake_table
SELECT
    ti.table_id,
    uuid(),
    ti.snapshot_id,
    NULL,
    0,
    ti.dataset_id,
    ti.dataset_id || '/',
    true
FROM TABLE_IDS AS ti;

-- Insert columns
INSERT INTO cat.ducklake_column
SELECT
    ci.column_id,
    ci.snapshot_id,
    NULL,
    ci.table_id,
    ci.col_order,
    ci.field_name,
    ci.duckdb_type,
    NULL,
    NULL,
    true,
    NULL
FROM COLUMN_IDS AS ci;

-- Snapshot changes (proper DuckLake format)
INSERT INTO cat.ducklake_snapshot_changes
SELECT
    ti.snapshot_id,
    'created_table:"main"."' || ti.dataset_id || '"',
    'rule4_socrata_sync',
    'Socrata catalog import: ' || ti.dataset_name,
    '{"source":"socrata","domain":"data.cityofnewyork.us","dataset_id":"' || ti.dataset_id || '"}'
FROM TABLE_IDS AS ti;

SELECT count(*) AS tables_inserted FROM TABLE_IDS;
SELECT count(*) AS columns_inserted FROM COLUMN_IDS;

-- ---------------------------------------------------------------------------
-- Generate COPY statements with FIELD_IDS
-- ---------------------------------------------------------------------------
-- DuckLake maps Parquet columns via field_id, not column name.
-- field_id must match column_id from ducklake_column.

.print ''
.print '--- Generating Parquet COPY script with FIELD_IDS ---'

CREATE TEMP TABLE FIELD_ID_MAPS AS
WITH COL_ENTRIES AS (
    SELECT
        t.table_name,
        c.column_name,
        c.column_id,
        c.column_order
    FROM cat.ducklake_table AS t
    JOIN cat.ducklake_column AS c ON c.table_id = t.table_id
    WHERE t.end_snapshot IS NULL AND c.end_snapshot IS NULL
)
SELECT
    table_name,
    string_agg('"' || column_name || '"', ', ' ORDER BY column_order) AS col_list,
    '{' || string_agg(column_name || ': ' || column_id, ', ' ORDER BY column_order) || '}' AS field_ids_struct
FROM COL_ENTRIES
GROUP BY table_name;

COPY (
    SELECT
        'COPY (SELECT ' || col_list
        || ' FROM read_csv(''raw/data.cityofnewyork.us/data/' || table_name
        || '.csv'', all_varchar=true, header=true, auto_detect=true, ignore_errors=true)) '
        || 'TO ''data/main/' || table_name || '/data_0.parquet'' (FORMAT PARQUET, FIELD_IDS '
        || field_ids_struct || ');'
    FROM FIELD_ID_MAPS
) TO 'generated_copy_data.sql' (HEADER false, QUOTE '', DELIMITER E'\n');

-- Also generate mkdir commands
COPY (
    SELECT 'mkdir -p data/main/' || table_name
    FROM FIELD_ID_MAPS
) TO 'generated_mkdir.sh' (HEADER false, QUOTE '', DELIMITER E'\n');

DETACH cat;

.print ''
.print '=== Catalog metadata populated ==='
.print 'Next steps:'
.print '  1. bash generated_mkdir.sh'
.print '  2. duckdb -c ".read generated_copy_data.sql"'
.print '  3. duckdb -c ".read 02b_register_data_files.sql"'
.print '  4. duckdb -c ".read 03_query_ducklake.sql"'
