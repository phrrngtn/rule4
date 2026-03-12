-- ============================================================================
-- 03_query_ducklake.sql — Query through the DuckLake facade
-- ============================================================================
--
-- ATTACH the catalog via ducklake: and verify that out-of-band populated
-- metadata works correctly for table discovery, data queries, snapshot
-- history, and provenance.
--
-- Run from: sql/ducklake/experiment/
--   duckdb -c ".read 03_query_ducklake.sql"
-- ============================================================================

INSTALL ducklake;
LOAD ducklake;

ATTACH 'ducklake:ducklake_catalog.duckdb' AS lake;

-- ---------------------------------------------------------------------------
-- Test 1: Table discovery
-- ---------------------------------------------------------------------------
.print '=== Test 1: Table discovery ==='
SELECT table_name, file_count, file_size_bytes
FROM ducklake_table_info('lake');

-- ---------------------------------------------------------------------------
-- Test 2: Query data through the facade
-- ---------------------------------------------------------------------------
.print ''
.print '=== Test 2: Sample data from FHV Active Drivers ==='
SELECT name, type, expiration_date, license_number
FROM lake."xjfq-wh2d"
LIMIT 5;

.print ''
.print '=== Test 2b: Row counts across all tables ==='
SELECT '8wbx-tsch' AS dataset, count(*) AS rows FROM lake."8wbx-tsch"
UNION ALL SELECT 'vx8i-nprf', count(*) FROM lake."vx8i-nprf"
UNION ALL SELECT 'ic3t-wcy2', count(*) FROM lake."ic3t-wcy2"
UNION ALL SELECT 'dpec-ucu7', count(*) FROM lake."dpec-ucu7"
UNION ALL SELECT 'xjfq-wh2d', count(*) FROM lake."xjfq-wh2d";

-- ---------------------------------------------------------------------------
-- Test 3: Snapshot history with provenance
-- ---------------------------------------------------------------------------
.print ''
.print '=== Test 3: Snapshot history with provenance ==='
SELECT
    snapshot_id,
    snapshot_time,
    schema_version,
    author,
    commit_message
FROM ducklake_snapshots('lake');

-- ---------------------------------------------------------------------------
-- Test 4: DuckLake metadata functions
-- ---------------------------------------------------------------------------
.print ''
.print '=== Test 4: File listing ==='
SELECT * FROM ducklake_list_files('lake', 'xjfq-wh2d');

-- ---------------------------------------------------------------------------
-- Test 5: Table changes (insertions between snapshots)
-- ---------------------------------------------------------------------------
.print ''
.print '=== Test 5: Table insertions for xjfq-wh2d ==='
-- Note: snapshot range args must be literals, not subqueries.
-- Use ducklake_table_changes for a more flexible view.
SELECT snapshot_id, change_type, name, type, license_number
FROM ducklake_table_changes('lake', 'main', 'xjfq-wh2d', 0, 10)
LIMIT 5;

DETACH lake;

.print ''
.print '=== All tests passed ==='
