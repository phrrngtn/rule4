-- query_schema.sql
-- Example queries against the Socrata schema registry in DuckLake.
--
-- Usage:
--   cd sql/ducklake/schema_registry
--   duckdb < query_schema.sql

INSTALL ducklake;
LOAD ducklake;

ATTACH 'ducklake:schema_catalog.duckdb' AS lake (DATA_PATH 'data/');

-- ─── Current state ────────────────────────────────────────────────────

.print '=== Current counts ==='
SELECT 'datasets' AS entity, count(*) AS n FROM lake.socrata_dataset
UNION ALL
SELECT 'columns', count(*) FROM lake.socrata_dataset_column;

-- Top 10 datasets by column count
.print '=== Top 10 datasets by column count ==='
SELECT
  d.dataset_id,
  d.dataset_name,
  count(c.field_name) AS num_columns
FROM lake.socrata_dataset AS d
LEFT JOIN lake.socrata_dataset_column AS c USING (dataset_id)
GROUP BY ALL
ORDER BY num_columns DESC
LIMIT 10;

-- ─── Snapshot history ─────────────────────────────────────────────────

.print '=== Snapshot history ==='
SELECT snapshot_id, snapshot_time, changes
FROM ducklake_snapshots('lake')
ORDER BY snapshot_id;

-- ─── PIT: columns for a specific dataset at a snapshot ────────────────

.print '=== DOB Job Filings columns (first load, snap 0-4) ==='
SELECT field_name, datatype, ordinal_position
FROM ducklake_table_insertions('lake', 'main', 'socrata_dataset_column', 0::BIGINT, 4::BIGINT)
WHERE dataset_id = 'ic3t-wcy2'
ORDER BY ordinal_position
LIMIT 15;

-- ─── Diff between loads ───────────────────────────────────────────────
-- After running load_schema.sql twice, compare changes:
--
-- New datasets added in second load:
--   SELECT dataset_id, dataset_name
--   FROM ducklake_table_insertions('lake', 'main', 'socrata_dataset', 5::BIGINT, 8::BIGINT) AS ins
--   WHERE NOT EXISTS (
--     SELECT 1
--     FROM ducklake_table_deletions('lake', 'main', 'socrata_dataset', 5::BIGINT, 8::BIGINT) AS del
--     WHERE del.dataset_id = ins.dataset_id
--   );
--
-- Datasets removed in second load:
--   SELECT dataset_id, dataset_name
--   FROM ducklake_table_deletions('lake', 'main', 'socrata_dataset', 5::BIGINT, 8::BIGINT) AS del
--   WHERE NOT EXISTS (
--     SELECT 1
--     FROM ducklake_table_insertions('lake', 'main', 'socrata_dataset', 5::BIGINT, 8::BIGINT) AS ins
--     WHERE ins.dataset_id = del.dataset_id
--   );

-- ─── Datatype distribution ────────────────────────────────────────────

.print '=== Column datatype distribution ==='
SELECT datatype, count(*) AS n
FROM lake.socrata_dataset_column
GROUP BY ALL
ORDER BY n DESC;
