-- load_schema.sql
-- Universal schema registry: rule4_table + rule4_column in DuckLake.
-- Sources:
--   1. Socrata (data.cityofnewyork.us) — from Discovery API JSON
--   2. DuckLake internal catalog — ducklake_table, ducklake_column, etc.
--   3. Rule4 self-description — rule4_table and rule4_column describe themselves
--
-- Each run = full refresh via DELETE + INSERT, creating DuckLake snapshots.
-- PIT queries show when any table or column from any source appeared.
--
-- Prerequisites:
--   1. Run fetch_catalog.sh to get raw/<domain>_catalog.json
--   2. DuckDB with ducklake extension
--
-- Usage:
--   cd sql/ducklake/schema_registry
--   duckdb < load_schema.sql

INSTALL ducklake;
LOAD ducklake;

ATTACH IF NOT EXISTS 'ducklake:schema_catalog.duckdb'
  AS lake (DATA_PATH 'data/');

-- ═══════════════════════════════════════════════════════════════════════
-- DDL — source-agnostic catalog (first run only)
-- ═══════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS lake.rule4_table (
  catalog_name    VARCHAR,    -- e.g. 'data.cityofnewyork.us', 'schema_catalog', 'rule4'
  schema_name     VARCHAR,    -- e.g. 'main', 'ducklake_internal'
  table_name      VARCHAR,    -- the table name
  source_type     VARCHAR,    -- 'socrata', 'ducklake', 'rule4'
  table_type      VARCHAR,    -- 'TABLE', 'VIEW', etc.
  description     VARCHAR
);

CREATE TABLE IF NOT EXISTS lake.rule4_column (
  catalog_name      VARCHAR,
  schema_name       VARCHAR,
  table_name        VARCHAR,
  column_name       VARCHAR,
  ordinal_position  INTEGER,
  data_type         VARCHAR,
  description       VARCHAR
);

-- ═══════════════════════════════════════════════════════════════════════
-- SOURCE 1: Socrata (data.cityofnewyork.us)
-- ═══════════════════════════════════════════════════════════════════════

CREATE OR REPLACE TEMP TABLE raw_catalog AS
SELECT *
FROM read_json(
  'raw/data.cityofnewyork.us_catalog.json',
  format = 'array',
  auto_detect = true,
  maximum_object_size = 10000000
);

CREATE OR REPLACE TEMP TABLE STG_SOCRATA_TABLE AS
SELECT
  'data.cityofnewyork.us'               AS catalog_name,
  'main'                                 AS schema_name,
  resource.id                            AS table_name,
  'socrata'                              AS source_type,
  resource.type                          AS table_type,
  resource.name || COALESCE(': ' || LEFT(resource.description, 200), '')
                                         AS description,
FROM raw_catalog;

CREATE OR REPLACE TEMP TABLE STG_SOCRATA_COLUMN AS
WITH EXPLODED AS (
  SELECT
    'data.cityofnewyork.us'              AS catalog_name,
    'main'                               AS schema_name,
    resource.id                          AS table_name,
    UNNEST(resource.columns_field_name)  AS column_name,
    UNNEST(resource.columns_datatype)    AS data_type,
    UNNEST(resource.columns_description) AS description,
  FROM raw_catalog
  WHERE resource.columns_field_name IS NOT NULL
    AND len(resource.columns_field_name) > 0
)
SELECT
  *,
  CAST(ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY (SELECT NULL)) AS INTEGER) AS ordinal_position
FROM EXPLODED;

-- ═══════════════════════════════════════════════════════════════════════
-- SOURCE 2: DuckLake internal catalog
-- The DuckLake metadata DB is already attached as __ducklake_metadata_lake.
-- Query its tables/columns via duckdb_tables()/duckdb_columns().
-- ═══════════════════════════════════════════════════════════════════════

CREATE OR REPLACE TEMP TABLE STG_DUCKLAKE_TABLE AS
SELECT
  'schema_catalog'   AS catalog_name,
  'ducklake'         AS schema_name,
  table_name,
  'ducklake'         AS source_type,
  'TABLE'            AS table_type,
  'DuckLake internal metadata table' AS description
FROM duckdb_tables()
WHERE database_name = '__ducklake_metadata_lake';

CREATE OR REPLACE TEMP TABLE STG_DUCKLAKE_COLUMN AS
SELECT
  'schema_catalog'   AS catalog_name,
  'ducklake'         AS schema_name,
  table_name,
  column_name,
  CAST(column_index + 1 AS INTEGER) AS ordinal_position,
  data_type,
  CAST(NULL AS VARCHAR) AS description
FROM duckdb_columns()
WHERE database_name = '__ducklake_metadata_lake';

-- ═══════════════════════════════════════════════════════════════════════
-- SOURCE 3: Rule4 self-description
-- rule4_table and rule4_column describe themselves.
-- ═══════════════════════════════════════════════════════════════════════

CREATE OR REPLACE TEMP TABLE STG_RULE4_TABLE (
  catalog_name VARCHAR, schema_name VARCHAR, table_name VARCHAR,
  source_type VARCHAR, table_type VARCHAR, description VARCHAR
);
INSERT INTO STG_RULE4_TABLE VALUES
  ('rule4', 'main', 'rule4_table',  'rule4', 'TABLE', 'Universal table catalog — every table from every source'),
  ('rule4', 'main', 'rule4_column', 'rule4', 'TABLE', 'Universal column catalog — every column from every source');

CREATE OR REPLACE TEMP TABLE STG_RULE4_COLUMN (
  catalog_name VARCHAR, schema_name VARCHAR, table_name VARCHAR,
  column_name VARCHAR, ordinal_position INTEGER, data_type VARCHAR, description VARCHAR
);
INSERT INTO STG_RULE4_COLUMN VALUES
  -- rule4_table columns
  ('rule4', 'main', 'rule4_table', 'catalog_name',    1, 'VARCHAR', 'Source catalog identifier'),
  ('rule4', 'main', 'rule4_table', 'schema_name',     2, 'VARCHAR', 'Schema within catalog'),
  ('rule4', 'main', 'rule4_table', 'table_name',      3, 'VARCHAR', 'Table name within schema'),
  ('rule4', 'main', 'rule4_table', 'source_type',     4, 'VARCHAR', 'Source system type: socrata, ducklake, rule4, ...'),
  ('rule4', 'main', 'rule4_table', 'table_type',      5, 'VARCHAR', 'TABLE, VIEW, etc.'),
  ('rule4', 'main', 'rule4_table', 'description',     6, 'VARCHAR', 'Human-readable description'),
  -- rule4_column columns
  ('rule4', 'main', 'rule4_column', 'catalog_name',      1, 'VARCHAR', 'Source catalog identifier'),
  ('rule4', 'main', 'rule4_column', 'schema_name',       2, 'VARCHAR', 'Schema within catalog'),
  ('rule4', 'main', 'rule4_column', 'table_name',        3, 'VARCHAR', 'Table this column belongs to'),
  ('rule4', 'main', 'rule4_column', 'column_name',       4, 'VARCHAR', 'Column name'),
  ('rule4', 'main', 'rule4_column', 'ordinal_position',  5, 'INTEGER', '1-based column position'),
  ('rule4', 'main', 'rule4_column', 'data_type',         6, 'VARCHAR', 'Data type (source-native type name)'),
  ('rule4', 'main', 'rule4_column', 'description',       7, 'VARCHAR', 'Human-readable description');

-- ═══════════════════════════════════════════════════════════════════════
-- LOAD: full refresh into DuckLake
-- ═══════════════════════════════════════════════════════════════════════

DELETE FROM lake.rule4_table;
INSERT INTO lake.rule4_table
  SELECT * FROM STG_SOCRATA_TABLE
  UNION ALL
  SELECT * FROM STG_DUCKLAKE_TABLE
  UNION ALL
  SELECT * FROM STG_RULE4_TABLE;

DELETE FROM lake.rule4_column;
INSERT INTO lake.rule4_column
  (catalog_name, schema_name, table_name, column_name, ordinal_position, data_type, description)
  SELECT catalog_name, schema_name, table_name, column_name, ordinal_position, data_type, description
  FROM STG_SOCRATA_COLUMN
  UNION ALL
  SELECT catalog_name, schema_name, table_name, column_name, ordinal_position, data_type, description
  FROM STG_DUCKLAKE_COLUMN
  UNION ALL
  SELECT catalog_name, schema_name, table_name, column_name, ordinal_position, data_type, description
  FROM STG_RULE4_COLUMN;

-- ═══════════════════════════════════════════════════════════════════════
-- VERIFY
-- ═══════════════════════════════════════════════════════════════════════

.print '=== Tables by source ==='
SELECT source_type, count(*) AS tables
FROM lake.rule4_table
GROUP BY ALL
ORDER BY tables DESC;

.print '=== Columns by source ==='
SELECT
  t.source_type,
  count(*) AS columns
FROM lake.rule4_column AS c
JOIN lake.rule4_table AS t
  USING (catalog_name, schema_name, table_name)
GROUP BY ALL
ORDER BY columns DESC;

.print '=== Self-description: rule4_table describes itself ==='
SELECT column_name, ordinal_position, data_type, description
FROM lake.rule4_column
WHERE catalog_name = 'rule4'
  AND table_name = 'rule4_table'
ORDER BY ordinal_position;

.print '=== Self-description: rule4_column describes itself ==='
SELECT column_name, ordinal_position, data_type, description
FROM lake.rule4_column
WHERE catalog_name = 'rule4'
  AND table_name = 'rule4_column'
ORDER BY ordinal_position;

.print '=== DuckLake internal tables ==='
SELECT table_name, count(c.column_name) AS num_columns
FROM lake.rule4_table AS t
LEFT JOIN lake.rule4_column AS c
  USING (catalog_name, schema_name, table_name)
WHERE t.source_type = 'ducklake'
GROUP BY ALL
ORDER BY table_name;

.print '=== Snapshot history ==='
SELECT snapshot_id, snapshot_time, changes
FROM ducklake_snapshots('lake')
ORDER BY snapshot_id;
