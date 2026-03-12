-- load_all_socrata.sql
-- Load ALL Socrata dataset metadata (10,000 datasets, 137 domains) into DuckLake.
-- No row data — only schema metadata (table names, column names, types, descriptions).
--
-- Usage:
--   cd sql/ducklake/schema_registry
--   duckdb < load_all_socrata.sql

INSTALL ducklake;
LOAD ducklake;

ATTACH IF NOT EXISTS 'ducklake:schema_catalog.duckdb'
  AS lake (DATA_PATH 'data/');

-- ═══════════════════════════════════════════════════════════════════════
-- Parse ALL Socrata catalog JSON
-- ═══════════════════════════════════════════════════════════════════════

CREATE OR REPLACE TEMP TABLE RAW_ALL_CATALOG AS
SELECT *
FROM read_json(
  'raw/all_socrata_catalog.json',
  format = 'array',
  auto_detect = true,
  maximum_object_size = 10000000
);

-- Extract domain from metadata
CREATE OR REPLACE TEMP TABLE STG_ALL_SOCRATA_TABLE AS
SELECT
  COALESCE(metadata.domain, 'unknown')  AS catalog_name,
  'main'                                 AS schema_name,
  resource.id                            AS table_name,
  'socrata'                              AS source_type,
  resource.type                          AS table_type,
  resource.name || COALESCE(': ' || LEFT(resource.description, 200), '')
                                         AS description,
FROM RAW_ALL_CATALOG;

CREATE OR REPLACE TEMP TABLE STG_ALL_SOCRATA_COLUMN AS
WITH EXPLODED AS (
  SELECT
    COALESCE(metadata.domain, 'unknown')  AS catalog_name,
    'main'                                 AS schema_name,
    resource.id                            AS table_name,
    UNNEST(resource.columns_field_name)    AS column_name,
    UNNEST(resource.columns_datatype)      AS data_type,
    UNNEST(resource.columns_description)   AS description,
  FROM RAW_ALL_CATALOG
  WHERE resource.columns_field_name IS NOT NULL
    AND len(resource.columns_field_name) > 0
)
SELECT
  *,
  CAST(ROW_NUMBER() OVER (PARTITION BY catalog_name, table_name ORDER BY (SELECT NULL)) AS INTEGER) AS ordinal_position
FROM EXPLODED;

-- ═══════════════════════════════════════════════════════════════════════
-- Summary before loading
-- ═══════════════════════════════════════════════════════════════════════

.print '=== Staged table counts ==='
SELECT count(*) AS tables FROM STG_ALL_SOCRATA_TABLE;
SELECT count(*) AS columns FROM STG_ALL_SOCRATA_COLUMN;
SELECT count(DISTINCT catalog_name) AS domains FROM STG_ALL_SOCRATA_TABLE;

.print '=== Top 20 domains by table count ==='
SELECT catalog_name, count(*) AS tables
FROM STG_ALL_SOCRATA_TABLE
GROUP BY ALL
ORDER BY tables DESC
LIMIT 20;

.print '=== Column datatype distribution (all domains) ==='
SELECT data_type, count(*) AS n
FROM STG_ALL_SOCRATA_COLUMN
GROUP BY ALL
ORDER BY n DESC;

-- ═══════════════════════════════════════════════════════════════════════
-- LOAD: full refresh into DuckLake (all sources)
-- ═══════════════════════════════════════════════════════════════════════

-- DuckLake internal catalog
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

-- Rule4 self-description
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
  ('rule4', 'main', 'rule4_table', 'catalog_name',    1, 'VARCHAR', 'Source catalog identifier'),
  ('rule4', 'main', 'rule4_table', 'schema_name',     2, 'VARCHAR', 'Schema within catalog'),
  ('rule4', 'main', 'rule4_table', 'table_name',      3, 'VARCHAR', 'Table name within schema'),
  ('rule4', 'main', 'rule4_table', 'source_type',     4, 'VARCHAR', 'Source system type: socrata, ducklake, rule4, ...'),
  ('rule4', 'main', 'rule4_table', 'table_type',      5, 'VARCHAR', 'TABLE, VIEW, etc.'),
  ('rule4', 'main', 'rule4_table', 'description',     6, 'VARCHAR', 'Human-readable description'),
  ('rule4', 'main', 'rule4_column', 'catalog_name',      1, 'VARCHAR', 'Source catalog identifier'),
  ('rule4', 'main', 'rule4_column', 'schema_name',       2, 'VARCHAR', 'Schema within catalog'),
  ('rule4', 'main', 'rule4_column', 'table_name',        3, 'VARCHAR', 'Table this column belongs to'),
  ('rule4', 'main', 'rule4_column', 'column_name',       4, 'VARCHAR', 'Column name'),
  ('rule4', 'main', 'rule4_column', 'ordinal_position',  5, 'INTEGER', '1-based column position'),
  ('rule4', 'main', 'rule4_column', 'data_type',         6, 'VARCHAR', 'Data type (source-native type name)'),
  ('rule4', 'main', 'rule4_column', 'description',       7, 'VARCHAR', 'Human-readable description');

-- Full refresh
DELETE FROM lake.rule4_table;
INSERT INTO lake.rule4_table
  SELECT * FROM STG_ALL_SOCRATA_TABLE
  UNION ALL
  SELECT * FROM STG_DUCKLAKE_TABLE
  UNION ALL
  SELECT * FROM STG_RULE4_TABLE;

DELETE FROM lake.rule4_column;
INSERT INTO lake.rule4_column
  (catalog_name, schema_name, table_name, column_name, ordinal_position, data_type, description)
  SELECT catalog_name, schema_name, table_name, column_name, ordinal_position, data_type, description
  FROM STG_ALL_SOCRATA_COLUMN
  UNION ALL
  SELECT catalog_name, schema_name, table_name, column_name, ordinal_position, data_type, description
  FROM STG_DUCKLAKE_COLUMN
  UNION ALL
  SELECT catalog_name, schema_name, table_name, column_name, ordinal_position, data_type, description
  FROM STG_RULE4_COLUMN;

-- ═══════════════════════════════════════════════════════════════════════
-- VERIFY
-- ═══════════════════════════════════════════════════════════════════════

.print '=== Loaded totals ==='
SELECT 'tables' AS entity, count(*) AS n FROM lake.rule4_table
UNION ALL
SELECT 'columns', count(*) FROM lake.rule4_column;

.print '=== Tables by source ==='
SELECT source_type, count(*) AS tables
FROM lake.rule4_table
GROUP BY ALL
ORDER BY tables DESC;

.print '=== Top 20 catalogs by table count ==='
SELECT catalog_name, count(*) AS tables
FROM lake.rule4_table
WHERE source_type = 'socrata'
GROUP BY ALL
ORDER BY tables DESC
LIMIT 20;
