/*  column_role — DuckDB projection.  VERIFIED against a live sample.

    Common denominator: columns, pk/uk, fk (with referenced columns).
    Exceptions (vs the full metamodel):
      - no clean index column list (duckdb_indexes() exposes only sql/expressions);
      - no procedures / parameters in the SQL-Server sense;
      - no CDC/CT — in DuckDB the change mechanism IS DuckLake snapshots.
    Not a CDC source itself; included to find the cross-dialect common denominator.

    28-column shape (see docs/metamodel.md). NULL where a role/dialect doesn't apply.
*/
CREATE OR REPLACE VIEW column_role AS

-- columns: tables and views
SELECT c.table_catalog                                  AS db,
       c.table_schema                                   AS schema_name,
       c.table_name                                     AS object_name,
       NULL::BIGINT                                     AS object_id,
       CASE t.table_type WHEN 'VIEW' THEN 'view' ELSE 'table' END AS container_kind,
       CASE t.table_type WHEN 'VIEW' THEN 'view' ELSE 'table' END AS grouping_kind,
       NULL::BIGINT                                     AS grouping_id,
       NULL                                             AS grouping_name,
       c.column_name                                    AS member_name,
       c.ordinal_position                               AS member_id,
       c.ordinal_position                               AS ordinal,
       c.data_type                                      AS data_type,
       c.character_maximum_length                       AS max_length,
       c.numeric_precision                              AS precision,
       c.numeric_scale                                  AS scale,
       (c.is_nullable = 'YES')                          AS is_nullable,
       c.column_default                                 AS default_expr,
       FALSE                                            AS is_identity,
       FALSE                                            AS is_computed,
       NULL::BOOLEAN AS is_descending, NULL::BOOLEAN AS is_included, NULL::BOOLEAN AS is_unique,
       NULL AS referenced_object, NULL AS referenced_member, NULL AS on_delete,
       NULL AS param_direction, NULL::BOOLEAN AS cdc_enabled, NULL::BOOLEAN AS ct_enabled
FROM information_schema.columns c
JOIN information_schema.tables  t USING (table_catalog, table_schema, table_name)
WHERE c.table_schema NOT IN ('information_schema', 'pg_catalog')

UNION ALL

-- pk / uk / fk: duckdb_constraints() carries the column names AND (for fk) referenced cols
SELECT con.database_name, con.schema_name, con.table_name, con.table_oid, 'table',
       CASE con.constraint_type WHEN 'PRIMARY KEY' THEN 'pk'
                                WHEN 'UNIQUE'      THEN 'uk'
                                WHEN 'FOREIGN KEY' THEN 'fk' END,
       con.constraint_index, con.constraint_name,
       cn.member_name, cn.ord, cn.ord,
       NULL, NULL, NULL, NULL, NULL, NULL, FALSE, FALSE,
       NULL, FALSE, (con.constraint_type IN ('PRIMARY KEY','UNIQUE')),
       con.referenced_table,
       CASE WHEN con.constraint_type = 'FOREIGN KEY'
            THEN con.referenced_column_names[cn.ord] END,
       NULL, NULL, NULL, NULL
FROM duckdb_constraints() con,
     unnest(con.constraint_column_names) WITH ORDINALITY AS cn(member_name, ord)
WHERE con.constraint_type IN ('PRIMARY KEY', 'UNIQUE', 'FOREIGN KEY');
