/*  column_role — PostgreSQL projection.  STRAWMAN (no server to verify against yet).

    Common denominator: columns, pk/uk, fk (with referenced columns), index.
    Exceptions (vs the full metamodel):
      - no SQL-Server CDC/CT; the nearest is logical-replication membership, so
        cdc_enabled ::= "table is in a PUBLICATION";
      - "procedures returning a result set" aren't a first-class concept — set-returning
        FUNCTIONS (proretset) are the TVF analogue; their RETURNS TABLE columns and
        parameters both live in pg_proc arrays (left as a follow-up branch).

    28-column shape (see docs/metamodel.md).
*/
CREATE OR REPLACE VIEW column_role AS

-- columns: tables, partitioned tables, views, matviews, foreign tables
SELECT current_database() AS db, n.nspname AS schema_name, c.relname AS object_name, c.oid::bigint AS object_id,
       CASE c.relkind WHEN 'v' THEN 'view' WHEN 'm' THEN 'view' ELSE 'table' END AS container_kind,
       CASE c.relkind WHEN 'v' THEN 'view' WHEN 'm' THEN 'view' ELSE 'table' END AS grouping_kind,
       c.oid::bigint AS grouping_id, NULL AS grouping_name,
       a.attname AS member_name, a.attnum AS member_id, a.attnum AS ordinal,
       format_type(a.atttypid, a.atttypmod) AS data_type, NULL::int AS max_length, NULL::int AS precision, NULL::int AS scale,
       NOT a.attnotnull AS is_nullable, pg_get_expr(ad.adbin, ad.adrelid) AS default_expr,
       (a.attidentity <> '') AS is_identity, (a.attgenerated <> '') AS is_computed,
       NULL::bool AS is_descending, NULL::bool AS is_included, NULL::bool AS is_unique,
       NULL AS referenced_object, NULL AS referenced_member, NULL AS on_delete, NULL AS param_direction,
       EXISTS (SELECT 1 FROM pg_publication_tables pt
               WHERE pt.schemaname = n.nspname AND pt.tablename = c.relname) AS cdc_enabled,
       NULL::bool AS ct_enabled
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
LEFT JOIN pg_attrdef ad ON ad.adrelid = c.oid AND ad.adnum = a.attnum
WHERE c.relkind IN ('r','p','v','m','f') AND n.nspname NOT IN ('pg_catalog','information_schema')

UNION ALL
-- pk / uk  (unnest the conkey attnum array, ordered)
SELECT current_database(), n.nspname, rel.relname, rel.oid::bigint, 'table',
       CASE con.contype WHEN 'p' THEN 'pk' ELSE 'uk' END, con.oid::bigint, con.conname,
       a.attname, a.attnum, k.ord,
       NULL,NULL,NULL,NULL,NULL,NULL,false,false,
       NULL, false, true, NULL,NULL,NULL, NULL,NULL,NULL
FROM pg_constraint con
JOIN pg_class rel ON rel.oid = con.conrelid
JOIN pg_namespace n ON n.oid = rel.relnamespace
JOIN unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord) ON true
JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum
WHERE con.contype IN ('p','u')

UNION ALL
-- fk: conkey (local) and confkey (referenced) unnested in parallel
SELECT current_database(), n.nspname, rel.relname, rel.oid::bigint, 'table', 'fk', con.oid::bigint, con.conname,
       la.attname, la.attnum, k.ord,
       NULL,NULL,NULL,NULL,NULL,NULL,false,false,
       NULL,NULL,NULL,
       rref.relname, ra.attname,
       CASE con.confdeltype WHEN 'a' THEN 'NO ACTION' WHEN 'r' THEN 'RESTRICT' WHEN 'c' THEN 'CASCADE'
                            WHEN 'n' THEN 'SET NULL' WHEN 'd' THEN 'SET DEFAULT' END,
       NULL,NULL,NULL
FROM pg_constraint con
JOIN pg_class rel ON rel.oid = con.conrelid
JOIN pg_namespace n ON n.oid = rel.relnamespace
JOIN pg_class rref ON rref.oid = con.confrelid
JOIN unnest(con.conkey, con.confkey) WITH ORDINALITY AS k(la_num, ra_num, ord) ON true
JOIN pg_attribute la ON la.attrelid = con.conrelid AND la.attnum = k.la_num
JOIN pg_attribute ra ON ra.attrelid = con.confrelid AND ra.attnum = k.ra_num
WHERE con.contype = 'f'

UNION ALL
-- non-constraint indexes (indoption bit 0 = DESC)
SELECT current_database(), n.nspname, tc.relname, tc.oid::bigint, 'table', 'index', i.indexrelid::bigint, ic.relname,
       a.attname, a.attnum, k.ord,
       NULL,NULL,NULL,NULL,NULL,NULL,false,false,
       ((i.indoption[k.ord-1] & 1) = 1), false, i.indisunique, NULL,NULL,NULL, NULL,NULL,NULL
FROM pg_index i
JOIN pg_class ic ON ic.oid = i.indexrelid
JOIN pg_class tc ON tc.oid = i.indrelid
JOIN pg_namespace n ON n.oid = tc.relnamespace
JOIN unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
WHERE NOT i.indisprimary AND k.attnum > 0 AND n.nspname NOT IN ('pg_catalog','information_schema');
