/*  column_role — SQL Server projection.  STRAWMAN (no server to verify against yet).

    The richest dialect — the one that actually drives CDC/CT federation:
      - cdc_enabled  = sys.tables.is_tracked_by_cdc
      - ct_enabled   = membership in sys.change_tracking_tables
      - procs / TVFs / parameters are first-class (the pollable signatures)

    28-column shape (see docs/metamodel.md). NULL where a role doesn't apply.
*/
CREATE OR ALTER VIEW dbo.column_role AS

-- (1) row-set columns: tables, views, table-valued functions (+ CDC/CT flags)
SELECT DB_NAME() AS db, s.name AS schema_name, o.name AS object_name, o.object_id AS object_id,
       CASE o.type WHEN 'U' THEN 'table' WHEN 'V' THEN 'view' ELSE 'tvf' END AS container_kind,
       CASE o.type WHEN 'U' THEN 'table' WHEN 'V' THEN 'view' ELSE 'tvf' END AS grouping_kind,
       o.object_id AS grouping_id, CAST(NULL AS sysname) AS grouping_name,
       c.name AS member_name, c.column_id AS member_id, c.column_id AS ordinal,
       ty.name AS data_type, c.max_length, c.precision, c.scale, c.is_nullable,
       dc.definition AS default_expr, c.is_identity, c.is_computed,
       CAST(NULL AS bit) AS is_descending, CAST(NULL AS bit) AS is_included, CAST(NULL AS bit) AS is_unique,
       CAST(NULL AS sysname) AS referenced_object, CAST(NULL AS sysname) AS referenced_member,
       CAST(NULL AS nvarchar(60)) AS on_delete, CAST(NULL AS nvarchar(10)) AS param_direction,
       t.is_tracked_by_cdc AS cdc_enabled,
       CAST(IIF(ct.object_id IS NOT NULL, 1, 0) AS bit) AS ct_enabled
FROM sys.objects o
JOIN sys.schemas s ON s.schema_id = o.schema_id
JOIN sys.columns c ON c.object_id = o.object_id
JOIN sys.types  ty ON ty.user_type_id = c.user_type_id
LEFT JOIN sys.default_constraints dc ON dc.object_id = c.default_object_id
LEFT JOIN sys.tables t ON t.object_id = o.object_id
LEFT JOIN sys.change_tracking_tables ct ON ct.object_id = o.object_id
WHERE o.type IN ('U','V','IF','TF')

UNION ALL
-- (2) primary keys / unique constraints
SELECT DB_NAME(), s.name, o.name, o.object_id, 'table',
       CASE kc.type WHEN 'PK' THEN 'pk' ELSE 'uk' END, kc.object_id, kc.name,
       c.name, c.column_id, ic.key_ordinal,
       NULL,NULL,NULL,NULL,NULL, NULL,NULL,NULL,
       ic.is_descending_key, CAST(0 AS bit), CAST(1 AS bit),
       NULL,NULL,NULL, NULL, NULL,NULL
FROM sys.key_constraints kc
JOIN sys.objects o ON o.object_id = kc.parent_object_id
JOIN sys.schemas s ON s.schema_id = o.schema_id
JOIN sys.index_columns ic ON ic.object_id = kc.parent_object_id AND ic.index_id = kc.unique_index_id
JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id

UNION ALL
-- (3) non-constraint indexes
SELECT DB_NAME(), s.name, o.name, o.object_id, 'table', 'index', i.index_id, i.name,
       c.name, c.column_id, ic.key_ordinal,
       NULL,NULL,NULL,NULL,NULL, NULL,NULL,NULL,
       ic.is_descending_key, ic.is_included_column, i.is_unique,
       NULL,NULL,NULL, NULL, NULL,NULL
FROM sys.indexes i
JOIN sys.objects o ON o.object_id = i.object_id
JOIN sys.schemas s ON s.schema_id = o.schema_id
JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE i.is_primary_key = 0 AND i.is_unique_constraint = 0 AND i.type IN (1,2)

UNION ALL
-- (4) foreign keys (the referencing -> referenced pairing)
SELECT DB_NAME(), s.name, po.name, po.object_id, 'table', 'fk', fk.object_id, fk.name,
       pc.name, fkc.parent_column_id, fkc.constraint_column_id,
       NULL,NULL,NULL,NULL,NULL, NULL,NULL,NULL, NULL,NULL,NULL,
       ro.name, rc.name, fk.delete_referential_action_desc, NULL, NULL,NULL
FROM sys.foreign_keys fk
JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
JOIN sys.objects po ON po.object_id = fk.parent_object_id
JOIN sys.schemas s ON s.schema_id = po.schema_id
JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
JOIN sys.objects ro ON ro.object_id = fk.referenced_object_id

UNION ALL
-- (5) routine parameters (the pollable signature)
SELECT DB_NAME(), s.name, o.name, o.object_id,
       CASE o.type WHEN 'P' THEN 'proc' WHEN 'FN' THEN 'proc' ELSE 'tvf' END, 'parameter', o.object_id, NULL,
       p.name, p.parameter_id, p.parameter_id,
       ty.name, p.max_length, p.precision, p.scale, CAST(1 AS bit),
       NULL,NULL,NULL, NULL,NULL,NULL, NULL,NULL,NULL,
       CASE WHEN p.is_output = 1 THEN 'out' ELSE 'in' END, NULL,NULL
FROM sys.parameters p
JOIN sys.objects o ON o.object_id = p.object_id
JOIN sys.schemas s ON s.schema_id = o.schema_id
JOIN sys.types  ty ON ty.user_type_id = p.user_type_id
WHERE o.type IN ('P','IF','TF','FN') AND p.parameter_id > 0;
GO

/*  (6) procedure result-set columns — the "single result set" assumption — via
    sys.dm_exec_describe_first_result_set_for_object(object_id, 0). Kept out of the view
    (DMV needs elevated permission and yields nothing for non-static shapes); UNION ALL
    it into a materialised capture when needed. */
