/*  column_role — SQLite projection.  VERIFIED against a live sample.

    Common denominator: columns, pk, fk (with referenced columns), index.
    Exceptions (vs the full metamodel):
      - no schemas (single namespace; db/schema_name are the literal 'main');
      - dynamic typing — data_type is the *declared* affinity hint, not enforced;
      - no procedures / parameters; no CDC/CT.
    Not a CDC source itself; included to find the cross-dialect common denominator.

    Driven by the table-valued PRAGMA functions joined against sqlite_master, so it is
    one set-based query rather than per-table PRAGMA round-trips.
    28-column shape (see docs/metamodel.md).
*/
CREATE VIEW column_role AS

-- columns: tables and views
SELECT 'main' AS db, 'main' AS schema_name, m.name AS object_name, NULL AS object_id,
       m.type AS container_kind, m.type AS grouping_kind, NULL AS grouping_id, NULL AS grouping_name,
       ti.name AS member_name, ti.cid AS member_id, ti.cid AS ordinal,
       ti.type AS data_type, NULL AS max_length, NULL AS precision, NULL AS scale,
       NOT ti."notnull" AS is_nullable, ti.dflt_value AS default_expr, 0 AS is_identity, 0 AS is_computed,
       NULL AS is_descending, NULL AS is_included, NULL AS is_unique,
       NULL AS referenced_object, NULL AS referenced_member, NULL AS on_delete,
       NULL AS param_direction, NULL AS cdc_enabled, NULL AS ct_enabled
FROM sqlite_master m, pragma_table_info(m.name) ti
WHERE m.type IN ('table', 'view')

UNION ALL
-- primary key (pragma_table_info.pk is the 1-based position within the PK, 0 = not a member)
SELECT 'main','main', m.name, NULL, 'table', 'pk', NULL, NULL,
       ti.name, ti.cid, ti.pk,
       NULL,NULL,NULL,NULL,NULL,NULL,0,0,
       NULL, 0, 1, NULL,NULL,NULL, NULL,NULL,NULL
FROM sqlite_master m, pragma_table_info(m.name) ti
WHERE m.type = 'table' AND ti.pk > 0

UNION ALL
-- foreign keys (referencing -> referenced pairing)
SELECT 'main','main', m.name, NULL, 'table', 'fk', fk.id, NULL,
       fk."from", NULL, fk.seq,
       NULL,NULL,NULL,NULL,NULL,NULL,0,0,
       NULL,NULL,NULL,
       fk."table", fk."to", fk.on_delete, NULL,NULL,NULL
FROM sqlite_master m, pragma_foreign_key_list(m.name) fk
WHERE m.type = 'table'

UNION ALL
-- explicitly-created indexes (origin='c'); auto PK/UK indexes excluded
SELECT 'main','main', m.name, NULL, 'table', 'index', NULL, il.name,
       ix.name, NULL, ix.seqno,
       NULL,NULL,NULL,NULL,NULL,NULL,0,0,
       ix.desc, 0, il."unique", NULL,NULL,NULL, NULL,NULL,NULL
FROM sqlite_master m, pragma_index_list(m.name) il, pragma_index_xinfo(il.name) ix
WHERE m.type = 'table' AND il.origin = 'c' AND ix.key = 1;
