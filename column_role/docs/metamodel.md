# Column Role Metamodel

*A database is a thing with attributes.* Codd's Rule 4 says the schema is ordinary data;
push that to its conclusion and a database — and the procedural surface around it —
reduces to **columns playing roles in groupings**. Describe the whole thing as **one
long, denormalized relation** where every row is *a member (a column or a parameter)
playing a role in a grouping*: a table is a grouping of columns, a PK / UK / index is a
grouping, a foreign key is a grouping that points at another grouping, a view / TVF /
single-result-set proc each *emit* a grouping (a row-set), and a routine's parameters are
a grouping (a signature).

This is the catalog in roughly sixth-normal form, projected back out wide. It is a
**universal descriptor**: each dialect's idiosyncratic catalog (`sys.*`, `pg_catalog`,
`duckdb_*`, SQLite pragmas) *projects into* this one shape (`sql/<dialect>.sql`). That's
the right kind of abstraction — you abstract the **output**, not the per-source query.

## Why denormalized is fine

Normalization prevents **update** anomalies. This relation is **never updated** — it is
append-only schema *history*: each capture is an immutable snapshot. No updates, no
anomalies, so denormalize freely (grouping attributes repeated onto each member row, the
hierarchy flattened) and optimize for reading. Stored *in a DuckLake* the wide form is the
query surface and the snapshots are the bitemporal lineage — what shape a table had last
quarter, when an FK appeared, when a column widened.

## The relation

```
column_role(
  db, schema_name, object_name, object_id,
  container_kind,        -- table | view | tvf | proc        (what the object IS)
  grouping_kind,         -- table|view|tvf|proc | pk|uk|fk|index | parameter
  grouping_id, grouping_name,
  member_name, member_id, ordinal,
  data_type, max_length, precision, scale, is_nullable,
  default_expr, is_identity, is_computed,           -- expression EDGE: carried, not modeled
  is_descending, is_included, is_unique,            -- key / index members
  referenced_object, referenced_member, on_delete,  -- fk: the column-pairing
  param_direction,                                  -- parameter: in | out | inout
  cdc_enabled, ct_enabled                           -- container flags (SQL Server)
)
```

| `grouping_kind` | a row is… | populates |
|---|---|---|
| `table` | a real column — the canonical definition | type/len/nullable/identity/computed (+ `cdc_enabled`/`ct_enabled` where the dialect has them) |
| `view` `tvf` `proc` | an output column of a row-set producer | type/nullable (resolved) |
| `pk` `uk` | a key column | `ordinal`, `is_descending` |
| `index` | an index column | `ordinal`, `is_descending`, `is_included`, `is_unique` |
| `fk` | a referencing column | `referenced_object`, `referenced_member`, `on_delete` |
| `parameter` | a routine input/output parameter | type, `ordinal`, `param_direction` |

**Containers unify.** A view, a TVF, and a (single-result-set) proc are just more
column-containers — same shape as `table`. The only new *member* type is the
**parameter** (columns of a *signature*), which is what lets a sproc/TVF be a *pollable*
source. **The wall:** expression-valued things — CHECK constraints, computed-column and
default *expressions* — are attributes you carry, not memberships.

## What each dialect can fill

Idiosyncratic by design — the projections are *not* uniform, only their output is:

| | columns | pk/uk | fk (+ ref cols) | index | params | cdc/ct |
|---|---|---|---|---|---|---|
| **SQL Server** | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ (`is_tracked_by_cdc`, change-tracking) |
| **PostgreSQL** | ✓ | ✓ | ✓ | ✓ | ✓ (function args) | publications ≈ cdc |
| **DuckDB** | ✓ | ✓ | ✓ | partial (no clean column list) | — | — |
| **SQLite** | ✓ | ✓ | ✓ | ✓ | — | — |

(DuckDB/SQLite verified against a live sample; SQL Server/PostgreSQL are careful
strawmen pending a server to run them against.)

## How a consumer reads it

One relation, three jobs, all set-based: `grouping_kind='table' AND cdc_enabled` →
`create_table` per table (via the type-map); `grouping_kind='pk'` → the diff/identity key
for updates/deletes; `grouping_kind='fk'` → the dimension-vs-measure signal.
