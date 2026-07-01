# Catalog as Data

*A read-only, variable-resolution, reconstruction-deferred database replica.*

This is the synthesis that fell out of building the pieces. It extends the [[Column Role
Metamodel]] from "describe the schema as data" to "describe **any** database object as data,
capture it over time as a bitemporal replica, and never write to the source." Everything below is
one idea wearing different hats.

---

## 1. The premise, and the one move

Codd's Rule 4: the schema is ordinary data. The catalog *is* the DBMS describing itself in
relations. So there is exactly one move, made over and over at every grain:

> **Project the authoritative catalog relation that *defines* a thing.**

We never model a thing; we mirror how the server already defines it. A column is defined by a
`sys.columns` row; a table by a `sys.objects` row; an index by `sys.indexes ⋈ sys.index_columns`;
a **database** by a `sys.databases` row; a **login** by a `sys.server_principals` row. Same move,
different grain.

This is why it *can't not work*. There is no inference, no heuristic, no model that could be
**wrong** — only **incomplete** (an essence you didn't record). The single failure axis is
**coverage, not correctness**. Contrast a sync framework, whose triggers can miss and whose
conflict resolution can diverge: it *can* be wrong. This can only be partial. Reconstruction is
deferred to a store that is *also* correct-by-construction (Snodgrass), so the whole pipeline is
correct end to end, and the only thing you ever tune is how much of it you bother to capture.

## 2. The problem it solves

Render a database you may only **read** as a local, queryable, time-travelling copy. The tools
that own this space — Dotmim.Sync, Microsoft Sync Framework, SQL Server replication — share an
architecture that disqualifies them: they **provision the source** (tracking tables, triggers,
`scope_info`), so **a client needs write/DDL permission on the publisher just to sync**. You
cannot replicate a catalog you are only allowed to read. That is the common case (a locked-down
production database), and it is the showstopper.

## 3. Four organizing decisions

1. **The source is read-only.** Nothing is ever provisioned there. Ride whatever change-signal the
   source already exposes (CDC, Change Tracking, a user-modeled `updated_at`, a trigger-maintained
   backlog); where there is none, fall back to a signal-free whole-row `EXCEPT` diff. All
   control/status lives in the replica or a third-party registry — **never in the source**.
2. **Reconstruction is deferred to [[DuckLake OOB Writer|DuckLake]].** We append *after-images* and
   let the store's inline MVCC and `AT (VERSION => …)` time-travel do the temporal reconstruction.
   The hard part is the store's job.
3. **The catalog is described as data, across dialects.** A config-as-data meta-schema *generates*
   each dialect's catalog queries. Adding a dialect or an essence is data entry, not code.
4. **Fidelity is a dial.** The same substrate yields many derived products, each priced to its own
   expressiveness (§8).

## 4. Catalog as data — the meta-schema

An **essence** is a kind-of-thing you can project from a catalog (`column`, `index`,
`primary_key`, `foreign_key`, `check`, `unique`, `extended_property`, `stats_histogram`,
`database`, `login`, `schema`, `database_principal`, …). The word is meant in **Locke's** sense —
a *nominal* essence (the bundle of properties we attach to a name, "the workmanship of the
understanding"), never Aristotle's intrinsic *real* essence. We only ever have the catalog's
account of a thing, so the essence is *chosen* — and §6 turns that choosing into a dial. The
registry describes, per dialect, how to project each one:

```
catalog_source(dialect, essence, from_sql, where_sql, change_signal)
catalog_attr  (dialect, essence, ord, attr, expr)     -- which columns ARE this essence
```

`generate_projection(dialect, essence)` reads these and builds a SQLAlchemy Core `select()` — no
string assembly, no ORM ceremony (the "tables" are dynamic catalog relations, so Core `table()`/
`column()` is the precise tool).

### The JOIN as relational data

`from_sql` started as an opaque SQL blob — the one un-relational smell. It was decomposed:

```
catalog_table(dialect, essence, alias, expr, is_root)      -- which catalog tables
catalog_join (dialect, essence, child, parent,             -- child references parent
              child_col, parent_col, is_outer)             -- columns usually NULL
catalog_key  (dialect, table_name, ord, col)               -- each table's identity, DRY
```

The join *columns* are **inferred** from the parent's identity key (the catalog convention that an
FK column shares the referenced PK's name), recorded explicitly only where the catalog breaks it
(`kc.parent_object_id → o.object_id`, `ic.index_id → kc.unique_index_id`). `is_outer` gives
`LEFT OUTER` for optional sides (a column's DEFAULT: most have none). So `catalog_join` is mostly
pure graph structure; the engine walks it into a real `Join`. Essences whose joins don't decompose
— `LATERAL unnest`, correlated TVFs (`CROSS APPLY sys.dm_db_stats_histogram`) — keep a small
`from_sql` fragment. It is a fallback, not the model.

### Self-hosting

The registry's durable home is DuckLake itself — schema-as-data in the same temporal store the
scraper produces (`to_lake` / `load_from_lake`). The scraper's config time-travels and diffs by
the same machinery as its output. Turtles.

## 5. Objects at any grain — the proxy principle

The ontological keystone. A database or a login is a first-class **object that is not itself a
relation** and has no attributes to project directly. But **the catalog relation that *defines* it
is a relation with attributes — the proxy.** We operate on the proxy. This is not a special case;
it is the general case seen clearly:

| thing | proxy (defining relation) | grain |
|---|---|---|
| column | `sys.columns` row | column |
| table | `sys.objects` row | object |
| schema | `sys.schemas` row | object |
| database | `sys.databases` row | server object |
| login | `sys.server_principals` row | server object |

Because the proxy *is* a relation, **all the relational machinery applies uniformly** — TTST it,
`EXCEPT`-diff it, time-travel it, join it to its extended properties. The object being
non-relational never bites, because it is never in the pipeline; only its relational shadow is.

**Rule4 sharpened:** not *"a database is a thing with attributes"* but *"a database is a thing
**defined by** a relation with attributes — and the definition is what we compute on."* The proxy
is the bridge from the opaque object into the relational algebra where diff, time-travel, and
lineage live. And the delight has a name: it is the **fixpoint** — the catalog is the DBMS being
Rule4 about *itself*, relations describing relations up to the server and down to the column. There
was never a bottom or a top, just proxies.

### Facets — richer proxies

Some essences enrich the object rather than structure it:

- **`extended_property`** (over the friendly `RULE4.extended_property` view — schema, INSTEAD-OF
  triggers, and a `break_ownership_user` + `ALTER AUTHORIZATION` that breaks the ownership chain
  for emergent row-level access control). Captures the human/curatorial layer:
  `survey.classification = measure | dimension`. Extended properties attach to *any* object, keyed
  by the proxy's catalog identity (`class` + `major_id`/`minor_id`), so a classification on a login
  or a schema finds its home.
- **`stats_histogram`** (`CROSS APPLY sys.dm_db_stats_histogram`). The data *distribution* — the
  raw material for the classification signals (cardinality ratio, repeatability, discreteness) the
  whole project is named for.

Both project `sql_variant` values through the base-type-safe convert (binary → `VARBINARY` style-1
hex) to dodge the UTF-16 surrogate trap on non-text values.

## 6. How many relations define a thing? — variable resolution

"It depends," and that is the point: **a thing has no canonical definition — its definition is a
*query*.** A database is one `sys.databases` row if you're indexing it; join `sys.database_files`
and it's a storage object; join `sys.database_principals` + permissions and it's a security object;
add extended properties and it's classified. Same thing, different closures over the catalog. Not
really ironic that we call them "essences," read à la Locke (§4): the *nominal* essence simply
**is** the chosen definition — there is no accessible *real* essence of a database to be
unfaithful to. The dial is just choosing a thin nominal essence or a thick one.

Two independent knobs, which the "lens/focal-point" metaphor conflates:

- **how much** you resolve — zoom / level-of-detail. A city is a dot on the world map and a street
  grid up close. "How many relations define the thing" *is* the LOD setting.
- **which aspect** you resolve — thematic, not focal. The same terrain gets a road map, a
  geological map, a watershed map.

Less "one lens with a focal point," more a **microscope with interchangeable objectives** crossed
with **stains**. Or, most Rule4 of all: it is just `SELECT` and `JOIN` — resolution is how many
relations you close over, theme is which ones. Every definition is a view; none is privileged.

And the limit case names itself. A **yoke** — Irish English for an under-specified thing, a
whatchamacallit — is a thing with *no* nominal essence yet: un-named, un-projected, pure referent.
The meta-schema is a machine for turning yokes into essences — point it at some opaque yoke on the
dataserver and it hands back a named bundle of attributes. Rule4 as a **nominal-essence factory**:
the certainty that it "can't not work" is the felt-sense of that machine having no failure mode
but incompleteness — you are only ever naming what is already, authoritatively, there.

## 7. Change detection

Two tiers, chosen by what the source offers:

- **`change_signal`** — where a cheap monotonic column exists (`modify_date`, CT version, CDC LSN),
  tail with `WHERE change_signal > :hwm`. Only SQL Server structural catalogs reliably have one.
- **`EXCEPT`-diff** — signal-free. Register the current scrape and the previous as Arrow tables in
  DuckDB and take the whole-row set difference both ways (added / removed). No PK required; works on
  any essence, including the object-grain TTSTs (schemas added between captures fall straight out).

## 8. The replica organism

`Replica.sync()` wires it into one poll → apply cycle:

1. **scrape** the source's `column` essence → shape/evolve the replica table;
2. **stream** the payload via a driver (§9) into a `HistoryReplica` (after-images);
3. **capture facets** (`capture_essence`) — extended properties, histograms — as bitemporal
   snapshots alongside the data;
4. **advance the watermark**.

Control/status is a pluggable `ControlStore`, **never the source**:

- **`ReplicaControl`** (common) — state in the replica's own DuckLake, appended per sync, so the
  watermark trail is itself bitemporal history.
- **`RegistryControl`** — a third-party registry, for when the replica target is administratively
  locked down (replicating a *subset* into a schema you can't extend).

`capture_essence(essence, now, keep=, name=)` is the generalization: `capture_facet` was never
facet-specific — it snapshots *an essence into DuckLake*. Point it at `login` / `database` /
`schema` with no filter and you retain a **bitemporal TTST for anything the dataserver catalogs**,
with `EXCEPT`-diff change detection, source read-only.

## 9. Drivers and after-images

The payload seam is `changed_since` / `sync`. Only the *driver* is model-specific; the apply side
is generic. Drivers: `UserColumnDriver` (`updated_at`), `ChangeTrackingDriver` (version),
`CDCDriver` (`__$start_lsn`), `BacklogDriver` (trigger table). All feed one **after-image** apply
path — temporal replicas store after-images, not before-images, because reconstruction is then a
forward stateless selection, not a backward fold. `sync` staples rows by transaction-time (`__tt`),
grouping each source commit into one DuckLake snapshot.

## 10. Reconstruction deferred, and the two clocks

DuckLake's inline MVCC (`begin/end_snapshot`) *is* the bitemporal interval. Time-travel is
`AT (VERSION => n)` or `AT (TIMESTAMP => t)`. But **order by the logical clock, `VERSION`** —
`snapshot_time` interleaves ingest-time (`now`) and source data-time (`updated_at`) non-
monotonically, so wall-clock time-travel is unreliable *by construction*. Carry two clocks per
fact: transaction-time for order/label, logical-time for integrity. TT/LT divergence is itself an
event (LT reset → drop-recreate; TT backward → clock skew).

## 11. Derived products — the fidelity dial, end to end

| product | cost | needs |
|---|---|---|
| **metadata-only facade** | cheapest — nothing materialised | catalog metadata + DuckDB federation (`read_parquet`, ODBC scanner, `ATTACH`) → generated `CREATE VIEW`/`ATTACH` DDL |
| **append-only TTST replica** | cheap | `column` essence + type mapping + payload + change-detection |
| **schema-history** | free once scraped | the catalog time-series in DuckLake |
| **migration DDL** | cheap | the difference between two schema captures |
| **SQL-Compare-grade clone** | asymptotic | the whole essence catalog + DDL dependency ordering |

The facade deserves its own line: because reconstruction lives in the store, *"give me a DuckDB
database presenting these Parquet exports, that ODBC source, and those replicated tables as one
schema"* is a **metadata-mostly operation** — read the catalog, emit the view/attach DDL, done.
See [[Disaggregated Lakehouse]], [[Data As Control Plane]].

## 12. Related work

vs **Dotmim.Sync** and the trigger-provisioning class: (1) read-only source, nothing provisioned
there — the showstopper dissolved; (2) reconstruction deferred to DuckLake, not owned by the
framework; (3) catalog described as data across dialects; (4) derived products a spectrum. The
trade, stated plainly: we give up **bidirectional convergence** (this is read-only replication, not
sync) to gain **non-intrusiveness, read-only sourcing, and store-deferred reconstruction**.
Log-based CDC (Debezium) is a different point again: non-intrusive on tables but needs replication
privileges and a streaming pipeline, and emits an event stream, not a queryable store.

## 13. The next arc — provenance / lineage

Lineage is just *more authoritative relations*, in three strata:

1. **Dataserver identity (SPID)** — a `session` essence over `sys.dm_exec_sessions ⋈
   sys.dm_exec_connections`: login, remote IP (`client_net_address`), `auth_scheme`, program.
2. **Client self-provenance** — the replica records its *own* identity (its Kerberos principal —
   the `login` essence already surfaced `PHRRNGTN\paulharrington` — PID, parent PID).
3. **Transaction-id correlation** — staple each change to the transaction that made it: CT's
   `sys.dm_tran_commit_table` maps `commit_ts → xdes_id`; CDC's `__$start_lsn` groups a commit.
   "These five row-changes were one transaction."

The honest catch: layer-1 identity is **ephemeral** (DMVs show who's connected *now*). The
transaction id (layer 3) is durable; the identity behind it is not. Full historical committer
identity needs capture at commit time — a write-side audit/trigger (breaks read-only) or, keeping
discipline, a durable **read-only third-party audit store** (SQL Server Audit → a log we scrape).
The same third-party-registry pattern, one level up.

## 14. The honest limits

- The clone is an **asymptote** — the long tail of dialect DDL (filegroups, compression, collation,
  partitioning) is unbounded; 90% cheap, the rest incremental forever.
- **State capture ≠ DDL ordering.** A clone needs the dependency graph; the replica barely cares.
- **Opaque objects** (trigger bodies, PL/pgSQL, Service Broker) diff at whole-object grain — store
  text, reproduce verbatim (the after-image principle); you can't say "the WHERE clause changed."
- **Cross-dialect is structural, not behavioral** — a `db'` on another dialect mirrors shape, not
  semantics (`money` ≠ `numeric` exactly).
- The replica's real limits are **type fidelity** (funky-value round-trip) and **scale** (inline-
  MVCC snapshot count under high-frequency CDC), not schema completeness.
- Committer **identity is ephemeral** (§13).

## 15. Artifacts

- `catalog_meta.py` — the meta-schema engine (`catalog_source/attr/table/join/key`,
  `generate_projection`, `from_clause`, `to_lake`/`load_from_lake`).
- `catalog_seed*.json` — the registry as data: base + per-dialect (postgresql, duckdb) + SQL Server
  ext (check/unique/service_broker/full_text) + joins + facets (extended_property, stats_histogram)
  + objects (database/login/schema/database_principal).
- `catalog_diff.py` — the `EXCEPT` tier.
- `column_collection.py` — essence → replica; the drivers; the `sync` seam.
- `organism.py` — `Replica.sync()`, `ReplicaControl`/`RegistryControl`, `capture_essence`.
- `sql/extended_properties.sql` (in the rule4 repo) — the `RULE4.extended_property` facade + broken
  ownership chain.
- `demo_*.py` — every claim above is a green demo against a live SQL Server (gfe) + PostgreSQL.

See also: [[Column Role Metamodel]], [[DuckLake OOB Writer]], [[Blobrule4 Project]],
[[Composable Relation Builders]].
