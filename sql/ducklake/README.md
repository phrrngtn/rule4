# DuckLake as a Temporal Schema Registry for Heterogeneous Sources

## Motivation

Rule4 implements Codd's Rule 4: the database description is represented at the
logical level in the same way as ordinary data, so authorized users can apply the
same relational language to its interrogation as they apply to regular data.

The existing SQLite implementation demonstrates this for Socrata open-data
catalogs: resource metadata, column definitions, and temporal backlogs are all
ordinary tables, queryable and versionable with standard SQL.

This document describes how to extend that pattern to maintain a
**DuckLake-compatible lakehouse** by translating logical "transaction logs" from
heterogeneous sources into DuckLake snapshots. The result is a single temporal
catalog where schema evolution, data lineage, and point-in-time reconstruction
are all first-class relational operations.

## Background: DuckLake's Temporal Model

DuckLake stores catalog metadata in ordinary tables (`ducklake_table`,
`ducklake_column`, `ducklake_snapshot`, etc.) alongside Parquet data files. Each
mutation to the catalog is wrapped in a **snapshot** — a row in
`ducklake_snapshot` with a `snapshot_time` timestamp and monotonically increasing
`snapshot_id`.

Schema changes (column adds, drops, type changes) are captured via
`begin_snapshot`/`end_snapshot` columns on `ducklake_column`, implementing
**Snodgrass transaction-time** (aka SQL:2011 `SYSTEM_TIME`) semantics: append-only
snapshots, immutable history, system-controlled timeline. Given any `snapshot_id`,
the catalog can be reconstructed to the state it was in at that point.

Key structural facts (confirmed from DuckLake source):

- `snapshot_time` is set to `NOW()` by default but has **no validation
  constraint** — custom values via direct INSERT are possible.
- Time travel resolution uses `snapshot_time` independently of `snapshot_id`
  ordering.
- `next_catalog_id` and `next_file_id` are simple `++` counters managed in
  `ducklake_metadata`.

## The Idea: Source-Authoritative Snapshots

Rather than using DuckLake's high-level API (which assumes it is the single
writer and uses wall-clock time), we **directly populate DuckLake's metadata
tables** with snapshots whose `snapshot_time` reflects the source system's event
time.

This is not backdating. It is replaying the source's causal history in order,
producing a DuckLake catalog whose time axis faithfully represents *when things
happened in the source*, not when we happened to scrape them.

### Single Writer, All Consumers Read-Only

One sync process owns the metadata tables and manages the counter state. All
other consumers attach the DuckLake catalog read-only and query via standard
DuckLake time-travel syntax (`AS OF`).

## Source Adapters

Each source provides a different level of temporal fidelity. The sync process
must translate each source's change semantics into DuckLake snapshots honestly —
no pretending we have more history than we do.

### SQL Server CDC (Change Data Capture)

**Fidelity: Full history with transaction boundaries.**

CDC captures every row version via `__$start_lsn` (Log Sequence Number). The LSN
provides a **total order** across all tables in the database; rows sharing the
same LSN were modified in the same transaction. `sys.fn_cdc_map_lsn_to_time()`
maps LSNs to wall-clock timestamps.

Translation to DuckLake:

| CDC concept | DuckLake concept |
|---|---|
| `__$start_lsn` | Groups rows into a single snapshot |
| `fn_cdc_map_lsn_to_time(lsn)` | `snapshot_time` |
| `__$operation` (1=delete, 2=insert, 3=pre-update, 4=post-update) | Drives INSERT/DELETE on data files; schema changes detected by column diff |
| Shared LSN across tables | Single snapshot spanning multiple `ducklake_table` entries |

CDC enables faithful reconstruction: every intermediate state of every captured
table is preserved, and transaction boundaries are respected.

### SQL Server Change Tracking (CT)

**Fidelity: Current state plus "changed since version N". Intermediate versions
lost.**

Change Tracking tells you *which* rows changed since a given version, but not
*what* the intermediate values were. The only defensible `snapshot_time` is the
poll time — we cannot claim to know when individual changes occurred between
polls.

Translation to DuckLake:

| CT concept | DuckLake concept |
|---|---|
| `CHANGE_TRACKING_CURRENT_VERSION()` | Recorded as provenance metadata |
| Poll timestamp | `snapshot_time` |
| Changed PKs (from `CHANGETABLE`) | Drive selective re-read of current state; full current row becomes new data file |

CT is weaker than CDC but still useful: it tells us *that* something changed,
letting us avoid full-table rescans.

### Socrata Open Data API

**Fidelity: One version per update, event time available.**

Socrata's catalog API (`/api/catalog/v1`) and view API (`/api/views`) provide
resource metadata including `data_updated_at` timestamps. Each scrape captures
the current state of a dataset's schema (column names, types, descriptions,
cached statistics like cardinality and min/max).

Translation to DuckLake:

| Socrata concept | DuckLake concept |
|---|---|
| `data_updated_at` | `snapshot_time` (source-authoritative) |
| Resource metadata JSON | Drives `ducklake_table` and `ducklake_column` population |
| Column definitions (field_name, dataTypeName, renderTypeName) | `ducklake_column` rows with begin/end snapshot lifecycle |
| Cached statistics (cardinality, min, max, count) | Extended metadata (see Provenance below) |

Socrata sits between CDC and CT in fidelity: we get event timestamps but only
one version per update cycle, with no visibility into intermediate states.

## Pseudo-Transaction Log Reconstruction

The sync process reconstructs a **causally consistent global log** from
per-source change streams:

1. **Collect** change events from all sources (CDC change rows, CT change sets,
   Socrata metadata diffs).
2. **Order** events by source-authoritative timestamp. For CDC, the LSN-derived
   timestamp provides sub-second ordering. For CT and Socrata, ordering is
   coarser.
3. **Group** events that share a transaction boundary (same LSN for CDC) into a
   single DuckLake snapshot.
4. **Replay** the ordered log as DuckLake snapshots, advancing counters and
   populating metadata tables.

The result is a single DuckLake catalog where `SELECT * FROM ducklake_snapshot
ORDER BY snapshot_time` tells the complete story across all sources, with
appropriate fidelity markers indicating the provenance of each snapshot.

## Provenance

Each snapshot carries provenance metadata in
`ducklake_snapshot_changes.commit_extra_info`:

- **Source type**: `cdc`, `ct`, `socrata`
- **Source identifier**: database/server for CDC/CT, domain for Socrata
- **Source transaction ID**: LSN for CDC, CT version for CT, `data_updated_at`
  for Socrata
- **Session context** (CDC/CT): login, host, program_name from
  `sys.dm_exec_sessions`
- **OpenTelemetry** (optional): if clients set `trace_id`/`span_id` in
  `SESSION_CONTEXT`, provenance triggers capture them — enabling distributed
  trace lineage from application through database into DuckLake

Provenance is recorded **per statement, not per row**. The cardinality is
manageable: one provenance record per DuckLake snapshot, regardless of how many
rows changed.

## Relationship to Existing Rule4 Components

### Temporal Backlogs (`sql/schema/backlog.sql`)

The existing SQLite temporal backlog system (trigger-maintained `_td_bl_*`
tables) served as the proof of concept. DuckLake's `begin_snapshot`/`end_snapshot`
pattern is the same idea implemented at the catalog level rather than via
userland triggers. The backlog tables continue to serve as the local staging area
for change detection before DuckLake sync.

### Extended Properties (`sql/extended_properties.sql`)

The `RULE4.extended_property` view on SQL Server stores classification results
(dimension vs. measure, cardinality ratio, histogram fingerprints) as
`sql_variant` values. These properties become additional columns or metadata in
the DuckLake catalog — the classification signals computed from SQL Server
histograms travel with the schema they describe.

### Socrata Metadata (`sql/socrata/`)

The existing SQLite tables (`resource`, `resource_column`, `resource_view_column`)
and their temporal backlogs are the staging layer for Socrata. The DuckLake sync
process reads from these tables (or their DuckDB equivalents, once ported) and
translates detected changes into DuckLake snapshots.

### Codegen

CDC capture configurations, provenance triggers, and DuckLake sync procedures
are generated from catalog metadata using the same template-driven approach
(`codegen_template` + `eval(ddl)`) used for temporal backlogs and FTS indexes.
The `rule4_temporal_backlog` and `rule4_fts` metadata tables have a natural
counterpart: a registry of DuckLake sync targets and their source configurations.

## What This Enables

With heterogeneous sources unified in a single DuckLake catalog:

- **Point-in-time schema reconstruction**: "What did the Socrata NYC buildings
  dataset look like on 2024-06-15?" is a time-travel query.
- **Cross-source lineage**: A DuckLake snapshot from CDC and one from Socrata can
  be correlated by timestamp to understand what source changes drove what catalog
  changes.
- **Classification propagation**: Column classifications computed on SQL Server
  (via histogram analysis and extended properties) attach to DuckLake columns,
  traveling with schema evolution.
- **Drift detection**: Comparing DuckLake column snapshots across time reveals
  schema drift — added/dropped/retyped columns — across all sources uniformly.
- **Reproducible analysis**: Any downstream query can be re-run against the
  catalog state as of any prior snapshot, because the full history is preserved
  with source-authoritative timestamps.

## Implementation Status (March 2026)

### Completed

1. **DuckLake catalog SA model** (`src/rule4/ducklake_catalog.py`): All 29
   DuckLake metadata tables as SQLAlchemy Table objects. `create_catalog(engine)`
   bootstraps the schema on any SA-supported dialect.

2. **DuckLake OOB writer** (`src/rule4/ducklake_writer.py`): Pure SA expression
   API writer for DuckLake metadata. Manages snapshot/catalog/file ID counters.
   Tested on all three DuckLake catalog backends (PostgreSQL, SQLite, DuckDB).

3. **TTST sync** (`src/rule4/temporal.py`): Dialect-independent transaction-time
   state table maintenance. JSON payload → CTE expansion → set-based close/insert.
   Custom `@compiles` elements (`JsonSource`, `JsonField`, `NullSafeNE`) handle
   the four dialect-specific bits. Tested on PostgreSQL, SQLite, DuckDB, and
   SQL Server.

4. **Schema registry** (`src/rule4/catalog.py`, `src/rule4/type_map.yaml`):
   Universal type mapping (15 Socrata types → 5 dialect targets). 10,000 Socrata
   datasets tested across DuckDB, SQLite, PostgreSQL — 100% success.

5. **Socrata metadata registry** (`sql/ducklake/experiment_pg/`): PG TTST tables
   for domains, resources, and resource columns. Incremental scraper with
   watermark-based polling. Stored procedure for TTST interning.

6. **Multi-dialect replica sync** (`sql/ducklake/experiment_pg/replica_sync.py`):
   Same SA MetaData drives table creation and data loading across SQLite, PG,
   DuckDB. High-water marks via SA expression API.

7. **SQL Server local instance**: Azure SQL Edge on Docker Desktop (ARM64 native).
   OPENJSON + JSON_VALUE for the JSON-to-CTE pattern. Full TTST tested.

8. **Provenance capture design** (`doc/provenance_capture.md`): Connection pool
   preamble that injects OpenTelemetry trace context into database sessions via
   dialect-specific mechanisms (`sp_set_session_context` on SQL Server, custom
   GUC variables on PostgreSQL, `SET VARIABLE` on DuckDB, application-defined
   functions on SQLite). Provenance triggers read session context and write
   trace IDs into a log table, linking DuckLake snapshots to distributed
   traces. Temporal semi-join on process accounting for actor identification.

9. **Design notes** (`doc/`): "Scalar Functions, JSON Tunneling, and Rule 4"
   — the JSON-as-scalar-envelope pattern and its relationship to Schueler's
   log-as-database insight. "Logs All the Way Down" — the structural analogy
   between git's DAG, CDC shared LSNs, and DuckLake snapshot timelines.

### Next Steps

1. **Provenance event listener**: Implement the SQLAlchemy connection pool
   preamble designed in `doc/provenance_capture.md`. Test on all four dialects.
2. **Provenance trigger codegen**: Generate session-context-reading triggers
   for SQL Server and PostgreSQL from catalog metadata.
3. **CDC adapter**: DuckDB SQL that reads CDC change tables via ODBC and produces
   DuckLake snapshots.
4. **Classification bridge**: Sync `RULE4.extended_property` values into DuckLake
   column metadata.
5. **FTS indexes** on the PG Socrata catalog (resource names, descriptions,
   column descriptions).
6. **Vector embeddings**: Hierarchical path embeddings on the catalog for
   semantic similarity search.
