# Temporal Source Taxonomy: Heterogeneous Capture into a DuckLake Time-Series

## Thesis

We can read from a range of **"temporal-ish" sources** — sources that expose, in their own
idiom, *what changed and roughly when* — and back-populate both the **schema** and the
**contents** of their tables into DuckLake via out-of-band (OOB) inline-MVCC insertion, after
which the tables are queryable with `AT (TIMESTAMP)` time-travel. The fidelity of that
time-travel is bounded by what each source actually records — hence a *taxonomy*, not a single
mechanism.

The design separates a generic **apply** path from a per-source **acquisition** seam:

```
heterogeneous source ──(acquisition driver)──▶ ColumnCollection.sync ──▶ HistoryReplica
                                                  (the seam)            (DuckLake inline MVCC)
                                                                              │
                                                                       AT (TIMESTAMP) ◀── query
```

Only the driver is source-specific. Everything downstream — stapling changes by
transaction-time, one DuckLake snapshot per distinct time, reconstruction — is shared.

## The seam and the driver protocol

`ColumnCollection.changed_since(conn, watermark, driver)` returns `(changes, next_watermark)`;
`ColumnCollection.sync(conn, watermark, driver, replica)` groups the changes by their
transaction-time (`__tt`) and applies each group as one snapshot. A **driver** is small:

- `query(cc, watermark, *, source_schema=None)` — a statement (SA Core `select()` *or* a
  `text()` for dialect-specific TVFs) that yields, per changed row: the type-aware column
  values, `__op` (I/U/D), `__key`, and `__tt` (the staple — the thing that orders/groups
  commits; need not be a wall-clock time).
- `next_watermark(rows, prev)` — how the high-water mark advances.
- `snapshot_time(tt)` — *optional*. Maps the staple to the datetime DuckLake stores. Default:
  coerce ISO/datetime. Only flavors whose staple is a logical version (CT) override it.

That's the entire extensibility surface. Adding a source = writing those one-to-three methods.

## The flavors (acquisition: contents → time-series)

| Flavor | Backend(s) | Staple (`__tt`) | Deletes | History fidelity | New columns auto? | Time axis | Status |
|---|---|---|---|---|---|---|---|
| **UserColumnDriver** | any SA source *(demoed: SQLite)* | user column (`:updated_at`) | tombstone col only | **net** — latest per key; updates between polls lost | yes (re-capture + live-table projection) | real (the user column) | built + demoed |
| **ChangeTrackingDriver** | **SQL Server only** | `SYS_CHANGE_VERSION` | yes (`SYS_CHANGE_OPERATION`) | **net, version-granular** — one snapshot per version per poll; same-row churn between polls collapsed | **yes** (CHANGETABLE ⋈ live base table) | **synthetic** (CT stores no commit time) | built + live vs gfe |
| **BacklogDriver** | any SA source *(demoed: SQLite triggers)* | backlog `ts` | yes (`op` col / tombstone) | **full** — every after-image; intermediate versions retained | **no** — log shape frozen by the trigger | real (the `ts`) | built + demoed |
| **CDCDriver** | SQL Server | `__$start_lsn` | yes (`__$operation`) | **full** — `fn_cdc_get_all_changes(...,'all')`, all intermediate versions | **no** — change tables frozen at enable; needs capture-instance rollover | **real** (`lsn_time_mapping`) | built; live needs sysadmin to enable CDC |

### Why after-images (all four store after-images, not before-images)

Reconstruction with after-images is a **forward, stateless selection** — state as-of T is
`WHERE begin <= T < end`, the after-image *is* the answer. Before-images would force a backward
fold from "current" (order-dependent, O(changes-since-T), fragile to a broken chain). DuckLake
inline MVCC stamps each after-image `(begin_snapshot, end_snapshot)`, so `HistoryReplica` *is*
Snodgrass's **backlog relation** and `AT (TIMESTAMP)` is literally `begin <= T < end`. See
Snodgrass, *Developing Time-Oriented Database Applications in SQL*.

## Per-flavor notes and gotchas

**Change Tracking (CT).** Lightweight, PK-keyed; stores no values, so the driver joins
`CHANGETABLE` to the live base table for the current after-image (and so **rides new columns
automatically**). The PK is sacred — altering/dropping it requires disabling CT and a consumer
re-baseline. `TRUNCATE` is blocked. Always check the watermark against
`CHANGE_TRACKING_MIN_VALID_VERSION` before trusting a poll; expired retention ⇒ full re-sync.
No commit time exists, so the timeline is **synthetic** (monotonic in version, not wall-clock).

**Change Data Capture (CDC).** Heavyweight, value-carrying; `fn_cdc_get_all_changes(...,'all')`
returns ops 2 (insert), 4 (after-update), 1 (delete) — full intermediate history (we drop the
3 = before-update image). The big gotcha: **change tables are frozen at
`sp_cdc_enable_table` time** — a new source column is *silently not captured*. Schema evolution
needs a **capture-instance rollover** (≤ 2 instances/table: enable a new instance, dual-run,
migrate consumers, drop the old); `cdc.ddl_history` ⋈ the `column_role` schema time-series is
the natural trigger for it. Enabling CDC on a database requires **sysadmin**
(`sp_cdc_enable_db`); enabling a table requires db_owner. **SQL Agent is not required for the
mechanism** — capture *is* the log scan `sys.sp_cdc_scan`, which can be invoked directly; Agent
only *schedules* that scan (the capture job) and runs the *cleanup* job.

**User-modeled transaction-time column (Socrata `:updated_at`).** The source carries its own
time column; the driver polls `WHERE tt > watermark ORDER BY tt`. Fidelity is only as good as
the source's discipline: it sees the *current* row per key, so updates between polls are
coalesced, and deletes are invisible without an explicit tombstone column. Assumes a monotonic
clock.

**Trigger-maintained backlog.** An append-only log of after-images (one row per change, with
`op` and `ts`) — the cleanest temporal primitive and the only **full-fidelity** flavor that
isn't SQL-Server-specific. Like CDC, the log's column set is frozen by the trigger, so schema
changes require updating the trigger + log table.

## The LSN → time question (and where ancillary triggers come in)

DuckLake snapshots need a wall-clock time; sources differ in whether they can supply one:

- **CDC has a built-in mapping.** `cdc.lsn_time_mapping` + `sys.fn_cdc_map_lsn_to_time(lsn)`
  give each LSN its transaction (commit) time — populated by CDC itself. The `CDCDriver` uses it
  directly for `__tt`; no custom plumbing.
- **CT has none.** Change Tracking records only versions, no times — which is exactly why the
  `ChangeTrackingDriver` synthesizes a monotonic timestamp from the version. To get *real*
  transaction times for a CT (or plain-table) source you record them yourself: an **ancillary
  trigger** writing the active transaction's begin time to a centralized side table — i.e. you
  build the backlog's `ts`. The transaction identity/time for such a trigger comes from the
  transaction DMVs — `CURRENT_TRANSACTION_ID()` joined to
  `sys.dm_tran_active_transactions.transaction_begin_time` (via
  `sys.dm_tran_session_transactions`). This is precisely the Snodgrass transaction-time-table
  pattern, and it is the same thing the `BacklogDriver` consumes.

So: CDC gives you the LSN→time mapping for free; for everything else, an ancillary
transaction-time trigger is how you manufacture the equivalent.

## Schema side (structure → registry → migration DDL)

The same `column_role` captures that drive *what* to replicate also form a **schema
time-series**. Four source dialects have `column_role` projections — **SQL Server, PostgreSQL,
SQLite, DuckDB** — so each can be captured, time-stamped into the DuckLake registry, and
diffed. `migration.py` interprets the changeset between two revisions (`schema_as_of(T_{n-1})`
vs `schema_as_of(T_n)`) as `ALTER TABLE` DDL, either direction (forward `n-1→n` or rollback
`n→n-3`), rendered per dialect. Validated live: capture a real table at two times, generate the
forward DDL, apply it to a fresh copy, re-capture — the copy's schema comes back identical.
*Limitation:* the registry currently keeps `data_type` as the base type name only, so DDL is
base-type-granular (length-faithful DDL needs the subset widened to carry
`max_length`/`precision`).

## What's built vs planned

- **Built + demoed:** the seam; UserColumn / ChangeTracking / Backlog drivers; `HistoryReplica`
  + `Replica` (inline MVCC); `column_role` capture + registry; `migration.py` (live-validated).
- **Built, live-validation pending:** `CDCDriver` — runs against a CDC-enabled table, but
  enabling CDC on the dev box needs a one-time sysadmin grant.
- **Planned:** CDC capture-instance rollover driven by `ddl_history` ⋈ schema time-series;
  payload **sampling** driven by the schema time-series (deciding *which* tables/rows to
  replicate — the "double application of DuckLake"); widening the registry type subset.

## Related

[[Asset Management as Data]] — the same schema-as-data + bitemporal machinery pointed at the
`{resources × principals × services}` product. The temporal substrate here is what makes
`access_as_of(T)` auditable.
