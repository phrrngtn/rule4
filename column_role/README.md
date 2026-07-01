# column_role

*A database is a thing with attributes.* One long, denormalized, **append-only** relation
in which every row is a member (a column or a parameter) playing a role in a grouping —
`table | view | tvf | proc | pk | uk | fk | index | parameter`. The schema (and the
procedural surface) reduced to columns-in-roles: Codd's Rule 4 taken to its conclusion.

- **[`docs/metamodel.md`](docs/metamodel.md)** — the relation, the `grouping_kind`
  taxonomy, the expression edge, and the per-dialect *common-denominator vs exceptions*
  table.
- **`sql/<dialect>.sql`** — the idiosyncratic catalog projections, one shape out:
  - `duckdb.sql`, `sqlite.sql` — **verified** against a live sample (columns, pk/uk, fk
    with referenced columns, index). Included not as CDC sources but to find the
    cross-dialect common denominator (and the exceptions).
  - `postgresql.sql`, `sqlserver.sql` — strawmen pending a server. SQL Server is the rich
    one that actually drives CDC/CT federation (`is_tracked_by_cdc`, change tracking,
    routine parameters).

The relation is the stable interface; the front-end is swappable — these dialect queries
today (set-based, federated via DuckDB's ODBC extension or run through SQLAlchemy), the
same rows feeding `create_table` (build), `pk` (diff/identity), `fk` (dimension signal).
Store it *in a DuckLake* and schema history is bitemporal for free.

## Related work

The immediate prompt for this design was running into a wall with
**[Dotmim.Sync](https://dotmimsync.readthedocs.io/)** — a capable .NET database-sync
framework — and the class of tools it represents (Microsoft Sync Framework, SQL Server
merge/transactional replication). They share an architecture that is a poor fit for
"render a database I can only *read* as a local, queryable copy":

- **They provision the source.** To track changes, Dotmim.Sync *creates* tracking tables,
  triggers, and stored procedures in the publisher's database (or, with the change-tracking
  provider, must still enable and provision SQL Server Change Tracking). The change-tracking
  state lives **in the source**. The practical consequence — and, for us, the **showstopper**
  — is that **a client needs write (DDL) permission on the publisher just to sync**. You
  cannot replicate a database you only have read access to, which is exactly the common case:
  a locked-down production catalog you may *read* and nothing more.
- **They own reconstruction.** These frameworks carry their own machinery for versioning
  rows, applying deltas, and resolving conflicts to converge two writable endpoints. That
  machinery is the bulk of the framework and most of its operational weight.
- **They are two-way.** Sync assumes both ends can be written and must be reconciled; that
  generality is what *forces* the intrusive, write-privileged tracking.

*(Log-based CDC — Debezium and kin — sits at a different point: non-intrusive on the tables
because it reads the transaction log, but it needs replication-level privileges and a
streaming pipeline, and it emits an **event stream** to be consumed rather than a queryable,
time-travelling **store**.)*

This project is organized on the opposite premises, and the differences are structural, not
incidental:

**1. The source is read-only; nothing is ever provisioned there.** No triggers, no tracking
tables, no scope rows, no DDL. We *ride whatever change-signal the source already exposes* —
SQL Server CDC or Change Tracking, a user-modeled `updated_at`, a trigger-maintained backlog
— and where there is none, fall back to a signal-free whole-row `EXCEPT` diff. **All control
and status — watermarks, last-sync, schema fingerprints — live in the replica itself or in a
third-party registry, never in the source.** (The replica organism ends by confirming the
source table still carries only its own columns.) This dissolves the showstopper outright:
read access is sufficient.

**2. Reconstruction is deferred to DuckLake.** We do not build a change-application or
bitemporal-reconstruction engine. We capture **after-images** and *append* them into a
DuckLake, then let its inline MVCC and `AT (VERSION => …)` / `AT (TIMESTAMP => …)`
time-travel do the temporal reconstruction — Snodgrass's reconstruction, performed by the
store, not by us. A driver's only job is to poll a source since a watermark and hand rows to
the append path. (Order by the logical clock, `VERSION`; `snapshot_time` interleaves
ingest-time and source data-time, so wall-clock time-travel is unreliable by construction.)

**3. The catalog is described as data, across dialects.** Rather than per-dialect
provisioning code, a config-as-data meta-schema (`catalog_source` / essences) *generates*
each dialect's catalog queries; the JOIN itself is relational metadata with the join columns
**inferred** from each catalog's identity-key convention (recorded explicitly only where the
catalog breaks it). Adding a dialect, or an essence — columns, indexes, keys, checks,
PostgreSQL's type subsystem, SQL Server's Service Broker — is **data entry, not code**.

**4. Reconstruction is deferred *because* the outputs are a spectrum.** The same captured
substrate yields different **derived products**, each at a different point on a cost/fidelity
curve:

  - an **append-only TTST replica** — cheapest: the `column` essence + type mapping + payload
    + change-detection is enough;
  - a **bitemporal schema-history** — free once the catalog is scraped into DuckLake;
  - **migration DDL** from the difference between two schema captures;
  - up to, asymptotically, a **SQL-Compare-grade clone** — the whole essence catalog plus DDL
    dependency ordering.

  Fidelity is a **dial**, not a fixed target: you pay only for the detail your chosen product
  actually requires. Dotmim.Sync gives you one product (a converged two-way replica) at one
  high provisioning cost; this gives you many, each priced to its own expressiveness.

**5. The cheapest product is *metadata-only*: a virtual database.** Because reconstruction lives
in the store, the DuckLake catalog *is* queryable metadata — table and column definitions, types,
snapshot intervals, the Parquet file locations — and DuckDB already federates heterogeneous
sources in place (`read_parquet`, the ODBC / nanodbc scanner, `ATTACH` of another DuckLake,
SQLite, or PostgreSQL). So you can **cons up a DuckDB facade over a pile of heterogeneous sources
purely by generating DDL from that metadata** — `CREATE VIEW … AS SELECT … FROM read_parquet(…)`,
`ATTACH …` — one schema out, nothing materialised. The facade is a thin metadata projection,
assembled by the same metadata-driven generation that produces the catalog queries. *"Give me a
DuckDB database presenting those Parquet exports, that ODBC source, and these replicated tables as
one schema"* becomes a **metadata-mostly operation**: read the catalog, emit the view/attach DDL,
done. It is the cheapest product to *construct* (nothing is copied) — its runtime cost and
expressiveness are whatever the underlying engines can push down and federate live. The heavy
lifting — scanning, coercion, time-travel — stays in the engine; we only describe.

The trade is explicit and worth stating plainly: we give up **bidirectional convergence**
(this is read-only replication, not sync) to gain **non-intrusiveness, read-only sourcing,
and store-deferred reconstruction**. For the problem that motivated it, that is the right
trade.
