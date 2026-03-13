# Logs All the Way Down

> "The database is not the database — the log is the database, and the database
> is just an optimized access path to the most recent version of the log."
>
> — B.-M. Schueler, "Update Reconsidered" (1977); see
> [Scalar Functions, JSON Tunneling, and Rule 4](scalar_json_pattern.md) for
> the full provenance of this quote.

## The Observation

Schueler's line is the project's load-bearing idea — the
[scalar JSON pattern doc](scalar_json_pattern.md) explains how it motivates
the "After the Fact" architecture, where we reconstruct logs from sources that
have already destroyed theirs.

But the observation recurses. We are using git — a log-structured store — to
record our work on a system whose central insight is that the log is the
database. The git commit history is itself a temporal record of the project's
evolution, and `git checkout` is time travel in exactly the sense that
DuckLake's `AS OF` is time travel.

## Git as a Tree of Logs

Git is not a single log. It is a DAG — a tree with many logs, where each
branch represents an independent timeline and each merge commit records a
causal unification of two timelines into one.

This parallels the CDC adapter design. In SQL Server CDC, a shared LSN across
multiple tables means that changes to those tables happened in the same
transaction — a single causal point spanning multiple entities. That is
structurally the same as a git merge: independent change streams unified at a
commit that records both parents.

## The Analogy Extends

| Git concept | Rule4 concept |
|---|---|
| Commit | DuckLake snapshot |
| Working tree | "The database" — optimized access path to HEAD |
| `git log` | `SELECT * FROM ducklake_snapshot ORDER BY snapshot_id` |
| `git checkout <sha>` | `AS OF` time travel |
| Merge commit | CDC snapshot with shared LSN across tables |
| Branch | Independent source change stream (Socrata, CDC, CT) |
| `git blame` | Provenance in `ducklake_snapshot_changes.commit_extra_info` |

Whether this analogy is deep or merely structural is worth revisiting once the
CDC adapter is working and we can see whether the merge semantics actually
compose the same way.

## Schueler's "Optimized Access Path" — Plural

Schueler wrote about *the* optimized access path — singular, the most recent
version. But the project generalizes this. DuckLake snapshots, TTST tables,
dialect-specific replicas, and git working trees are all optimized access paths
to different points in the same underlying log. The log may be reconstructed
(CDC change streams stitched back together), lossy (Change Tracking's
polling-gap sketch), or faithfully preserved (git's reflog), but in every case
Schueler's insight holds: the access paths are derived, the log is primary.

Git makes this viscerally concrete. `git reflog` is a log of logs — a
transaction-time record of every HEAD movement, including rebase rewrites and
reset operations that the commit DAG itself forgets. Even git's garbage
collector, which prunes unreachable objects, is an instance of the general
problem: a source destroying its own history, leaving downstream consumers
(in this case, `git fsck`) to reconstruct what they can from whatever
references survive.

It is, as the title says, logs all the way down.


## What Real Transaction Logs Look Like

If the log is the database, what does the log actually look like inside the
major engines? The answer reveals a spectrum from purely physical (opaque
page images) to purely logical (self-describing tuple records), and this
spectrum maps directly onto how much catalog metadata you need to reconstruct
meaning from the log.

### The spectrum

| Engine | Unit of logging | Envelope | Payload | Self-describing? |
|---|---|---|---|---|
| SQLite | Whole page image | 24-byte frame header (page number, commit size, salt, checksum) | Raw page bytes (entire 4K/8K page) | No — you cannot tell a B-tree interior node from a leaf from an overflow page without consulting `sqlite_master` |
| PostgreSQL | Block reference + tuple bytes | 24-byte `XLogRecord` (`xl_tot_len`, `xl_xid`, `xl_prev`, `xl_info`, `xl_rmid`, `xl_crc`) | Block headers + tuple data as raw `HeapTuple` bytes | Envelope yes (resource manager dispatch via `xl_rmid`, ~20 types). Tuple payload no — needs `pg_catalog` to decode column boundaries |
| SQL Server | Row within page ("physiological") | 10-byte LSN (VLF sequence : block offset : slot) + `LOP_*` operation + `LCX_*` context | Row slot ID + byte-range before/after images (`RowLog Contents 0-4`) | Operation types yes (~100 `LOP_*` codes). Row bytes no — needs `sys.columns` to decode |
| Oracle | Block-level change vectors | 24-68 byte redo record header + 28-byte change vector header (DBA, SCN, layer.opcode) | Element arrays within change vectors; ~150+ opcodes across ~25 layers (e.g., layer 11 = table/row ops) | Structure yes (layer.opcode dispatch). Column values no — needs data dictionary; LogMiner joins redo with dictionary |
| DuckDB | Logical tuples + catalog DDL | `[SIZE (uint64)][CHECKSUM][WALType enum][DATA...]` | Full catalog definitions for DDL; typed vector data for DML | **Yes** — `CREATE_TABLE` WAL entries carry column names, types, constraints; `INSERT_TUPLE` carries typed data. Replayable without external catalog |

The term of art for the PostgreSQL and SQL Server approach is **physiological**
— "physical to a page, logical within a page." The log record identifies
*where* by page number (physical) and *what* by tuple slot or byte offset
(logical within the page). But the tuple bytes themselves are opaque without
the catalog.

### What the engines share

Every engine's log has a fixed-size **envelope** (header) that is
self-describing: you can parse record boundaries, identify the operation type,
and chain records by LSN/SCN/sequence without understanding the payload. This
is the log's equivalent of a network packet header — it supports routing
(recovery, replication, archival) without understanding content.

Every engine *except DuckDB* stores the payload as raw bytes whose
interpretation requires an external catalog. The log is not a relation — it is
a byte stream with typed headers and opaque bodies.

### DuckDB as the outlier

DuckDB's WAL is the only one that is **fully self-describing**. A `CREATE_TABLE`
WAL entry precedes the first `INSERT_TUPLE` for that table, so the WAL itself
carries the schema needed to interpret subsequent data entries. You can replay
a DuckDB WAL from scratch without any external catalog — the catalog *is in
the log*.

This is exactly Schueler's formulation made literal: the log is the database,
and DuckDB's WAL is a log from which you can reconstruct the entire database
including its catalog. Every other engine's log requires the catalog as a
side-channel — which is fine for crash recovery (the catalog is on disk) but
inadequate for the kind of cross-system reconstruction that "After the Fact"
requires.


## JSON as a Synthesized Log Record

The [scalar JSON pattern](scalar_json_pattern.md) describes how blobodbc's
query results tunnel structured data through scalar functions. But viewed
through the lens of log formats, what blobodbc produces is something more
specific: a **synthesized transaction log entry** for a system that never
exposed its real log.

The `{meta, header, body}` representation proposed in that document maps
directly onto log record structure:

| Log record component | blobodbc JSON equivalent |
|---|---|
| Fixed envelope (LSN, timestamp, operation type) | `meta` — query text, execution timestamp, elapsed time, row count |
| Schema reference (table ID, column IDs) | `header` — column names, ordinal positions, ODBC type codes, size, scale |
| Payload (tuple data) | `body` — array of arrays, positional values, no repeated keys |

The critical property: the `header` makes the record **self-describing**, like
DuckDB's WAL and unlike every other engine's. A consumer can interpret the
`body` without consulting an external catalog because the `header` carries
everything needed to reconstruct a typed table — column names, types, and
ordinal positions. This is what DuckDB's `CREATE_TABLE` WAL entry does for
`INSERT_TUPLE`, and it is what the JSON header does for the JSON body.

### Enriching the envelope

The `meta` envelope can carry far more than query text and timing. For "After
the Fact" reconstructed log entries, the envelope should capture the full
provenance chain:

```json
{
  "meta": {
    "query":          "SELECT ... FROM sys.dm_db_stats_histogram ...",
    "source":         "rule4_test.dbo.SalesOrderDetail",
    "source_system":  "sql2025.localhost:1433",
    "target":         "ducklake:rule4_socrata",
    "driver":         "ODBC Driver 18 for SQL Server",
    "principal":      "rule4",
    "otel_trace_id":  "4bf92f3577b34da6a3ce929d0e0e4736",
    "otel_span_id":   "00f067aa0ba902b7",
    "executed_at":    "2026-03-13T14:22:07.123Z",
    "elapsed_ms":     12,
    "row_count":      47,
    "column_count":   6
  },
  "header": [ ... ],
  "body":   [ ... ]
}
```

The provenance fields — `source_system`, `driver`, `principal`,
`otel_trace_id` — are the database equivalent of IP packet headers: they
describe the path the data traveled, not the data itself. The `query` field
is the log's operation descriptor; `source` identifies the object being read;
`executed_at` is the LSN/SCN equivalent.

### What should NOT go in the envelope

Not everything about a log entry belongs in the entry itself. This is where
a lesson from network architecture applies.

Floyd, Jacobson, McCanne, Liu, and Zhang's SRM paper ("A Reliable Multicast
Framework for Light-weight Sessions and Application Level Framing," SIGCOMM
1995 / IEEE/ACM ToN 1997) discovered a fundamental tension: SRM named data
at the application layer (Application Data Units with semantic identifiers)
but the network only routed by address. When a receiver needed a missing
named data item, neither the network nor other hosts knew where the nearest
copy resided. Recovery requests had to be flooded to the entire multicast
group because the network could not route toward data by name.

Van Jacobson later resolved this with Named Data Networking (CCN/NDN, CoNEXT
2009): make data names the primary network-layer primitive, so the network
itself can route toward cached copies of named content. The one-liner:
**"content has a name, not a location."**

The principle that separates SRM from NDN — and that applies to our log
records — is: **the identity of a data item must be intrinsic to the data,
not derived from the transport path that delivered it.** Concretely:

**Belongs in the envelope (intrinsic identity):**
- Content hash or deterministic identifier of the payload
- Source object identity (schema-qualified table name, API endpoint)
- Semantic timestamp (the source's event time, not wall clock)
- Schema version or header fingerprint

**Belongs in a separate provenance record (transport metadata):**
- Connection string, driver, DSN name
- Network address of the source system
- Credentials / principal used
- OTEL trace/span IDs for this specific execution
- Wall-clock execution time, latency, retry count

The reason: the same logical data (same table, same snapshot, same rows) might
be obtained via different transport paths — a direct ODBC connection, a
replicated copy, a cached Parquet file, an API response. If the transport
metadata is embedded in the log entry's identity, then two copies of the same
data obtained via different paths appear as different entries. This is the SRM
problem restated: confusing *where you got the data* with *what the data is*.

The provenance record — who fetched it, how, when, through what driver —
should be a separate relation that references the log entry by its
content-derived identity. This is the same separation that git achieves:
a commit's identity (SHA) is derived from its content (tree, parent, message),
not from the remote you fetched it from. `git fetch origin` and `git fetch
backup` produce the same commit objects if the content is the same.

### The two-table design

This suggests a log record design with two relations:

1. **Entry** (content-addressed): `{entry_id, source_object, source_event_time,
   header_hash, body_hash, header, body}` — identity derived from content,
   stable across transport paths.

2. **Provenance** (transport-addressed): `{provenance_id, entry_id,
   connection_string, driver, principal, otel_trace_id, otel_span_id,
   wall_clock_time, elapsed_ms, retry_count}` — one row per *acquisition*
   of an entry, potentially many per entry.

This is the same separation already present in DuckLake's design:
`ducklake_snapshot` (content: what changed) vs
`ducklake_snapshot_changes.commit_extra_info` (provenance: who did it, from
where). And it is the separation that the
[provenance capture doc](provenance_capture.md) implements for SQL Server
triggers — one provenance record per statement, capturing session context and
OTEL correlation, linked to the data changes by transaction ID.

The log entry's identity should be like a git SHA or an NDN data name:
determined by what it contains, not by how it arrived.


## Why It Has to Be Logs

It has to be logs all the way down because time's arrow has only one direction,
and the log is the only data structure that respects this as a first-class
constraint. Tables, indexes, materialized views, caches — all are timeless
projections that discard ordering. You can reconstruct a table from its log
but you cannot reconstruct the log from a table, for the same reason you
cannot unstir cream from coffee. The log is isomorphic to the causal history
of the system, and causal history is what time's arrow produces. Every UPDATE
that overwrites a value is an act of forgetting — entropy reduction achieved
by discarding the record of what was there before. Schueler's "optimized
access path" is an optimization that trades temporal fidelity for spatial
efficiency. The log grows because time moves forward, and it can only grow,
because time does not move back. "After the Fact" is the work of recovering the arrow of time from systems
that deliberately erased it. We are, of course, trying to unstir the cream
from the coffee.
