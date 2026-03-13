# Provenance Capture: From Cloud Chambers to Connection Pools

> "The database is not the database — the log is the database, and the database
> is just an optimized access path to the most recent version of the log."
>
> — B.-M. Schueler, "Update Reconsidered" (1977)

## The Cloud Chamber Analogy

In early twentieth-century physics, cloud chambers made subatomic particles
visible. You could not observe the particle itself, but you could observe its
interaction with the environment — a trail of ionized gas whose curvature,
density, and length revealed the particle's mass, charge, and momentum.

The situation in databases is analogous. The data in a table and its
trigger-maintained temporal backlog are the trail. The "particles" are the
database sessions — each belonging to some OS process, which belongs to some
process tree, which may belong to an application server, a cron job, an ETL
pipeline, or a human typing at a terminal. We can see the interaction of the
particle with the data, but we know nothing of its nature or provenance.

CDC gives us the trail (every row version, with LSN ordering). Change Tracking
gives us a coarser sketch ("something changed since version N"). Temporal
backlogs give us before/after snapshots. But none of these mechanisms tell us
*who* or *what* made the change. The trail is there; the particle is invisible.

## Building a Detector

A cloud chamber becomes a particle detector when you add instrumentation: a
magnetic field to curve the trails (revealing charge-to-mass ratio), multiple
chamber layers to track trajectory, timing circuits to measure velocity. Each
layer resolves more about the particle's identity.

Our detector has analogous layers:

1. **Session metadata** — the database engine already knows something about each
   connection: login name, client hostname, program name, client interface. On
   SQL Server, `sys.dm_exec_sessions` exposes these. On PostgreSQL,
   `pg_stat_activity` does the same. This is the coarsest layer — it tells us
   "a process called MyApp.exe on host BUILDSERVER connected as login etl_svc."

2. **Application-injected context** — if the application cooperates, it can
   inject structured identity into the session before executing DML. This is
   the key mechanism this document designs. The application says "I am span
   `00f067aa0ba902b7` of trace `4bf92f3577b34da6a3ce929d0e0e4736`, running as
   service `rule4-sync`, on behalf of user `jane@example.com`."

3. **External enrichment** — after the fact (no pun intended), we can join the
   captured trace IDs against the OpenTelemetry trace store, OS process
   accounting, application server logs, or job scheduler records. The session's
   `program_name` is "python3" — but the trace ID links it to a specific
   Airflow DAG run, a specific Kubernetes pod, a specific git commit of the
   sync code.

Each layer is optional. A system with only layer 1 still captures useful
provenance. A system with all three gives you distributed trace lineage from
the user's browser click through the application server, through the database
trigger, into the DuckLake snapshot.

## OpenTelemetry Trace Context

OpenTelemetry (OTEL) defines a standard trace context that propagates across
service boundaries. The relevant fields for database provenance:

| Field | Size | Purpose |
|---|---|---|
| `trace_id` | 16 bytes (32 hex chars) | Globally unique identifier for an end-to-end request |
| `span_id` | 8 bytes (16 hex chars) | Identifies one unit of work within a trace |
| `trace_flags` | 1 byte (2 hex chars) | Sampling flag (`01` = sampled) |
| `tracestate` | String, max 32 entries | Vendor-specific key=value pairs |

The W3C `traceparent` header encodes these as a fixed 55-character string:

```
00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
^^                                  ^^^^^^^^^^^^^^^^  ^^
version         trace_id               span_id      flags
```

OTEL also defines **Baggage** — application-level key-value pairs (user_id,
tenant_id, request_id) that propagate alongside trace context but are
semantically separate. Trace context identifies *where* in the trace you are;
baggage carries *who/what/why*.

For our purposes, we want to propagate into the database session:

- `traceparent` (the full 55-char string — easy to parse, carries everything)
- `service_name` (which service/process is making the change)
- Optionally, baggage values like `user_id` or `job_id`

## Session Context Mechanisms by Dialect

Each database engine provides a different mechanism for injecting
application-level metadata into a session where it can be read by triggers or
audit functions.

### SQL Server: `sp_set_session_context`

Introduced in SQL Server 2016. A typed key-value store scoped to the session.

```sql
EXEC sp_set_session_context @key = N'traceparent',
    @value = '00-4bf92f35...-01', @read_only = 1;
```

- Keys: `sysname` (max 128 chars). Values: `sql_variant` (max 8,000 bytes).
- Total capacity: 1 MB across all keys.
- `@read_only = 1` locks the value for the session — prevents overwriting
  deeper in the call stack. Cleared on pool reset (`sp_reset_connection`), so
  the next checkout gets a clean slate. **Caveat**: SA connection pools may
  reuse a physical connection without triggering `sp_reset_connection`, so
  `read_only` keys from a prior checkout will block the next injection. In
  practice, omit `@read_only` and rely on the checkout preamble to overwrite
  values on every use.
- **Readable from triggers** via `SESSION_CONTEXT(N'key')` — this is the
  critical feature.
- **Pool behavior**: `sp_reset_connection` clears all session context. The
  preamble must be re-executed on every connection checkout.
- **Known bug** (unfixed as of March 2026): `SESSION_CONTEXT()` returns
  incorrect results under parallel execution plans. Workaround: trace flag
  11042, or `OPTION (MAXDOP 1)`. Trigger execution is typically serial, so
  this mainly affects RLS predicates on large scans.

### PostgreSQL: Custom GUC Variables

PostgreSQL allows arbitrary dotted-prefix variables set via `SET` or
`set_config()` and read via `current_setting()`.

```sql
SET LOCAL otel.traceparent = '00-4bf92f35...-01';
-- or equivalently:
SELECT set_config('otel.traceparent', '00-4bf92f35...-01', true);
```

- Values are untyped (always text).
- `SET LOCAL` (or `set_config(..., true)`) is **transaction-scoped** — reverts
  automatically at COMMIT/ROLLBACK. This is the only safe mode with connection
  poolers like PgBouncer in transaction-pooling mode.
- `SET` (session-scoped) persists across transactions but leaks to the next
  pool consumer unless `DISCARD ALL` / `RESET ALL` is executed on return.
- **Readable from trigger functions** via `current_setting('otel.traceparent',
  true)` (the `true` = `missing_ok`, returns NULL if never set).
- **Pool behavior**: `SET LOCAL` is inherently safe — no cleanup needed.
  PgBouncer's `track_extra_parameters` can cache session-scoped GUCs per
  client, but `SET LOCAL` is simpler and more portable.

### DuckDB: `SET VARIABLE` / `getvariable()`

DuckDB has a first-class session variable system.

```sql
SET VARIABLE traceparent = '00-4bf92f35...-01';
SELECT getvariable('traceparent');
```

- Variables can be any DuckDB type, including `MAP` and `STRUCT`.
- Session-scoped — persists until the connection closes.
- `getvariable('nonexistent')` returns NULL (no error).
- **DuckDB has no triggers.** The provenance capture must happen in the
  application layer — the sync process reads `getvariable()` and writes it
  into `commit_extra_info` as part of the DML that creates DuckLake snapshots.
- This is architecturally fine for our use case: the DuckLake OOB writer
  (`ducklake_writer.py`) already controls all writes and can inject provenance
  at the application level.

### SQLite: Application-Defined Functions

SQLite has no built-in session context store, but it has a mechanism that
achieves the same result: `sqlite3_create_function()` with a user-data pointer
that closes over connection-scoped state.

```python
session_ctx = {}

def session_get(key):
    return session_ctx.get(key)

conn.create_function("session_get", 1, session_get)

# Set context
session_ctx["traceparent"] = "00-4bf92f35...-01"

# Now any trigger can call session_get('traceparent')
```

The function's implementation reads from a Python dict (or C struct) that lives
in the host process, scoped to the connection. Triggers can call this function
**provided** it is not registered with `SQLITE_DIRECTONLY`. In Python's
`sqlite3` module, functions registered via `create_function()` are callable from
schema-stored triggers by default.

An alternative is a TEMP table + TEMP trigger pattern:

```sql
CREATE TEMP TABLE _session_context (key TEXT PRIMARY KEY, value TEXT);
INSERT INTO _session_context VALUES ('traceparent', '00-4bf92f35...-01');
```

TEMP triggers on the target table can read from `temp._session_context`.
Persistent (schema-stored) triggers cannot reference TEMP tables — SQLite
explicitly prevents this.

For the application-defined function approach, the function must be registered
on every new connection. This is a natural fit for a SQLAlchemy connection pool
event.

## The Connection Pool Preamble

The design goal: a SQLAlchemy event listener that fires on every connection
checkout and injects the current OTEL trace context into the session using the
appropriate dialect mechanism. The listener must:

1. Detect the dialect (SQL Server, PostgreSQL, DuckDB, SQLite).
2. Extract the current OTEL trace context from the active span (if any).
3. Execute the dialect-specific injection statement(s).
4. Be idempotent — safe to call multiple times on the same connection.

### SQLAlchemy Pool Events

SQLAlchemy provides two relevant pool events:

- **`checkout`** — fired when a connection is retrieved from the pool. Receives
  `(dbapi_connection, connection_record, connection_proxy)`. This is the right
  hook for session context injection.
- **`checkin`** — fired when a connection is returned to the pool. Could be used
  for cleanup, but `SET LOCAL` (PG) and `sp_reset_connection` (SQL Server)
  handle cleanup automatically.

For SQLite, we also need the **`connect`** event (fired when a new raw
connection is created) to register the `session_get` function — this only needs
to happen once per physical connection, not on every checkout.

### Trace Context Source

The preamble reads trace context from the OTEL API:

```python
from opentelemetry import trace, baggage

span = trace.get_current_span()
ctx = span.get_span_context()
traceparent = f"00-{ctx.trace_id:032x}-{ctx.span_id:016x}-{ctx.trace_flags:02x}"
```

If no active span exists (OTEL not configured, or called outside a traced
context), the preamble should set context values to NULL rather than skip the
injection entirely — this ensures triggers have a consistent interface and can
distinguish "no trace context available" from "context not injected."

### What Gets Injected

| Key | Value | Purpose |
|---|---|---|
| `traceparent` | W3C traceparent string (55 chars) | Full trace context in standard format |
| `service.name` | OTEL resource attribute | Which service made the change |
| `user.id` | OTEL baggage or application-supplied | Application-level user identity |

The `traceparent` string is sufficient for trace correlation. The additional
keys provide denormalized context that avoids requiring a round-trip to the
trace store for basic provenance queries ("who changed this table?" can be
answered from `service.name` + `user.id` alone).

### Dialect-Specific Injection

**SQL Server:**
```sql
EXEC sp_set_session_context @key = N'traceparent', @value = :tp, @read_only = 1;
EXEC sp_set_session_context @key = N'service.name', @value = :svc, @read_only = 1;
```

**PostgreSQL:**
```sql
SET LOCAL otel.traceparent = :tp;
SET LOCAL otel.service_name = :svc;
```

(Note: PG custom GUCs require dotted prefix and cannot contain dots in the
value portion of the key name after the prefix. We use `otel.traceparent`,
`otel.service_name`.)

**DuckDB:**
```sql
SET VARIABLE traceparent = :tp;
SET VARIABLE service_name = :svc;
```

**SQLite:**
```python
# On connect (once per physical connection):
conn.create_function("session_get", 1, lambda k: session_ctx.get(k))

# On checkout (every logical checkout):
session_ctx["traceparent"] = tp
session_ctx["service.name"] = svc
```

## Provenance Triggers

With session context available, a provenance trigger captures it alongside the
DML metadata. The trigger writes one row per statement (not per row) into a
provenance log table.

On SQL Server, the trigger reads:

```sql
INSERT INTO _provenance_log (
    event_time, table_name, operation,
    traceparent, service_name,
    login_name, host_name, program_name
)
SELECT
    SYSUTCDATETIME(),
    'my_table',
    CASE WHEN EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted)
         THEN 'UPDATE'
         WHEN EXISTS (SELECT 1 FROM inserted) THEN 'INSERT'
         ELSE 'DELETE' END,
    CAST(SESSION_CONTEXT(N'traceparent') AS VARCHAR(55)),
    CAST(SESSION_CONTEXT(N'service.name') AS VARCHAR(128)),
    ORIGINAL_LOGIN(),
    HOST_NAME(),
    PROGRAM_NAME();
```

On PostgreSQL, the trigger function reads:

```sql
INSERT INTO _provenance_log (
    event_time, table_name, operation,
    traceparent, service_name,
    login_name, client_addr
)
VALUES (
    clock_timestamp(),
    TG_TABLE_NAME,
    TG_OP,
    current_setting('otel.traceparent', true),
    current_setting('otel.service_name', true),
    session_user::text,
    inet_client_addr()::text
);
```

On SQLite (using the application-defined function):

```sql
INSERT INTO _provenance_log (
    event_time, table_name, operation,
    traceparent, service_name
)
VALUES (
    strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
    'my_table',
    'INSERT',  -- SQLite triggers are per-operation, not dynamic
    session_get('traceparent'),
    session_get('service.name')
);
```

On DuckDB, there are no triggers. The application layer (e.g.,
`ducklake_writer.py`) reads `getvariable('traceparent')` and includes it in
the `commit_extra_info` JSON when creating snapshots.

## Session Metadata Available from SQL Server DMVs

The provenance trigger captures OTEL trace context from session context, but
SQL Server also exposes rich session metadata through DMVs that should be
captured alongside:

From `sys.dm_exec_sessions` (own-session visible without VIEW SERVER STATE):

| Column | What it tells us |
|---|---|
| `login_name` | Current effective login |
| `original_login_name` | Login before any `EXECUTE AS` |
| `host_name` | Client machine name (client-supplied) |
| `host_process_id` | Client-side OS process ID |
| `program_name` | Client application name (client-supplied) |
| `client_interface_name` | Client library family: ODBC, JDBC, .Net SqlClient, OLEDB |
| `login_time` | When the session was established |

From `CONNECTIONPROPERTY()` (built-in scalar function, no permissions needed):

| Property | What it tells us |
|---|---|
| `client_net_address` | Client IP address |
| `local_tcp_port` | Server-side TCP port |
| `net_transport` | Physical transport: TCP, Shared memory, Named pipe |
| `auth_scheme` | Authentication: NTLM, Kerberos, SQL |

These two sources together provide all the session metadata the provenance
trigger needs. `CONNECTIONPROPERTY()` returns connection-level properties
regardless of `EXECUTE AS` context changes, always reflecting the original
physical connection.

The combination of `host_name`, `host_process_id`, and `login_time` is the key
to the temporal enrichment join described below.

### Permissions: DMVs and CONNECTIONPROPERTY

The key permission question: can a low-privilege trigger read session metadata
without `VIEW SERVER STATE`?

The answer depends on which DMV:

| Source | Own-session without VIEW SERVER STATE? |
|---|---|
| `sys.dm_exec_sessions WHERE session_id = @@SPID` | **Yes** — "Everyone can see their own session information" (Microsoft docs) |
| `sys.dm_exec_connections WHERE session_id = @@SPID` | **No** — requires VIEW SERVER STATE, returns zero rows otherwise |
| `CONNECTIONPROPERTY('client_net_address')` | **Yes** — built-in scalar function, no permissions needed |
| `CONNECTIONPROPERTY('net_transport')` | **Yes** — same |
| `CONNECTIONPROPERTY('auth_scheme')` | **Yes** — same |

The combination of `sys.dm_exec_sessions` + `CONNECTIONPROPERTY()` gives us
everything we need without any special grants:

- From `dm_exec_sessions`: `login_name`, `original_login_name`, `host_name`,
  `host_process_id`, `program_name`, `login_time`
- From `CONNECTIONPROPERTY()`: `client_net_address`, `local_tcp_port`,
  `net_transport`, `auth_scheme`

No `VIEW SERVER STATE`, no `TRUSTWORTHY`, no `EXECUTE AS OWNER`, no deprecated
`sysprocesses`. All modern, all documented, all self-session-scoped.

```sql
-- Inside trigger: accessible to any user for their own session
SELECT
    s.login_name, s.original_login_name,
    s.host_name, s.host_process_id,
    s.program_name, s.login_time,
    CONNECTIONPROPERTY('client_net_address') AS client_net_address,
    CONNECTIONPROPERTY('local_tcp_port')     AS local_tcp_port,
    CONNECTIONPROPERTY('net_transport')       AS net_transport,
    CONNECTIONPROPERTY('auth_scheme')         AS auth_scheme
FROM sys.dm_exec_sessions AS s
WHERE s.session_id = @@SPID;
```

`CONNECTIONPROPERTY()` returns connection-level properties regardless of
`EXECUTE AS` context changes — it always reflects the original physical
connection, making it particularly suitable for provenance.

This is the mirror image of the extended properties pattern
(`sql/extended_properties.sql`) where ownership chaining is deliberately
**broken** (via `break_ownership_user`) to enforce per-row security trimming.
Here we avoid the chaining question entirely by using a DMV that grants
self-visibility and built-in functions that require no permissions.

## From Trails to Particle Identification

The provenance log gives us trails annotated with trace IDs and session
metadata. The enrichment phase joins these against external telemetry:

1. **OTEL trace store** (Jaeger, Tempo, Honeycomb, etc.) — given a trace_id,
   retrieve the full span tree. The database span is one leaf; its ancestors
   reveal the HTTP handler, the queue consumer, the cron trigger, the CI
   pipeline that initiated the change.

2. **Process accounting via temporal semi-join** — the `host_process_id` from
   `sys.dm_exec_sessions` is the PID of the client process on the remote host.
   The `login_time` tells us when that process connected. The process must have
   been created *before* `login_time` (modulo NTP skew). So the enrichment
   query is a **sequenced temporal join**: find the most recent process creation
   event for `host_process_id` on `host_name` where `process_create_time <
   login_time`.

   In Snodgrass terms, this is a temporal semi-join on the process accounting
   dataset:

   ```sql
   WITH PROCESS_MATCH AS (
       SELECT
           p.*,
           ROW_NUMBER() OVER (
               PARTITION BY p.host_name, p.pid
               ORDER BY p.create_time DESC
           ) AS rn
       FROM process_accounting AS p
       JOIN provenance_log AS prov
           ON p.host_name = prov.host_name
           AND p.pid = prov.host_process_id
           AND p.create_time < prov.login_time
   )
   SELECT * FROM PROCESS_MATCH WHERE rn = 1
   ```

   This resolves the PID to an actual process: its executable path, command
   line arguments, parent PID (and thus the full process tree), the user who
   launched it, and — if it is a containerized workload — the container ID and
   Kubernetes pod metadata.

   The "most recent creation before login" predicate handles PID reuse
   correctly: if PID 12345 was a cron job at 03:00 and a different process at
   14:00, and the database session logged in at 14:01, we match the 14:00
   creation, not the 03:00 one.

3. **Job scheduler metadata** — if the trace originated from an Airflow DAG,
   a cron job, or a CI pipeline, the trace baggage or service name identifies
   it. The enrichment phase can pull DAG run metadata, git commit SHAs, or
   pipeline configuration. Even without OTEL instrumentation, the process tree
   from step 2 often reveals the scheduler: the parent process of the client
   is `airflow worker`, `crond`, `gitlab-runner`, etc.

The result is a provenance record that answers not just "what changed" (the
CDC/CT/backlog trail) but "who changed it, from where, as part of what
operation, triggered by what event." The particle is identified.

## Classification of Actors

With enough enriched provenance records, patterns emerge:

- **Periodic batch** — regular cadence (hourly, daily), large change sets,
  consistent table ordering, service name matches a known ETL pipeline.
- **Interactive user** — irregular timing, small change sets, varied tables,
  traces originate from a web application or CLI tool.
- **Ad-hoc correction** — single-row updates at unusual hours, often from
  a direct SQL client (`program_name = 'Azure Data Studio'`), no trace context
  (manual session, no OTEL instrumentation).
- **Cascade effect** — a change to a dimension table triggers downstream
  updates. The trace tree shows a fan-out pattern; the root span identifies the
  initiating change.

This classification can be computed from the provenance log using the same
histogram and cardinality techniques that rule4 already applies to data columns
— periodicity is a histogram shape, affected-table-set is a cardinality
measure, time-of-day distribution is a density function.

## Relationship to DuckLake Provenance

The DuckLake OOB writer already accepts `commit_extra_info` as a JSON string
on `create_table()` and `register_data_file()`. The connection pool preamble
populates the trace context; the writer reads it and includes it in
`commit_extra_info`:

```json
{
    "source": "socrata",
    "dataset_id": "vfnx-vebw",
    "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    "service_name": "rule4-sync"
}
```

This connects DuckLake's snapshot timeline to the distributed trace. A query
like "what changed in the catalog between snapshots 42 and 57?" returns
snapshot_changes rows with trace IDs. Each trace ID links to a full span tree
in the OTEL backend, revealing the complete causal chain.

## Inferred Telemetry

Not all events in our system are directly instrumented. When the rule4 sync
process detects that a Socrata dataset was updated at `data_updated_at =
2024-06-15T14:32:00Z`, it is observing the trail of an event that happened
in a remote system — an event we were not present to instrument. The update
is real. The timestamp is real. But we have no span, no trace, no session
context from the source system. We are the cloud chamber, not the
accelerator.

The question is whether we can synthesize OTEL records for these inferred
events and feed them into the same telemetry pipeline that handles our
directly-instrumented operations.

### Why This Is Legitimate

OTEL's data model does not distinguish "observed" from "inferred" events.
A span is a struct with timestamps, attributes, and relationships. Nothing
in the spec requires that the process emitting a span was the process that
performed the work. Exporters, collectors, and backends accept spans and
logs from any source.

More importantly, this is the same operation we already perform with
DuckLake's `snapshot_time`. When we set `snapshot_time = data_updated_at`
instead of `NOW()`, we are creating a synthetic temporal record that says
"this event happened at time T in the source system." An OTEL record with
the same timestamp is the same fact expressed in a different vocabulary.

The source systems we catalog — Socrata APIs, SQL Server databases, PG
catalogs — have already destroyed their telemetry (if they ever had any).
We reconstruct what we can from the metadata they expose, just as we
reconstruct their logs from whatever temporal signal they provide.

### OTEL LogRecords, Not Spans

OTEL has three signal types: traces (spans), metrics, and **logs**. For
inferred events, LogRecords are the right choice over spans:

1. **LogRecords have two timestamp fields**: `timestamp` (when the event
   happened) and `observed_timestamp` (when we saw it). This is exactly the
   duality we need. A Socrata update detected during a sync would have:

   ```
   timestamp:          2024-06-15T14:32:00Z  (data_updated_at — source-authoritative)
   observed_timestamp: 2026-03-12T19:00:00Z  (when our sync process noticed)
   ```

   Spans have `start_time` and `end_time`, which represent duration, not the
   event/observation duality.

2. **LogRecords support trace correlation**. Every OTEL LogRecord can carry
   `trace_id` and `span_id` fields, linking it to a trace. So the inferred
   event can be correlated with the real sync trace that detected it:

   ```
   LogRecord:
     timestamp:     2024-06-15T14:32:00Z
     observed:      2026-03-12T19:00:00Z
     trace_id:      <sync trace>
     span_id:       <detect_change span>
     body:          "Socrata dataset vfnx-vebw updated"
     attributes:
       source:            socrata
       dataset_id:        vfnx-vebw
       rule4.inferred:    true
       rule4.fidelity:    socrata_event_time
   ```

3. **Log backends handle late-arriving data well.** Log storage systems
   (Loki, Elasticsearch, ClickHouse) are designed for high-cardinality
   historical ingestion. They don't assume temporal locality the way trace
   backends sometimes do.

### Stitching Inferred and Observed Events

The sync process produces both real spans (its own instrumented operations)
and inferred LogRecords (source events it detects). The `trace_id` links
them:

```
[real span]  rule4.sync.poll_socrata        (t=2026-03-12T19:00:00Z)
  ├─ [real span]  rule4.sync.detect_change  (t=2026-03-12T19:00:00Z)
  │    └─ [LogRecord] socrata.data_updated  (timestamp=2024-06-15T14:32:00Z,
  │                                          observed=2026-03-12T19:00:00Z,
  │                                          rule4.inferred=true)
  └─ [real span]  rule4.sync.create_snapshot (t=2026-03-12T19:00:01Z)
       └─ commit_extra_info: {"traceparent": "00-...", "source_event_time": "2024-06-15T14:32:00Z"}
```

The causal chain is explicit: this DuckLake snapshot exists because that
Socrata update happened. The time gap between the inferred event and the
observed detection is not a bug — it is the latency of our polling, honestly
represented.

### Fidelity Markers

Every inferred record must carry attributes that describe how it was inferred
and what confidence we have in its timestamps:

| Attribute | Values | Purpose |
|---|---|---|
| `rule4.inferred` | `true` | Distinguishes inferred from directly observed |
| `rule4.fidelity` | `cdc_lsn`, `ct_poll`, `socrata_event_time` | Temporal fidelity of the source |
| `rule4.source` | `socrata`, `sqlserver_cdc`, `sqlserver_ct` | Source system type |
| `rule4.source_id` | dataset ID, database name, etc. | Specific source instance |

Consumers of the telemetry data can filter on `rule4.inferred = true` to
separate directly observed operations from reconstructed history. The fidelity
marker tells them how much to trust the timestamp: CDC LSN-derived timestamps
are high-fidelity (sub-second, causally ordered); Socrata `data_updated_at` is
medium-fidelity (event time, but one version per update cycle); CT poll
timestamps are low-fidelity (we only know when we looked, not when it changed).

### The Broader Pattern

This is not specific to Socrata. Any source adapter that detects changes in a
remote system can emit inferred LogRecords:

- **CDC adapter**: inferred LogRecords with `timestamp` from
  `fn_cdc_map_lsn_to_time(lsn)`, `fidelity = cdc_lsn`. High confidence.
- **CT adapter**: inferred LogRecords with `timestamp` = poll time,
  `fidelity = ct_poll`. The record says "we observed a change at poll time"
  — honest about what we don't know.
- **Socrata adapter**: inferred LogRecords with `timestamp` =
  `data_updated_at`, `fidelity = socrata_event_time`. Medium confidence —
  source-authoritative but coarse.

The telemetry pipeline treats all of these uniformly. The fidelity markers
let downstream consumers (dashboards, alerts, enrichment queries) make
appropriate decisions about temporal precision.

This is the cloud chamber fully realized: every interaction between our
system and the sources it monitors leaves a trail — either directly
instrumented (real spans from our sync process) or inferred from the
evidence (LogRecords synthesized from source metadata). The provenance
log, the DuckLake snapshots, and the OTEL telemetry are three
materializations of the same reconstructed history: Schueler's log,
rendered as different optimized access paths for different consumers.

## Implementation Plan

1. **SQLAlchemy event listener** — `checkout` handler that detects dialect and
   injects trace context. `connect` handler for SQLite function registration.
   Pure SA, no raw SQL.

2. **Test harness** — verify context injection and readback on all four
   dialects. For SQL Server and PostgreSQL, verify trigger-based capture. For
   DuckDB, verify `getvariable()` readback. For SQLite, verify
   `session_get()` from a trigger.

3. **Provenance trigger codegen** — extend the existing codegen machinery to
   generate provenance-capture triggers for SQL Server and PostgreSQL.
   SQLite uses the application-defined function pattern. DuckDB uses
   application-layer injection.

4. **DuckLake writer integration** — modify `ducklake_writer.py` to
   automatically populate `commit_extra_info` with trace context from the
   session when available.

5. **Inferred LogRecord emitter** — source adapters emit OTEL LogRecords
   for detected remote events, with `timestamp` / `observed_timestamp`
   duality and fidelity markers. Linked to the sync trace via `trace_id`.

6. **Enrichment queries** — DuckDB SQL that joins the provenance log against
   OTEL trace data (exported as Parquet or queried via OTEL's gRPC/HTTP API)
   to produce enriched provenance records.

## Two-Table Provenance Design

The original single-table `session_provenance` conflated two granularities:
session lifecycle (WHO connected) and transaction identity (WHAT/WHEN). These
are now separated into two tables.

### session_provenance (WHO)

One row per connection lifecycle, keyed on `(session_id, login_time)`. The
`login_time` handles SPID reuse — SQL Server reuses session IDs across
connections, but `login_time` distinguishes them.

```sql
CREATE TABLE dbo.session_provenance (
    session_id            INT          NOT NULL,
    login_time            DATETIME     NOT NULL,
    traceparent           VARCHAR(55)  NULL,
    service_name          VARCHAR(128) NULL,
    user_id               VARCHAR(128) NULL,
    login_name            VARCHAR(128) NOT NULL,
    original_login_name   VARCHAR(128) NOT NULL,
    host_name             VARCHAR(128) NULL,
    host_process_id       INT          NULL,
    program_name          VARCHAR(128) NULL,
    client_interface_name VARCHAR(32)  NULL,
    client_net_address    VARCHAR(48)  NULL,
    local_tcp_port        INT          NULL,
    net_transport         VARCHAR(40)  NULL,
    auth_scheme           VARCHAR(40)  NULL,
    first_seen            DATETIME2    NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_session_provenance
        PRIMARY KEY CLUSTERED (session_id, login_time)
)
```

All columns are `VARCHAR`, not `NVARCHAR`. The values (login names, hostnames,
program names, transport names, auth schemes) are machine-generated
identifiers — pure ASCII in any realistic deployment. This avoids UCS-2
encoding for downstream consumers (DuckDB, Parquet, JSON) that work natively
in UTF-8.

### transaction_provenance (WHAT/WHEN)

One row per transaction, keyed on `(session_id, login_time, xact_id)`. Foreign
key to `session_provenance`. CT versions are per-commit (not per-statement),
so all changes within a single transaction share the same CT version and
`xact_id`.

```sql
CREATE TABLE dbo.transaction_provenance (
    session_id   INT      NOT NULL,
    login_time   DATETIME NOT NULL,
    xact_id      BIGINT   NOT NULL,
    ct_version   BIGINT   NULL,
    event_time   DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_transaction_provenance
        PRIMARY KEY CLUSTERED (session_id, login_time, xact_id),
    CONSTRAINT FK_transaction_session
        FOREIGN KEY (session_id, login_time)
        REFERENCES dbo.session_provenance (session_id, login_time)
)
```

### Trigger deduplication

The AFTER trigger performs two `IF NOT EXISTS` checks:

1. **Session**: insert into `session_provenance` only on first DML in this
   connection lifecycle.
2. **Transaction**: insert into `transaction_provenance` only on first DML in
   this transaction. A transaction touching multiple tables fires the trigger
   multiple times (once per table per statement type), but the `xact_id`
   dedup ensures one provenance row per commit.

### Join chain: CT changes → provenance

The join from Change Tracking changes to provenance goes through
`transaction_provenance`:

```sql
SELECT
    ct.vehicle_license_number,
    ct.SYS_CHANGE_VERSION,
    ct.SYS_CHANGE_OPERATION,
    s.login_name,
    s.service_name,
    s.user_id,
    s.client_net_address
FROM CHANGETABLE(CHANGES dbo.fhv_vehicles, 0) AS ct
LEFT JOIN dbo.transaction_provenance AS t
    ON ct.SYS_CHANGE_VERSION = t.ct_version + 1
LEFT JOIN dbo.session_provenance AS s
    ON t.session_id = s.session_id
    AND t.login_time = s.login_time
ORDER BY ct.SYS_CHANGE_VERSION
```

The `ct_version + 1` join works because `CHANGE_TRACKING_CURRENT_VERSION()`
called inside the trigger returns the version *before* the current transaction
commits. The change itself is assigned the next version.

On SQL Server editions that expose `sys.dm_tran_commit_table` (the modern
replacement for `sys.syscommittab`), a more robust join is available via
`xdes_id`:

```sql
FROM CHANGETABLE(CHANGES dbo.fhv_vehicles, 0) AS ct
JOIN sys.dm_tran_commit_table AS cmt
    ON ct.SYS_CHANGE_VERSION = cmt.commit_ts
LEFT JOIN dbo.transaction_provenance AS t
    ON cmt.xdes_id = t.xact_id
```

### CHANGE_TRACKING_CONTEXT for direct correlation

`SYS_CHANGE_CONTEXT` is a `VARBINARY(128)` stored with each CT change. The
application layer sets it before DML:

```sql
DECLARE @ctx VARBINARY(128) = CAST(@@SPID AS VARBINARY(128));
WITH CHANGE_TRACKING_CONTEXT(@ctx)
INSERT INTO dbo.fhv_vehicles ...
```

This provides an exact join key — no version arithmetic needed. However, it
requires application cooperation (each DML statement must be prefixed) and
**cannot be set inside a trigger** — the CT change is recorded as part of the
original DML, before the AFTER trigger fires. The horse has bolted.

## CDC vs CT: Intervals of Capture

CDC and CT differ fundamentally in what they preserve:

| Aspect | CDC | CT |
|---|---|---|
| Granularity | Every row version | Most recent change per PK |
| Row data | Full before/after images | None (join to base table) |
| Intermediate versions | Preserved (5 updates = 10 rows) | Lost (5 updates = 1 record) |
| Deletes | Full before-image of deleted row | PK only (row is gone) |
| Mechanism | Async log reader (SQL Agent) | Synchronous, in-engine |
| Overhead | Higher (transaction log scanning) | Lower (lightweight) |
| Watermark | LSN (`fn_cdc_map_lsn_to_time`) | Version number (monotonic) |
| Can coexist? | Yes — independent of CT | Yes — independent of CDC |

CDC's `__$operation` values: 1=DELETE, 2=INSERT, 3=UPDATE before, 4=UPDATE
after.

CT's `SYS_CHANGE_OPERATION`: I, U, D (PK only, no row data).

### PK mutation under CT

If a primary key value is updated, CT records it as a DELETE of the old PK and
an INSERT of the new PK — two separate, unlinked change records. CDC preserves
the before/after images in the same transaction (same `__$start_lsn`), so the
PK mutation is traceable. This is one more reason to never mutate PKs.

### Which maps to DuckLake?

DuckLake is an **after-image-only** system. It accumulates Parquet files; each
snapshot says "these files are visible now." There are no before-images, no
change operations — just row data.

CT's model maps perfectly:

1. Poll `CHANGETABLE(CHANGES ..., @last_version)` → changed PKs
2. JOIN to base table → current state (after-images)
3. Write as delta Parquet → new DuckLake snapshot
4. Merge-on-read: `ROW_NUMBER() OVER (PARTITION BY pk ORDER BY snapshot DESC)`

CDC is richer than DuckLake can natively represent. Options: collapse to
after-images (discarding intermediate history), or store the raw CDC stream as
a separate table preserving all `__$operation` values.

### Handling deletes

DuckLake has no native tombstone mechanism. For CT-detected deletes:

- **Soft-delete column**: add `_is_deleted BOOLEAN` to the delta Parquet. The
  merge-on-read query filters on `_is_deleted = false`. Rows are never
  physically absent, just marked. This is the recommended approach for
  incremental sync.
- **Periodic full snapshots**: write all current rows, not just deltas. Simple
  but defeats incrementality.

## End-to-End Pipeline: SQL Server → DuckLake

The full provenance flow, demonstrated on SQL Server 2025 Developer Edition
running under Rosetta 2 on Apple Silicon:

```
Python client (OTEL traceparent + app context)
  → SQLAlchemy pool preamble (sp_set_session_context)
    → SQL Server trigger (dm_exec_sessions + CONNECTIONPROPERTY)
      → session_provenance + transaction_provenance tables
        → DuckDB reads via odbc_query
          → Parquet files (data)
          → DuckLake catalog (metadata + commit_extra_info)
```

Three databases, three roles:

- **SQL Server 2025** — source of truth (CT/CDC + provenance triggers)
- **DuckDB** — query engine + Parquet writer (via `odbc_scanner` extension,
  works on ARM)
- **DuckLake catalog** (PostgreSQL or DuckDB) — temporal schema registry with
  provenance in `commit_extra_info` JSON

The `commit_extra_info` on each DuckLake snapshot carries the full provenance:

```json
{
    "source": {
        "server": "localhost:1433",
        "database": "rule4_test",
        "engine": "SQL Server 2025 (17.0.4006.2)"
    },
    "change_tracking": {
        "ct_versions_covered": [6, 7, 8],
        "transaction_ids": [45028, 45104, 45236]
    },
    "provenance": [
        {
            "xact_id": 45028,
            "ct_version": 6,
            "service_name": "socrata-sync",
            "user_id": "paul@rule4.dev",
            "client_net_address": "192.168.65.1",
            "auth_scheme": "SQL",
            "traceparent": null
        },
        {
            "xact_id": 45236,
            "ct_version": 8,
            "service_name": "frontend-api",
            "user_id": "jane@acme.com",
            "client_net_address": "192.168.65.1",
            "auth_scheme": "SQL",
            "traceparent": null
        }
    ]
}
```

The provenance traveled from a Python `set_app_context()` call, through
`sp_set_session_context`, into a SQL Server trigger reading
`SESSION_CONTEXT()` + `dm_exec_sessions` + `CONNECTIONPROPERTY()`, across ODBC
via DuckDB's `odbc_query`, and landed as JSON in a DuckLake snapshot metadata
record.

### Platform notes

- **SQL Server 2025 on ARM Mac**: No native ARM64 image exists. Use
  `mcr.microsoft.com/mssql/server:2025-CU1-ubuntu-22.04` under Rosetta 2
  (`--platform linux/amd64`). Works reliably; avoid special characters in
  `MSSQL_SA_PASSWORD` env var (shell escaping issues through Docker/Rosetta).
- **Azure SQL Edge**: Officially retired (September 2025). Was the only native
  ARM64 SQL Server image. Lacks `syscommittab`/`dm_tran_commit_table`, SQL
  Agent (needed for CDC capture jobs), and other features.
- **DuckDB ODBC on ARM**: The `nanodbc` community extension is not built for
  `osx_arm64`. Use `odbc_scanner` instead — same `odbc_query()` function,
  works on ARM.
- **SQLAlchemy + SQL Server 2025**: Produces a harmless warning
  `Unrecognized server version info '17.0.4006.2'` — SQLAlchemy doesn't
  know about version 17.x yet. Suppress with
  `warnings.filterwarnings("ignore", message="Unrecognized server version")`.
