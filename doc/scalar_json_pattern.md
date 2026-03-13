# Scalar Functions, JSON Tunneling, and Rule 4

> "The database is not the database — the log is the database, and the database
> is just an optimized access path to the most recent version of the log."
>
> — B.-M. Schueler, "Update Reconsidered" (1977), as quoted in Date, Darwen,
> and Lorentzos, *Temporal Data and the Relational Model* (Morgan Kaufmann,
> 2003)

Schueler's observation, buried in the proceedings of a 1977 IFIP working
conference and largely forgotten, was rediscovered independently by Pat Helland
in 2015 ("The truth is the log. The database is a cache of a subset of the
log.") and became a foundational idea for event sourcing, Kafka, and the
immutability movement. But Schueler's formulation predates Helland's by 38 years — the earliest
statement of the idea we have found — and Date et al. recognized it by placing
it in the front matter of their temporal database book — where it
was, for at least one reader, the most valuable thing in the volume.

Rule4's work is a practical elaboration of Schueler's insight. The sources we
catalog — Socrata open-data APIs, SQL Server databases with CDC or Change
Tracking, PostgreSQL catalogs — have already destroyed their logs, or never kept
them, or expose only a lossy sketch of what changed. We reconstruct what we can
of the log from whatever temporal signal the source provides, and then
materialize that reconstructed log into multiple optimized access paths: TTST
tables, DuckLake snapshots, dialect-specific replicas. Each is Schueler's
"optimized access path" generalized to support not just the most recent version
but *any* version the log can reconstruct.

## The Pattern

Rule4 makes heavy use of scalar functions that perform side-effects or
computation traditionally associated with separate services: ODBC queries
against remote databases, HTTP requests to APIs, Inja template expansion,
JMESPath reshaping of JSON documents. Because these are scalar functions, they
can appear anywhere a SQL expression can — in SELECT lists, WHERE clauses, JOIN
conditions, CTEs, or as arguments to other scalar functions.

This is unremarkable on its own. What makes it interesting is the combination
with JSON as a tunneling mechanism for structured data through the scalar
interface.

## JSON as a Scalar Envelope

SQL scalar functions accept and return single values. A relation — rows and
columns — is not a single value. But a JSON array of objects is. By packing
tabular data into JSON (via `JSON_GROUP_ARRAY(JSON_OBJECT(...))` in SQLite,
`FOR JSON PATH` in SQL Server, `to_json()` in PostgreSQL, or `to_json()` in
DuckDB), we can pass entire result sets through scalar channels.

This creates a pipeline where each stage is a SQL expression:

1. **Derive a connection string** from catalog metadata (a scalar function over
   dimension tables that hold server names, credentials, database names).
2. **Derive a query** from catalog metadata (template expansion over column
   definitions, using Inja or string concatenation).
3. **Execute the query** against a remote source via an ODBC or HTTP scalar
   function, receiving the result as a JSON document.
4. **Reshape the result** with JMESPath — project, filter, flatten, rename —
   without leaving SQL.
5. **Unpack the JSON** into rows via `json_each()`, `OPENJSON`, or
   `jsonb_array_elements()`, typically inside a CTE.

Each of these steps is composable. The output of one can be the input to
another. The entire pipeline is a single SQL statement — no intermediate
materialization, no application-layer orchestration, no imperative control flow.

## The Bind Parameter Trick

The same tunneling works in reverse. When a SQL statement needs complex,
structured input — say, a batch of records to upsert — the caller packs them
into a JSON array and passes it as a single bind parameter. The statement's
CTEs unpack the JSON into rows using the dialect's JSON table-valued function
(`json_each`, `OPENJSON`, `jsonb_array_elements`, `unnest(from_json(...))`).

This is what `temporal.py`'s TTST sync does. The caller provides a JSON payload
and a list of column names. The `@compiles`-driven `JsonSource` and `JsonField`
elements generate the correct unpacking CTE for each dialect. The rest of the
statement — the CLOSE update and the INSERT of new/changed rows — is pure
SQLAlchemy expression API, dialect-independent.

The practical consequence: any language that can serialize JSON and call
`conn.execute(stmt, {"payload": json_string})` can drive the sync. No ORM, no
model classes, no code generation at call time. The CTE *is* the code
generation, and it happens once at statement construction.

## What This Violates

This pattern is a deliberate violation of first normal form. A JSON document
inside a VARCHAR column is a relation masquerading as a scalar. Codd would not
approve. The relational model requires that every attribute value be atomic;
JSON arrays of objects are anything but.

It also strains the boundaries of Codd's Rule 7 (high-level insert, update, and
delete). When a scalar function performs an ODBC query inside a SELECT, the
side-effect is invisible to the optimizer, opaque to the transaction manager,
and impossible to roll back. The function looks like a value but behaves like a
statement.

## What This Enables

The violation is in service of Rule 4 itself. The catalog metadata that
describes remote systems — server addresses, schema definitions, column types,
API endpoints, update timestamps — is stored in ordinary tables and queried
with ordinary SQL. The scalar-function-plus-JSON pattern is the mechanism that
*closes the loop*: the metadata is not just queryable but actionable. A query
over the catalog can *produce* the queries that fetch data from the systems the
catalog describes.

Without this pattern, the metadata is inert. You can look at it, but acting on
it requires application code that reads the metadata, constructs strings,
dispatches calls, and parses responses. With this pattern, the application code
collapses to a single SQL statement (or a small number of them), and the
orchestration logic lives in CTEs and scalar expressions that the database
engine evaluates.

The result is that Rule 4 extends beyond *description*. The catalog does not
merely describe the database in relational terms — it describes the process of
acquiring, transforming, and maintaining the database in relational terms. The
sync procedures, the type mappings, the connection parameters, the temporal
bookkeeping — all of it is data, all of it is queryable, and all of it composes
via the same relational operations that Codd intended for ordinary data.

## Precedent and Limitations

This is not entirely new. SQL Server's linked servers and `OPENROWSET`,
Oracle's database links, and PostgreSQL's `dblink` and foreign data wrappers
all allow queries to span system boundaries from within SQL. What differs here
is the emphasis on *composability through JSON* rather than through the
database engine's native federation mechanisms, and the use of *scalar
functions* (which compose freely in expressions) rather than *table-valued
functions* (which are restricted to FROM clauses).

The limitations are real. JSON tunneling has serialization overhead. Scalar
functions that perform I/O are invisible to the query planner — there is no
cost model, no predicate pushdown, no parallelism. Error handling is crude:
a failed HTTP request or ODBC call surfaces as a SQL error, with limited
diagnostic context. And the pattern depends on every participating database
engine having reasonable JSON support, which was not a safe assumption before
roughly 2016 but is now nearly universal.

The trade-off is acceptable for metadata-scale workloads. Catalog tables have
thousands of rows, not millions. Schema definitions change infrequently.
The bottleneck is network latency to remote sources, not JSON parse time. For
the data itself — the actual rows of the datasets being cataloged — conventional
bulk-load mechanisms (Parquet files, COPY, BCP) remain appropriate. The scalar
JSON pattern governs the *control plane*, not the *data plane*.

## Result Representations: List-of-Dicts vs Header+Body

blobodbc's `odbc_query` currently returns a JSON array of objects (list-of-dicts):

    [{"session_id": 75, "login_name": "rule4"}, ...]

This is convenient but redundant — column names repeat on every row. For a
10,000-row, 20-column result set, that is ~200KB of repeated key strings.

The alternative is a **relational representation** — the format that relational
theory greybeards would recognize as a relation's heading and body:

    {
      "meta":   {"query": "...", "executed_at": "...", "elapsed_ms": 12,
                 "row_count": 2, "column_count": 6},
      "header": [{"name": "session_id", "ordinal": 0, "type_name": "int",
                  "sql_type": 4, "size": 10, "scale": 0}, ...],
      "body":   [[75, "rule4", ...], [76, "app_user", ...]]
    }

The header carries everything needed to reconstruct a typed table in any
consumer: column names, ordinal positions, driver-reported type names, ODBC SQL
type codes, size, and decimal scale. The body is a list of lists — positional
values, no repeated keys.

### Design Decision: Always Produce the Rich Form Internally

blobodbc should always produce the `{meta, header, body}` representation
internally. The list-of-dicts format is a trivial expansion of this (zip header
names with each body row). The choice of output format can be:

- A different function name (`odbc_query` → list-of-dicts, `odbc_relation` →
  header+body), or
- A JMESPath expression that transforms the internal representation before
  returning it.

This avoids maintaining two independent code paths. The rich form is the single
source of truth; the list-of-dicts form is a presentation convenience.

### Consumer-Side CTE Generation

Given a deterministic header (same columns, same types for a given query), a
consumer can generate a fixed CTE that reads body arrays positionally with
typed casts:

    SELECT
        CAST(j->>0 AS INT)          AS session_id,
        CAST(j->>1 AS VARCHAR(128)) AS login_name,
        CAST(j->>2 AS VARCHAR(128)) AS service_name
    FROM (SELECT unnest(cast(body AS JSON[])) AS j FROM ...)

The CTE is generated once from the header and reused. The body flows as data,
never as SQL text. No VALUES-clause materialization, no batch compilation cost
scaling with row count.

The header's `type_name` and `sql_type` fields are join keys into
`registry.type_mapping` in PostgreSQL, which provides canonical type mappings
across dialects. This enables automatic generation of correctly-typed CTE
projections for any target dialect.

### JMESPath Reshaping at the Query Execution Level

blobodbc includes jsoncons as an internal dependency (header-only, not exposed
via any public function). This allows an optional JMESPath expression to reshape
the JSON result before it crosses the FFI boundary back to the calling database.

This is motivated by catalog-scraping use cases where different source dialects
produce different result-set shapes (different column names, different column
orders) but we want to conform them to a common schema. The JMESPath expression
knows about source and destination columns, and the reshaping happens inside
blobodbc — no serialize/deserialize round-trip through the database layer.

jsoncons is **not** exposed as UDF functions from blobodbc. JMESPath, JSON
diff/patch, and flatten/unflatten are exposed as database functions from
blobtemplates, which is the designated JSON toolkit extension. blobodbc is an
ODBC client that happens to use jsoncons internally; blobtemplates is the
extension that makes jsoncons capabilities available to SQL.

See Lemire et al. on JSON parsing performance — the serialization cost is
dominated by ODBC network latency for metadata-scale workloads, but keeping the
reshape close to the data source is a matter of expressiveness and composability,
not just performance.
