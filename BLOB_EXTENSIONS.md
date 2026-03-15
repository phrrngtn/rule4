# The BLOB Extension Family

Five C/C++ libraries that follow a common pattern: a core library with a
narrow C API, compiled into thin wrappers for SQLite, DuckDB, and Python.
No shared library, no IPC — each consumer statically links the core.

> **On naming:** The "blob" prefix is a misnomer — these extensions deal
> almost exclusively with text (JSON, YAML, SQL, JMESPath expressions,
> HTTP responses), making them CLOBs (Character Large Objects) by the
> strict database definition. But "blob" is punchier and the `blob*`
> prefix has stuck.

**blobapi** is a companion Python project that uses these extensions to
build an OpenAPI catalog with metadata-driven API adapters.

All four use CMake with FetchContent for dependencies, C++17, and optional
build flags (`BUILD_SQLITE_EXTENSION`, `BUILD_DUCKDB_EXTENSION`,
`BUILD_PYTHON_BINDINGS`).

Source: <https://github.com/phrrngtn>


## Loading the extensions

All four extensions build to the same three targets. The build system
uses CMake flags to select which bindings to produce:

```
cmake -DBUILD_SQLITE_EXTENSION=ON \
      -DBUILD_DUCKDB_EXTENSION=ON \
      -DBUILD_PYTHON_BINDINGS=ON \
      ..
```

### SQLite

```sql
.load /path/to/blobtemplates
.load /path/to/blobboxes
.load /path/to/blobfilters
.load /path/to/blobodbc
```

Requires a sqlite3 binary that supports extension loading (the macOS
system sqlite3 does not — use Homebrew's `sqlite3` or build from
source). The `.so`/`.dylib` suffix is optional in the `.load` command.

blobodbc additionally requires unixODBC (`brew install unixodbc` on
macOS) and an appropriate ODBC driver for your target database.

### DuckDB

Extensions must be unsigned (not distributed via the community
extension repository). After building, append metadata:

```bash
python3 duckdb_ext/append_metadata.py libext.so blobtemplates
mv libext.so blobtemplates.duckdb_extension
```

Then in DuckDB:

```sql
SET allow_unsigned_extensions = true;
LOAD '/path/to/blobtemplates.duckdb_extension';
LOAD '/path/to/blobboxes.duckdb_extension';
LOAD '/path/to/blobfilters.duckdb_extension';
LOAD '/path/to/blobodbc.duckdb_extension';
```

Or launch with the `-unsigned` flag: `duckdb -unsigned`.

### Python

Build with `BUILD_PYTHON_BINDINGS=ON`. The compiled extension module
(e.g. `blobtemplates_ext.cpython-314-darwin.so`) must be importable —
either install the package or place it on `sys.path`.

```python
import blobtemplates
import blobboxes
import blobfilters
import blobodbc
```

Tests run via uv: `uv run --python 3.14 --with pytest pytest python/tests/ -v`


## blobtemplates

**Jinja2-style template rendering and JSON processing.**

Wraps [Inja](https://github.com/pantor/inja) (C++ Jinja2 implementation)
and [jsoncons](https://github.com/danielaparker/jsoncons) behind a C API.
Process-wide LRU cache (1024 entries) for parsed templates; per-statement
auxdata caching in SQLite.

### Functions

| Function | Description |
|---|---|
| `template_render(tmpl, json)` | Render Inja template against JSON data |
| `template_render_with_options(tmpl, json, opts)` | Custom delimiters (e.g. for SQL that clashes with `{{ }}`) |
| `jmespath_search(json, expression)` | JMESPath query (jsoncons) |
| `json_from_diff(source, target)` | JSON Patch RFC 6902 diff (jsoncons) |
| `json_apply_patch(json, patch)` | Apply JSON Patch (jsoncons) |
| `json_diff(source, target)` | JSON Patch RFC 6902 diff (nlohmann) |
| `json_patch(source, patch)` | Apply JSON Patch (nlohmann) |
| `json_flatten(json)` | Nested JSON → flat object with JSON Pointer keys |
| `json_unflatten(json)` | Reverse of flatten |
| `yaml_to_json(yaml)` | YAML → JSON conversion (rapidyaml, C speed) |
| `zip_arrays(obj)` | `{a:[1,2], b:[3,4]}` → `[{a:1,b:3}, {a:2,b:4}]` (JMESPath custom function) |
| `unzip_arrays(arr)` | Inverse of zip_arrays (JMESPath custom function) |
| `to_entries(obj)` | `{k:v}` → `[{key:k, value:v}]` (JMESPath custom function) |
| `from_entries(arr)` | Inverse of to_entries (JMESPath custom function) |

### Dependencies

Inja v3.4.0, nlohmann/json v3.11.3, jsoncons v1.1.0, rapidyaml v0.11.0 —
all fetched at build time. Header-only except ryml (static lib).

### Typical use

- Generate dialect-specific SQL from catalog metadata via templates
- Reshape JSON (JMESPath) before feeding to templates
- Compute diffs between JSON snapshots (temporal change tracking)
- ODBC connection string templating with custom delimiters


## blobboxes

**Document content extraction into a normalized relational schema.**

Opens PDF, XLSX, DOCX, and plain text files and produces a fixed 5-table
schema regardless of format. Cursor-based streaming — no full
materialization required.

### Schema

| Table | Key columns |
|---|---|
| **doc** | document_id, source_type, filename, checksum (MD5), page_count |
| **pages** | page_id, document_id, page_number, width, height |
| **fonts** | font_id, font_name |
| **styles** | style_id, font_id, font_size, color, weight, italic, underline |
| **bboxes** | page_id, style_id, x, y, w, h, text, formula |

SQLite exposes these as virtual tables with a `file_path` hidden column.
DuckDB exposes them as table functions (`pdf_bboxes()`, `xlsx_bboxes()`,
etc.) and JSON scalar functions.

### Dependencies

PDFium (pre-built binaries), xlnt (XLSX), pugixml + miniz (DOCX),
nlohmann/json, hash-library (MD5).

### Typical use

- Extract tabular data from PDF/Excel attachments into the same SQL
  shape as structured datasets
- Feed bounding-box text into blobfilters for domain fingerprinting
- Join document content with catalog metadata for search/classification


## blobfilters

**Roaring bitmap fingerprinting for domain detection and column
classification.**

Hashes column values (FNV-1a → uint32) into roaring bitmaps. Probes
unknown columns against stored domain fingerprints using containment
and Jaccard similarity. Runs in microseconds per column, no ML, no
training data.

### Core operations

| Function | Description |
|---|---|
| `rfp_create()` / `rfp_free(bm)` | Bitmap lifecycle |
| `rfp_add_hash(bm, data, len)` | Add a value (FNV-1a hashed) |
| `rfp_add_json_array(bm, json, json_len)` | Build from JSON array of strings |
| `rfp_containment(probe, ref)` | \|probe ∩ ref\| / \|probe\| |
| `rfp_jaccard(a, b)` | \|A ∩ B\| / \|A ∪ B\| |
| `rfp_probe_json(symbols_json, json_len, refs, num_refs)` | Batch comparison against multiple domains, returns ranked JSON |
| `rfp_serialize(bm, buf, buf_len)` / `rfp_deserialize(buf, len)` | Binary serialization (portable roaring format) |
| `rfp_to_base64(bm, buf, buf_len)` / `rfp_from_base64(b64, len)` | Text-safe serialization (JSON-embeddable) |

### Histogram fingerprints

Combines bitmap membership with frequency distribution and shape metrics.
Accepts values from any source — full column scan, TABLESAMPLE,
`pg_stats` MCV lists, SQL Server `sys.dm_db_stats_histogram`, or
blobboxes cell extraction.

Shape metrics: cardinality ratio, repeatability (avg equal_rows),
discreteness (fraction of zero-range steps), range density.

### Dependencies

CRoaring (amalgamation, vendored), nlohmann/json.

### Typical use

- Fingerprint reference domains (US states, ZIP codes, agency names)
  once, then probe unknown columns in microseconds
- Classify columns as dimension vs measure using histogram shape metrics
- Detect shared domains across databases or data portals without
  reading data dictionaries


## blobodbc

**ODBC query execution as scalar functions returning JSON.**

Wraps [nanodbc](https://github.com/nanodbc/nanodbc) behind a C API.
Queries any ODBC-accessible database (SQL Server, PostgreSQL, MySQL,
Oracle, etc.) and returns results as JSON or as a single text value
(CLOB mode, useful for `FOR JSON PATH` on SQL Server).

### Functions

| Function | Description |
|---|---|
| `odbc_query(conn, sql)` | Execute query, return JSON array of objects |
| `odbc_clob(conn, sql)` | Execute query, return first column of first row |
| `odbc_query_named(conn, sql, bind_json)` | Named `:param` placeholders bound from JSON object |
| `odbc_clob_named(conn, sql, bind_json)` | Same for CLOB variant |

SQLite additionally supports variadic positional bind parameters:
`odbc_query(conn, sql, p1, p2, ...)`.

### Dependencies

nanodbc (fetched at build time), nlohmann/json, unixODBC (system).

### Why scalar functions, not table functions?

Returning JSON from a scalar function is more composable than a table
function: you can chain `jmespath_search(odbc_query(...), expr)` in a
single expression, nest it inside CTEs, pass it to `template_render()`,
or feed it to `json_each()` / `unnest()` when you do want rows. No TVF
restrictions, no special syntax, works in any position a scalar works.

For the primary use case — catalog metadata, stats histograms, extended
properties — result sets are small and high-structure. JSON is the right
intermediate form.

### Typical use

- Query SQL Server catalog views and extended properties from DuckDB
- Fetch `sys.dm_db_stats_histogram` for column classification
- Generate ODBC connection strings via `template_render()` then query
  through them in the same expression
- Bridge databases that DuckDB doesn't have a native scanner for


## blobhttp

**HTTP client as composable SQL scalar functions.**

Wraps [libcurl](https://curl.se/libcurl/) (via [cpr](https://github.com/libcpr/cpr))
behind scalar functions that return structured results (STRUCT in DuckDB,
JSON string in SQLite). Parallel execution via libcurl's multi interface.

### Functions

| DuckDB | SQLite | Description |
|---|---|---|
| `http_get(url, headers, params)` | `bhttp_get(url, headers, params)` | GET with query params |
| `http_post(url, headers, params, body)` | `bhttp_post(url, body, headers, params)` | POST |
| `http_request(method, url, ...)` | `bhttp_request(method, url, ...)` | Generic, all verbs |

Headers and params are JSON objects — composable via `json_object()`,
`json_merge_patch()`, and vault-derived values.

### Key features

- **Scoped configuration**: URL-prefix matching for per-service auth, rate limits, timeouts
- **Vault/OpenBao integration**: `vault_path` in config → automatic secret fetch and auth injection
- **GCRA rate limiting**: per-host and global, with 429 backoff and diagnostics
- **Parallel execution**: configurable `max_concurrent` per scalar function chunk
- **Auth**: SPNEGO/Kerberos, Bearer with expiry, mutual TLS, query param keys

### Dependencies

cpr (libcurl C++ wrapper), nlohmann/json — fetched at build time.

### Typical use

- Call web APIs from SQL with automatic auth and rate limiting
- Compose API calls in CTE chains (vault → geocode → weather → analysis)
- JMESPath-driven adapters reshape responses to canonical schemas
- Batch API calls driven by table data (blobhttp parallelizes automatically)


## How they compose

```
  Web APIs (weather, geocoding, etc.) Relational databases
       │                                    │
       │  blobhttp                          │  blobodbc (JSON scalars)
       │  (rate-limited, vault-backed auth) │  postgres_scanner / sqlite_scanner
       ▼                                    ▼  (bulk data)
  ┌──────────────────────────────────────────────┐
  │                   DuckDB                      │
  │                                               │
  │  blobtemplates: JMESPath reshape, SQL codegen │
  │  blobfilters:   domain fingerprints, classify │
  │  blobboxes:     PDF/XLSX/DOCX → relational    │
  │  blobodbc:      ODBC → JSON scalars           │
  │                                               │
  │  + built-in: json_each, unnest, read_parquet, │
  │    read_csv, Python dataframes, iceberg, ...  │
  └──────────────────────────────────────────────┘
       │
       ▼
  DuckLake catalog (PostgreSQL)  +  Data files (MinIO / local)
```

### Composition patterns

**Catalog-driven codegen**: Query catalog metadata (from any database or
API), reshape with `jmespath_search()`, feed to `template_render()` to
produce CREATE TABLE DDL, COPY commands, or sync procedures. The template
can vary by target dialect (SQL Server, PostgreSQL, DuckDB, SQLite)
while the metadata source is the same.

**Cross-source domain detection**: Fingerprint columns from one database
into blobfilters reference domains. Probe columns from other databases
against those fingerprints. High containment = same domain, different
name. Works across any combination of databases and data portals.

**Document-to-table pipeline**: Blobboxes extracts text with geometry
from documents into a relational schema. That text can be fed into
blobfilters for domain detection, or joined back to catalog metadata
for enrichment.

**Temporal diff tracking**: Use `json_diff()` or `json_from_diff()` to
compute RFC 6902 patches between successive snapshots of any JSON data.
Store patches compactly for provenance and auditing.

**Shape-based classification**: Blobfilters histogram fingerprints
capture shape metrics (cardinality ratio, repeatability, discreteness)
from any source — full table scans, TABLESAMPLE, `pg_stats` MCV lists,
SQL Server `sys.dm_db_stats_histogram`, or blobboxes cell extraction.
Combined with foreign key membership and data type from the catalog,
these classify columns as dimensions or measures without manual labeling.


## Data access: JSON scalars vs bulk scanners

The blob extensions use JSON as lingua franca for metadata — low
cardinality, high structure data where the cost of serialization is
negligible and the composability benefit is large. But this is explicitly
not the path for bulk data movement.

### When to use what

| Use case | Tool | Why |
|---|---|---|
| Catalog metadata, stats, extended properties | `blobodbc` / `blobhttp` | Small results, high structure, composable as JSON |
| Bulk table scans, data migration | `postgres_scanner`, `sqlite_scanner`, nanodbc TVF (eventually) | Streaming, columnar, no serialization overhead |
| Web API pagination | `blobhttp` via curl + proxy | Rate limiting, backpressure (see below) |

### The JSON tax

Returning an ODBC result set as a JSON string has a cost: every value
is serialized to text, wrapped in JSON syntax, and parsed again on the
consuming side. For a 50-row catalog query this is immeasurable. For a
million-row table scan it would be absurd. The blob extensions are
designed for the first case. DuckDB's native scanner extensions
(`postgres_scanner`, `sqlite_scanner`) and eventually the nanodbc
community extension handle the second.

### HTTP ingestion with backpressure

The `duckdb-http-enterprise` extension supports fetching data from web
APIs through a local proxy (e.g. mitmproxy, squid) that handles rate
limiting. The pattern:

1. Configure `blobhttp` with a proxy URL
2. The proxy enforces per-domain rate limits (e.g. 5 req/s for Socrata)
3. DuckDB's query engine naturally applies backpressure — when the
   consuming operator (INSERT, COPY) slows down, the HTTP fetch slows
   down. No explicit throttling code needed in the client.
4. Pagination is driven by SQL (a recursive CTE or a generate_series
   of offsets), not by application code

This means the entire ETL — paginate an API, parse the response,
transform, and load into DuckLake — can be a single SQL statement.


## After the Fact: temporal catalog reconstruction

"After the Fact" is the pattern of building a temporalized store from
sources that were not designed for temporal queries. The catalog is
reconstructed after the fact from whatever timestamps the source
provides, rather than being captured in real-time.

### Architecture

```
  ┌─────────────────────┐     ┌─────────────────────┐
  │  Catalog database    │     │  Data files          │
  │  (PostgreSQL)        │     │  (MinIO / local)     │
  │                      │     │                      │
  │  ducklake_snapshot   │     │  Parquet files with   │
  │  ducklake_table      │     │  field_id mapping    │
  │  ducklake_column     │     │                      │
  │  ducklake_data_file  │     │                      │
  └──────────┬───────────┘     └──────────┬───────────┘
             │                            │
             └──────────┬─────────────────┘
                        │
              ┌─────────▼──────────┐
              │  DuckDB            │
              │  (local or         │
              │   MotherDuck)      │
              │                    │
              │  ATTACH            │
              │  'ducklake:...'    │
              │  AS lake           │
              └────────────────────┘
```

### Out-of-band DuckLake population

DuckLake's metadata tables (`ducklake_snapshot`, `ducklake_column`,
`ducklake_data_file`, etc.) use a `begin_snapshot`/`end_snapshot`
valid-time pattern — this is Snodgrass temporal by another name. We
populate these tables directly via SQL INSERT rather than through
DuckLake's API, which allows us to:

- **Set source-authoritative timestamps**: `snapshot_time` is set to
  the source's event time (e.g. Socrata's `metadata_updated_at`, SQL
  Server's CDC `tran_end_time`), not wall clock. This faithfully
  reconstructs the source's causal history.

- **Register externally-produced Parquet files**: Data fetched via
  `blobhttp` or `blobodbc` is written to Parquet with explicit
  `field_id` mappings matching the DuckLake catalog's `column_id`.
  The file path, row count, and footer size are registered in
  `ducklake_data_file`.

- **Achieve time travel over non-temporal sources**: Once the catalog
  is populated, DuckLake's standard time travel queries work:
  `SELECT * FROM lake.table AT SNAPSHOT ...` reconstructs the table
  as it existed at any point in the source's history.

### Key implementation details

- **field_id is mandatory**: DuckLake maps Parquet columns via
  `field_id`, not column names. Use `COPY ... (FORMAT PARQUET,
  FIELD_IDS {col: id, ...})` to set them.

- **footer_size must be accurate**: DuckLake validates Parquet footer
  size. Extract from the file: last 4 bytes before `PAR1` magic.

- **One snapshot per operation**: DuckLake expects one DDL or DML
  operation per snapshot, recorded in `changes_made`.

- **Source-specific temporal fidelity varies**:
  - CDC (SQL Server): full history via LSN + `fn_cdc_map_lsn_to_time()`
  - Change Tracking: only current state + "changed since version N"
  - Socrata: `data_updated_at` gives event time, one version per update
  - All stored in the same catalog; consumers must understand provenance

### The universal query

With this architecture, a single DuckDB session can join across:

- **DuckLake temporal tables** (Parquet on MinIO, catalog on PostgreSQL)
- **Live relational databases** (via `postgres_scanner`, `sqlite_scanner`,
  or `blobodbc`)
- **Web APIs** (via `blobhttp`)
- **Document content** (via `blobboxes` — PDF, Excel, Word, plain text)
- **Python dataframes** (via DuckDB's native dataframe registration)
- **Local files** (CSV, Parquet, JSON via DuckDB built-ins)

All in one SQL statement. The blob extensions provide the glue for
the metadata-intensive parts (reshaping, fingerprinting, codegen);
DuckDB's native capabilities handle the data-intensive parts.
