# Rule4

Codd's Rule 4: the database description is represented at the logical level in
the same way as ordinary data, so authorized users can apply the same relational
language to its interrogation as they apply to regular data.

Rule4 builds a **temporal schema registry** for heterogeneous data sources.
Schema metadata, column definitions, and their evolution over time are all
ordinary tables — queryable, versionable, and composable with standard SQL.

## Architecture

The system maintains temporalized replicas of remote, heterogeneous catalogs
using two complementary mechanisms:

1. **Transaction-Time State Tables (TTST)** — user-visible temporal tables with
   `tt_start`/`tt_end` columns, maintained via set-based close/insert from JSON
   payloads. Dialect-independent: works on PostgreSQL, SQLite, DuckDB, and SQL
   Server.

2. **DuckLake Out-of-Band (OOB) metadata** — direct population of DuckLake's
   `ducklake_*` metadata tables with source-authoritative timestamps, bypassing
   the DuckLake API. Consumers attach read-only and get time-travel, PIT
   reconstruction, and schema evolution for free via DuckLake's `AS OF` syntax.

Both mechanisms share the same pattern: replay a source's causal history as
transaction-time snapshots, preserving whatever temporal fidelity the source
provides (full CDC history, poll-based change tracking, or event timestamps).

A third mechanism — **provenance capture** — injects OpenTelemetry trace
context into database sessions at connection checkout time, making distributed
trace IDs available to triggers that record *who* or *what* made each change.
Session metadata (PID, login time, client address) enables temporal semi-joins
against process accounting data to identify the actor behind the trail.

See [`sql/ducklake/README.md`](sql/ducklake/README.md) for the full design
document and [`doc/`](doc/) for design notes on JSON tunneling, provenance
capture, and the log-as-database insight.

## Python Modules (`src/rule4/`)

All modules use the SQLAlchemy expression API — no f-string SQL, no dialect
branching in application code.

| Module | Purpose |
|---|---|
| `temporal.py` | TTST sync: JSON → CTE → set-based close/insert. Custom `@compiles` elements (`JsonSource`, `JsonField`, `NullSafeNE`) handle the four dialect-specific bits. |
| `ducklake_catalog.py` | SA `Table` definitions for all 29 DuckLake metadata tables. `create_catalog(engine)` bootstraps on any SA-supported backend. |
| `ducklake_writer.py` | OOB writer for DuckLake metadata. Manages snapshot/catalog/file ID counters. Tested on PostgreSQL, SQLite, DuckDB. |
| `catalog.py` | Universal schema registry with type mapping from `type_map.yaml`. 15 Socrata types → 5 dialect targets. 10,000 datasets tested at 100% success. |

## Source Adapters

| Source | Temporal fidelity | Status |
|---|---|---|
| **Socrata Open Data** | Event time (`data_updated_at`), one version per update | Working: TTST + DuckLake OOB |
| **SQL Server CDC** | Full history via LSN, transaction boundaries | TTST tested, DuckLake adapter planned |
| **SQL Server Change Tracking** | "Changed since version N", poll-time only | TTST tested |

## Local Development

```bash
# Install dependencies (uses uv)
uv sync

# Run tests
uv run python /tmp/test_ducklake_writer.py   # DuckLake OOB across 3 backends
uv run python /tmp/test_temporal.py           # TTST across 4 dialects
```

### Prerequisites

- **PostgreSQL** with mTLS client cert auth for `rule4` user (see
  `~/.rule4/pg-certs/README.md`)
- **SQL Server** via Azure SQL Edge on Docker Desktop (ARM64 native)
- **DuckDB** (via `duckdb-engine` SA dialect)
- **SQLite** (built-in)

## Legacy SQLite Implementation

The original SQLite implementation (`sql/socrata/`, `sql/schema/`) used
[sqlean](https://github.com/nalgeon/sqlean) with community extensions:

- [sqlite-http](https://github.com/asg017/sqlite-http) — HTTP access
- [sqlite-template-inja](https://github.com/phrrngtn/sqlite-template-inja) —
  Inja templating for codegen
- [sqlite-embedded-odbc](https://github.com/phrrngtn/sqlite-embedded-odbc) —
  ODBC catalog scraping

This layer continues to serve as a staging area for Socrata change detection.
The DuckLake sync process reads from these tables and translates detected
changes into DuckLake snapshots.

## Related Projects

- [`phrrngtn/duckdb-http-enterprise`](https://github.com/phrrngtn/duckdb-http-enterprise) — DuckDB HTTP client extension
- [`phrrngtn/sqlite-embedded-odbc`](https://github.com/phrrngtn/sqlite-embedded-odbc) — SQLite ODBC extension
- [`phrrngtn/sqlite-jsoncons`](https://github.com/phrrngtn/sqlite-jsoncons) — SQLite jsoncons extension