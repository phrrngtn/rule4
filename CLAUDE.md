# CLAUDE.md

## Development Environment

- **Python**: Always use `uv run python`, never bare `python3`. The project uses uv for dependency management; bare python3 doesn't have sqlalchemy or other project deps installed.
- **DuckDB CLI**: Use `duckdb -unsigned` when loading locally-built extensions (blobodbc, blobtemplates, etc.) since they are not signed by MotherDuck.
- **DuckDB extension signing**: Locally-built DuckDB extensions (.so) need a metadata footer appended to produce a `.duckdb_extension` file. Each blob extension has `duckdb_ext/append_metadata.py` for this. The footer includes platform, version, ABI type, and a 256-byte signature placeholder. Without this footer, DuckDB won't recognize the file as an extension at all.
- **DuckDB Python**: Use `uv run python -c "import duckdb; ..."` (not bare python3). To load unsigned extensions: `conn.execute("SET allow_unsigned_extensions=true")` before `LOAD`.
- **PostgreSQL**: Always use `gssencmode=disable` on PG connection strings. Without it, macOS tries to contact a Kerberos KDC and hangs for ~3 minutes before falling back to password/cert auth. This applies to psql, pg_isready, SQLAlchemy, and any libpq client.
- **PostgreSQL binary**: `/opt/homebrew/opt/postgresql@17/bin/psql` (not on PATH)
- **PostgreSQL cert auth**: The `rule4` user authenticates via mTLS client certificate at `~/.rule4/pg-certs/` — no password. Connection string needs `sslmode=verify-full&sslcert=...&sslkey=...&sslrootcert=...&gssencmode=disable`. **Important**: psycopg2 does NOT expand `~` in paths — use `Path.home()` or absolute paths in Python code.

## Local Infrastructure

### PostgreSQL
- **Binary**: `/opt/homebrew/opt/postgresql@17/bin/`
- **Database**: `rule4_test` — the "high ceremony" metadata registry
- **Schemas**: `ducklake` (DuckLake catalog tables), `socrata` (metadata registry)
- **User**: `rule4` via mTLS cert auth (see Development Environment above)
- **DuckLake attach**: `ducklake:postgres:dbname=rule4_test host=localhost gssencmode=disable`
- **Data storage**: MinIO S3 at `localhost:9000`, bucket `ducklake-data`, creds `minioadmin`/`minioadmin`

### SQL Server 2025
- **Container**: `sql2025` — `mcr.microsoft.com/mssql/server:2025-CU1-ubuntu-22.04`
- **Docker run**: `docker run -d --name sql2025 --platform linux/amd64 -e ACCEPT_EULA=Y -e "MSSQL_SA_PASSWORD=Rule4TestPassword1" -p 1433:1433 mcr.microsoft.com/mssql/server:2025-CU1-ubuntu-22.04`
- **Database**: `rule4_test` (Change Tracking enabled)
- **SA account**: `sa` / `Rule4TestPassword1` — bootstrap only. Note: avoid special chars in SA password (Docker env var + Rosetta shell escaping issues).
- **App login**: `rule4` / `R4Developer!2024` — `db_owner` on `rule4_test`
- **Low-priv login**: `app_user` / `AppUser!2024` — DML only on data tables
- **ODBC connection**: `Driver={ODBC Driver 18 for SQL Server};Server=localhost,1433;Database=rule4_test;UID=rule4;PWD=R4Developer!2024;TrustServerCertificate=yes`
- **SQLAlchemy**: `mssql+pyodbc://rule4:R4Developer%212024@localhost:1433/rule4_test?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes` (note `%21` for `!`)
- **Setup script**: `/tmp/setup_sql2025.py` — creates database, logins, users, CT, tables, provenance triggers
- **Platform**: Runs under Rosetta 2 (`--platform linux/amd64`) on Apple Silicon. No native ARM64 SQL Server exists.

## Project Context

This project implements automated column classification (dimension vs measure) for database tables using statistics histograms and catalog metadata. The primary workflow is:

1. Extract histogram and catalog data from SQL Server (and PostgreSQL) via DuckDB's nanodbc ODBC extension
2. Compute classification features (cardinality ratio, repeatability, discreteness) in DuckDB
3. Write results back to SQL Server as extended properties via the `RULE4.extended_property` view

The target databases are SQL Server, PostgreSQL, DuckDB, and SQLite. Each has its own catalog query dialect — do not attempt to abstract across them.

## SQL Style Guidelines

- **CTE naming**: Use UPPER_CASE_SNAKE_CASE for CTE names (e.g., `SINGLE_COL_STATS`).
- **Table aliasing**: Always use explicit `AS` between a table/CTE reference and its alias (e.g., `FROM sys.stats AS s`, not `FROM sys.stats s`).
- **Prefer CTEs over correlated subqueries**: Factor out filtering logic into CTEs rather than using `NOT EXISTS` or correlated subqueries in `WHERE` clauses.
- **Prefer window functions over GROUP BY**: When a CTE needs to compute aggregates for filtering, use windowing functions (e.g., `COUNT(*) OVER (PARTITION BY ...)`) rather than `GROUP BY` / `HAVING`.
- **Prefer PIVOT over self-joins or conditional aggregation**: When pivoting rows to columns, use the `PIVOT` operator rather than multiple self-joins or `MAX(CASE WHEN ...)` patterns.
- **Output raw IDs for programmatic use**: When results will be consumed by automated processing, prefer raw identifiers (`object_id`, `stats_id`, `column_id`) over resolved names. Do not wrap IDs in helper functions like `OBJECT_NAME()` or `COL_NAME()` unless explicitly requested or needed for debugging.
- **No unnecessary ORDER BY**: Omit `ORDER BY` unless the consumer requires ordered output. When ordering is needed and the query runs via `odbc_query`, apply `ORDER BY` on the DuckDB side rather than the SQL Server side to avoid query plan regressions.
- **NVARCHAR(MAX) column positioning**: When querying SQL Server via ODBC, place `NVARCHAR(MAX)` and other LOB columns last in the SELECT list. The ODBC driver requires large object columns to be fetched after all fixed-width columns (`Invalid Descriptor Index` error otherwise).
- **SQL_VARIANT handling**: Use `SQL_VARIANT_PROPERTY(value, 'BaseType')` to inspect the underlying type of variant values. For binary/varbinary/image/timestamp base types, convert through `VARBINARY` with style 1 to produce hex strings safe for UTF-8 transit.
- **Always use prepared statements / bind parameters** — never dynamically construct SQL via string interpolation/f-strings. Use `conn.execute(sql, [param1, param2])` or equivalent.

## DuckDB via ODBC Conventions

- Queries to SQL Server are executed via `odbc_query('dsn_name', '...')`.
- Single quotes inside the SQL string must be escaped as `''` (DuckDB string escaping).
- DuckDB can resolve column names from `odbc_query` result sets at planning time — unlike SQL Server's `OPENQUERY`, you can reference column names in outer queries without prior declaration.
- Sorting, filtering, and joining against `odbc_query` results should be done on the DuckDB side when possible for performance.

## Extended Properties Architecture

- The `RULE4.extended_property` view provides a relational facade over `sys.extended_properties` with INSTEAD OF INSERT/UPDATE/DELETE triggers.
- MERGE statements work against this view (all three INSTEAD OF triggers are present, which is required).
- Security trimming is achieved by transferring view ownership to a no-login user (`break_ownership_user`), breaking the ownership chain so that metadata visibility rules apply per-row.
- Property names use dotted prefix conventions to define clusters: `survey.classification`, `survey.cardinality_ratio`, `fingerprint.histogram`, etc.
- The cardinality of distinct property names is expected to be small (< 100). Each cluster can be exposed as a PIVOT view.
- Extended property values are `sql_variant` (max 7,500 bytes) — sufficient for classification labels, numeric features, and compact JSON fingerprint vectors.

## Classification Signals (strongest to weakest)

1. **Foreign key membership** — column is a dimension key, full stop
2. **Primary key structure** — single-column PK = surrogate key; composite PK member without FK = degenerate dimension
3. **Histogram shape** — cardinality ratio, repeatability (avg equal_rows), discreteness (fraction of zero-range steps), range density
4. **Data type** — money/float/decimal almost always measures; varchar with low cardinality almost always dimensions; date/datetime are special cases (dimensions in DW sense but continuous histograms)

## Blob Extension Family

Local checkouts at `/Users/paulharrington/checkouts/`:
- **blobtemplates** — Inja templates + jsoncons (JMESPath, JSON diff/patch, flatten)
- **blobboxes** — Document extraction (PDF, XLSX, DOCX, text) -> 5-table schema
- **blobfilters** — Roaring bitmap domain fingerprinting
- **blobodbc** — ODBC queries as JSON scalar functions via nanodbc

All four follow the same build pattern: core C API + SQLite/DuckDB/Python wrappers, CMake + FetchContent. Each has `duckdb_ext/append_metadata.py` for extension signing.
