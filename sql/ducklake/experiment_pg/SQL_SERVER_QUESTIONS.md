# SQL Server Setup — RESOLVED

## Working Setup (March 2026)

**Azure SQL Edge** via Docker Desktop on macOS Apple Silicon (ARM64 native).

```bash
docker run -d --name sql_edge \
  -e "ACCEPT_EULA=Y" \
  -e "MSSQL_SA_PASSWORD=Rule4Test!2024" \
  -e "MSSQL_COLLATION=Latin1_General_100_CS_AS_SC_UTF8" \
  -p 1433:1433 \
  mcr.microsoft.com/azure-sql-edge:latest
```

- **Image**: `mcr.microsoft.com/azure-sql-edge:latest` (ARM64, ~1.5GB)
- **Version**: Microsoft Azure SQL Edge Developer (RTM) - 15.0.2000.1574 (ARM64)
- **Collation**: `Latin1_General_100_CS_AS_SC_UTF8` (case-sensitive, UTF-8)
- **ODBC driver**: `msodbcsql18` via Homebrew (`brew install msodbcsql18`)
- **Python driver**: `pyodbc` (SA dialect: `mssql+pyodbc`)

### Credentials

- **SA** (bootstrap only): `sa` / `Rule4Test!2024`
- **App login**: `rule4` / `Rule4Dev!2024` — `db_owner` on `rule4_test`

### SA connection string (pyodbc)
```
DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost,1433;UID=sa;PWD=Rule4Test!2024;TrustServerCertificate=yes;
```

### App connection string (SQLAlchemy)
```
mssql+pyodbc://rule4:Rule4Dev!2024@localhost:1433/rule4_test?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes
```

## JSON Path Syntax

SQL Server's `JSON_VALUE` requires double-quoted keys for special characters:

- `$.name` — plain key (works)
- `$.":id"` — colon-prefixed key (must double-quote)
- `$.:id` — **FAILS** (`Unexpected character ':' at position 2`)

This double-quoted syntax also works on SQLite and DuckDB, so `_json_path()`
uses it uniformly for keys with special characters.

## TTST Sync — Verified Working

All four dialects tested with identical payload and identical results:
- **PostgreSQL**: `jsonb_array_elements(:payload::jsonb)` + `elem ->> 'key'`
- **SQLite**: `json_each(:payload)` + `elem.value ->> '$.key'`
- **DuckDB**: `unnest(from_json(:payload, ...))` + `elem ->> '$.key'`
- **SQL Server**: `OPENJSON(:payload)` + `JSON_VALUE(elem.value, '$.key')`

`NullSafeNE` custom element handles SQL Server's lack of `IS DISTINCT FROM`.
