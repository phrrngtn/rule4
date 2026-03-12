"""
Round-trip test: Socrata catalog across DuckDB, SQLite, and/or PostgreSQL.

Each dialect creates tables from Socrata metadata, reads back column info
from the catalog, and verifies column names and type families match.

Architecture:
  - Each domain gets its own schema (PG) or temp database (DuckDB/SQLite)
  - Per-domain lifecycle: create schema → create tables → verify → drop schema
  - Parallel across domains via ThreadPoolExecutor

Usage:
    cd sql/ducklake/schema_registry
    uv run python build_dialects.py sqlite duckdb postgresql
    uv run python build_dialects.py duckdb          # just one
"""

import sys
print("starting ...", flush=True)

import json
import os
import tempfile
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb
from sqlalchemy import (
    MetaData, Table, Column, quoted_name,
    create_engine, inspect, text,
)
from sqlalchemy.schema import CreateTable
from sqlalchemy.dialects import postgresql
from sqlalchemy.pool import QueuePool

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))
from rule4.catalog import _sa_type, type_family

PG_WORKERS = 8
DUCK_WORKERS = 4
SQLITE_WORKERS = 4

VALID_DIALECTS = {"duckdb", "sqlite", "postgresql"}

PG_MAX_IDENT = 63


# ── Shared helpers ───────────────────────────────────────────────────

def load_catalog_json(path: str) -> dict[str, list[dict]]:
    """Load all_socrata_catalog.json, return {domain: [results]}."""
    with open(path) as f:
        results = json.load(f)

    by_domain = defaultdict(list)
    for r in results:
        domain = r.get("metadata", {}).get("domain", "unknown")
        by_domain[domain].append(r)

    return dict(by_domain)


def _safe_column_names(names: list[str], max_len: int = PG_MAX_IDENT) -> list[str]:
    """Truncate column names to max_len, disambiguating collisions with _N suffix."""
    result = []
    seen = set()
    for name in names:
        trunc = name[:max_len]
        if trunc in seen:
            for i in range(1, 100):
                suffix = f"_{i}"
                candidate = name[:max_len - len(suffix)] + suffix
                if candidate not in seen:
                    trunc = candidate
                    break
        seen.add(trunc)
        result.append(trunc)
    return result


def build_metadata_for_domain(domain: str, results: list[dict]) -> MetaData:
    """Build SQLAlchemy MetaData from Discovery API results for one domain.
    Column names are truncated to PG_MAX_IDENT and force-quoted to handle
    reserved words across all dialects."""
    meta = MetaData()
    for r in results:
        res = r.get("resource", {})
        table_name = res.get("id", "")
        if not table_name:
            continue

        field_names = res.get("columns_field_name") or []
        datatypes = res.get("columns_datatype") or []
        descriptions = res.get("columns_description") or []

        if not field_names:
            continue

        safe_names = _safe_column_names(field_names)

        columns = []
        for i, fname in enumerate(safe_names):
            dtype = datatypes[i] if i < len(datatypes) else "Text"
            desc = descriptions[i] if i < len(descriptions) else None
            columns.append(Column(
                quoted_name(fname, quote=True),
                _sa_type(dtype),
                comment=desc,
            ))

        desc_text = res.get("name", "")
        if res.get("description"):
            desc_text += ": " + res["description"][:200]

        Table(table_name, meta, *columns, comment=desc_text)

    return meta


def domain_to_schema(domain: str) -> str:
    """Sanitize domain name for use as a schema name."""
    return "s_" + domain.replace(".", "_").replace("-", "_")


def compare_columns(orig_table, readback: dict[str, str]) -> tuple[bool, str]:
    """Compare original table columns against readback {name: type_family}.
    Returns (ok, error_message)."""
    original_names = {c.name for c in orig_table.columns}

    if original_names != set(readback.keys()):
        missing = original_names - set(readback.keys())
        extra = set(readback.keys()) - original_names
        return False, f"column mismatch missing={missing} extra={extra}"

    type_mismatches = []
    for col in orig_table.columns:
        orig_fam = type_family(type(col.type).__name__)
        back_fam = type_family(readback[col.name])
        if orig_fam != back_fam:
            type_mismatches.append(f"{col.name}:{type(col.type).__name__}->{readback[col.name]}")
    if type_mismatches:
        return False, f"type mismatches: {', '.join(type_mismatches[:5])}"

    return True, ""


def _run_dialect_parallel(name, worker_fn, domains_meta, max_workers, **kwargs):
    """Run a dialect's worker function in parallel across domains."""
    totals = {"ok": 0, "fail": 0}
    all_errors = []
    last_print = time.time()
    t_start = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(worker_fn, domain, meta, **kwargs): domain
            for domain, meta in domains_meta
        }
        done = 0
        tables_done = 0
        for future in as_completed(futures):
            domain, ok, fail, errs = future.result()
            totals["ok"] += ok
            totals["fail"] += fail
            tables_done += ok + fail
            if errs:
                all_errors.extend([f"[{domain}] {e}" for e in errs[:2]])
            done += 1
            now = time.time()
            if now - last_print >= 1.0 or done == len(domains_meta):
                print(f"    {name}: {done}/{len(domains_meta)} domains, "
                      f"{tables_done} tables ({totals['ok']} ok, {totals['fail']} fail) "
                      f"[{now - t_start:.1f}s]",
                      flush=True)
                last_print = now

    return totals, all_errors


# ── DuckDB ───────────────────────────────────────────────────────────

def _duckdb_worker(domain: str, meta: MetaData) -> tuple[str, int, int, list[str]]:
    """Create tables in a temp DuckDB database, verify via information_schema."""
    ok = fail = 0
    errors = []
    db_path = tempfile.mktemp(suffix=".duckdb")
    schema = domain_to_schema(domain)
    pg_dialect = postgresql.dialect()

    try:
        conn = duckdb.connect(db_path)
        conn.execute(f'CREATE SCHEMA "{schema}"')

        for table in meta.tables.values():
            try:
                schema_meta = MetaData(schema=schema)
                cols = [Column(c.name, c.type) for c in table.columns]
                sa_table = Table(table.name, schema_meta, *cols)
                ddl = str(CreateTable(sa_table).compile(dialect=pg_dialect))
                conn.execute(ddl)
            except Exception as e:
                fail += 1
                errors.append(f"  {table.name}: {e}")

        # Bulk verify all tables in this schema
        rows = conn.execute(f"""
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = '{schema}'
            ORDER BY table_name, ordinal_position
        """).fetchall()

        actual = defaultdict(dict)
        for tbl, col, dtype in rows:
            actual[tbl][col] = type_family(dtype)

        for table in meta.tables.values():
            if table.name not in actual:
                continue  # already counted as fail above
            is_ok, msg = compare_columns(table, actual[table.name])
            if is_ok:
                ok += 1
            else:
                fail += 1
                errors.append(f"  {table.name}: {msg}")

        conn.close()
    finally:
        for suffix in ["", ".wal"]:
            p = db_path + suffix
            if os.path.exists(p):
                os.unlink(p)

    return domain, ok, fail, errors


def run_duckdb(domains_meta):
    """Run DuckDB round-trip test."""
    t0 = time.time()
    print(f"DuckDB ({DUCK_WORKERS} workers) ...", flush=True)
    totals, errors = _run_dialect_parallel(
        "duckdb", _duckdb_worker, domains_meta, DUCK_WORKERS,
    )
    elapsed = time.time() - t0
    print(f"  DuckDB: {totals['ok']}/{totals['ok']+totals['fail']} OK in {elapsed:.1f}s",
          flush=True)
    return {"duckdb": totals}, {"duckdb": errors}, elapsed


# ── SQLite ───────────────────────────────────────────────────────────

def _sqlite_worker(domain: str, meta: MetaData) -> tuple[str, int, int, list[str]]:
    """Create tables in a temp SQLite database, verify via SA inspect.
    SQLite has no schemas, so each domain gets its own temp database file."""
    ok = fail = 0
    errors = []
    db_path = tempfile.mktemp(suffix=".sqlite")

    try:
        engine = create_engine(f"sqlite:///{db_path}")
        insp = inspect(engine)

        for table in meta.tables.values():
            try:
                table.create(engine, checkfirst=False)
            except Exception as e:
                fail += 1
                errors.append(f"  {table.name}: {e}")

        # Bulk verify all tables
        for table in meta.tables.values():
            try:
                cols_back = insp.get_columns(table.name)
            except Exception:
                continue  # already counted as fail above
            readback = {c["name"]: type(c["type"]).__name__ for c in cols_back}
            is_ok, msg = compare_columns(table, readback)
            if is_ok:
                ok += 1
            else:
                fail += 1
                errors.append(f"  {table.name}: {msg}")

        engine.dispose()
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)

    return domain, ok, fail, errors


def run_sqlite(domains_meta):
    """Run SQLite round-trip test."""
    t0 = time.time()
    print(f"SQLite ({SQLITE_WORKERS} workers) ...", flush=True)
    totals, errors = _run_dialect_parallel(
        "sqlite", _sqlite_worker, domains_meta, SQLITE_WORKERS,
    )
    elapsed = time.time() - t0
    print(f"  SQLite: {totals['ok']}/{totals['ok']+totals['fail']} OK in {elapsed:.1f}s",
          flush=True)
    return {"sqlite": totals}, {"sqlite": errors}, elapsed


# ── PostgreSQL ───────────────────────────────────────────────────────

def _pg_worker(
    domain: str, meta: MetaData, pg_engine,
) -> tuple[str, int, int, list[str]]:
    """Create schema, create tables, verify via catalog, drop schema."""
    ok = fail = 0
    errors = []
    schema = domain_to_schema(domain)
    pg_dialect = postgresql.dialect()

    with pg_engine.connect() as conn:
        conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        conn.commit()

        # Create all tables in this schema, using SAVEPOINTs to isolate failures
        for table in meta.tables.values():
            try:
                conn.execute(text("SAVEPOINT sp"))
                schema_meta = MetaData(schema=schema)
                cols = [Column(c.name, c.type) for c in table.columns]
                sa_table = Table(table.name, schema_meta, *cols)
                ddl = str(CreateTable(sa_table).compile(dialect=pg_dialect))
                conn.execute(text(ddl))
                conn.execute(text("RELEASE SAVEPOINT sp"))
            except Exception as e:
                conn.execute(text("ROLLBACK TO SAVEPOINT sp"))
                fail += 1
                errors.append(f"  {table.name}: {e}")
        conn.commit()

        # Bulk verify all tables in this schema
        rows = conn.execute(text(f"""
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = '{schema}'
            ORDER BY table_name, ordinal_position
        """)).fetchall()

        actual = defaultdict(dict)
        for tbl, col, dtype in rows:
            actual[tbl][col] = type_family(dtype)

        for table in meta.tables.values():
            if table.name not in actual:
                continue  # already counted as fail above
            is_ok, msg = compare_columns(table, actual[table.name])
            if is_ok:
                ok += 1
            else:
                fail += 1
                errors.append(f"  {table.name}: {msg}")

        # Drop schema to keep catalog clean
        conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        conn.commit()

    return domain, ok, fail, errors


def run_postgresql(domains_meta):
    """Run PostgreSQL round-trip test."""
    pg_url = os.environ.get("PG_URL", "postgresql://localhost/rule4_test?gssencmode=disable")

    try:
        pg_engine = create_engine(
            pg_url,
            poolclass=QueuePool,
            pool_size=PG_WORKERS,
            max_overflow=2,
        )
        with pg_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print(f"PostgreSQL: pool={PG_WORKERS}", flush=True)
    except Exception as e:
        print(f"PostgreSQL not available ({e}) — skipping", flush=True)
        return {}, {}, 0

    t0 = time.time()
    print(f"PostgreSQL ({PG_WORKERS} workers) ...", flush=True)
    totals, pg_errors = _run_dialect_parallel(
        "postgresql", _pg_worker, domains_meta, PG_WORKERS,
        pg_engine=pg_engine,
    )
    elapsed = time.time() - t0
    print(f"  PostgreSQL: {totals['ok']}/{totals['ok']+totals['fail']} OK in {elapsed:.1f}s",
          flush=True)

    pg_engine.dispose()

    return {"postgresql": totals}, {"postgresql": pg_errors}, elapsed


# ── Main ─────────────────────────────────────────────────────────────

DIALECT_RUNNERS = {
    "duckdb": run_duckdb,
    "sqlite": run_sqlite,
    "postgresql": run_postgresql,
}


def main():
    dialects = sys.argv[1:] if len(sys.argv) > 1 else sorted(VALID_DIALECTS)
    for d in dialects:
        if d not in VALID_DIALECTS:
            print(f"ERROR: unknown dialect '{d}'. Valid: {', '.join(sorted(VALID_DIALECTS))}")
            sys.exit(1)

    catalog_path = "raw/all_socrata_catalog.json"
    if not os.path.exists(catalog_path):
        print(f"ERROR: {catalog_path} not found. Run fetch_all_socrata.sh first.")
        sys.exit(1)

    print(f"Loading {catalog_path} ...", flush=True)
    by_domain = load_catalog_json(catalog_path)

    domains_meta = []
    for domain in sorted(by_domain, key=lambda d: -len(by_domain[d])):
        meta = build_metadata_for_domain(domain, by_domain[domain])
        if len(meta.tables) > 0:
            domains_meta.append((domain, meta))

    total_tables = sum(len(m.tables) for _, m in domains_meta)
    print(f"{len(domains_meta)} domains, {total_tables} tables\n", flush=True)

    all_results = {}
    all_errors = {}
    total_time = 0

    for dialect in dialects:
        results, errors, elapsed = DIALECT_RUNNERS[dialect](domains_meta)
        all_results.update(results)
        all_errors.update(errors)
        total_time += elapsed

    # ── Summary ───────────────────────────────────────────────────────
    print(f"\n{'='*70}")
    print(f"SUMMARY — {len(domains_meta)} domains, {total_tables} tables, {total_time:.1f}s")
    print(f"{'='*70}")

    for dialect, counts in all_results.items():
        total = counts["ok"] + counts["fail"]
        pct = 100 * counts["ok"] / total if total > 0 else 0
        print(f"  {dialect:12s}: {counts['ok']:6d} OK / {total:6d} total ({pct:.1f}%)")

    for dialect, errs in all_errors.items():
        if errs:
            print(f"\n  Sample errors ({dialect}, first 10):")
            for e in errs[:10]:
                print(f"    {e}")

    print(flush=True)


if __name__ == "__main__":
    main()
