"""
Round-trip test: ALL Socrata domains across DuckDB, SQLite, and PostgreSQL.

Architecture:
  - PG: create all tables first (parallel, CREATE TABLE via table.create()),
    then verify all at once with two bulk catalog queries
  - DuckDB/SQLite: parallel per-domain with per-worker temp databases
  - PG tablespace on RAM disk if available
  - Schemas dropped on setup, not teardown

Usage:
    cd sql/ducklake/schema_registry
    uv run python build_all_dialects.py
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
    MetaData, Table, Column, String, Integer, BigInteger,
    Numeric, Float, Boolean, DateTime, Date, Text,
    create_engine, inspect, text,
)
from sqlalchemy.schema import CreateTable
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.pool import QueuePool

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))
from rule4.catalog import _sa_type, type_family

PG_WORKERS = 8
DUCK_WORKERS = 4
SQLITE_WORKERS = 4


def load_catalog_json(path: str) -> dict[str, list[dict]]:
    """Load all_socrata_catalog.json, return {domain: [results]}."""
    with open(path) as f:
        results = json.load(f)

    by_domain = defaultdict(list)
    for r in results:
        domain = r.get("metadata", {}).get("domain", "unknown")
        by_domain[domain].append(r)

    return dict(by_domain)


def build_metadata_for_domain(domain: str, results: list[dict]) -> MetaData:
    """Build SQLAlchemy MetaData from Discovery API results for one domain."""
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

        columns = []
        for i, fname in enumerate(field_names):
            dtype = datatypes[i] if i < len(datatypes) else "Text"
            desc = descriptions[i] if i < len(descriptions) else None
            columns.append(Column(fname, _sa_type(dtype), comment=desc))

        desc_text = res.get("name", "")
        if res.get("description"):
            desc_text += ": " + res["description"][:200]

        Table(table_name, meta, *columns, comment=desc_text)

    return meta


def domain_to_schema(domain: str) -> str:
    """Sanitize domain name for use as a PG/DuckDB schema name."""
    return "s_" + domain.replace(".", "_").replace("-", "_")


def compare_columns(orig_table, readback: dict[str, str]) -> tuple[bool, str]:
    """Compare original table columns against readback {name: type_name}.
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


# ── DuckDB round-trip (per-worker temp database) ─────────────────────

def _duckdb_worker(domain: str, meta: MetaData) -> tuple[str, int, int, list[str]]:
    ok = fail = 0
    errors = []
    db_path = tempfile.mktemp(suffix=".duckdb")

    try:
        conn = duckdb.connect(db_path)
        pg_dialect = postgresql.dialect()

        for table in meta.tables.values():
            try:
                ddl = str(CreateTable(table).compile(dialect=pg_dialect))
                conn.execute(ddl)

                cols_back = conn.execute(f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = '{table.name}'
                    ORDER BY ordinal_position
                """).fetchall()
                readback = {r[0]: r[1] for r in cols_back}

                is_ok, msg = compare_columns(table, readback)
                if is_ok:
                    ok += 1
                else:
                    fail += 1
                    errors.append(f"  {table.name}: {msg}")

            except Exception as e:
                fail += 1
                errors.append(f"  {table.name}: {e}")

        conn.close()
    finally:
        for suffix in ["", ".wal"]:
            p = db_path + suffix
            if os.path.exists(p):
                os.unlink(p)

    return domain, ok, fail, errors


# ── SQLite round-trip (per-worker temp database) ─────────────────────

def _sqlite_worker(domain: str, meta: MetaData) -> tuple[str, int, int, list[str]]:
    ok = fail = 0
    errors = []
    db_path = tempfile.mktemp(suffix=".sqlite")

    try:
        engine = create_engine(f"sqlite:///{db_path}")
        insp = inspect(engine)

        for table in meta.tables.values():
            try:
                table.create(engine, checkfirst=False)

                cols_back = insp.get_columns(table.name)
                readback = {c["name"]: type(c["type"]).__name__ for c in cols_back}

                is_ok, msg = compare_columns(table, readback)
                if is_ok:
                    ok += 1
                else:
                    fail += 1
                    errors.append(f"  {table.name}: {msg}")

            except Exception as e:
                fail += 1
                errors.append(f"  {table.name}: {e}")

        engine.dispose()
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)

    return domain, ok, fail, errors


# ── PostgreSQL: bulk create then bulk verify ─────────────────────────

def _pg_create_worker(
    domain: str, meta: MetaData, pg_engine,
) -> tuple[str, int, int, list[str]]:
    """Create all tables for one domain in a single transaction.
    Table names are prefixed with sanitized domain to avoid schemas."""
    ok = fail = 0
    errors = []
    prefix = domain_to_schema(domain)
    pg_dialect = postgresql.dialect()

    with pg_engine.connect() as conn:
        for table in meta.tables.values():
            prefixed_name = f"{prefix}__{table.name}"
            try:
                pg_meta = MetaData()
                cols = [Column(c.name, c.type) for c in table.columns]
                sa_table = Table(prefixed_name, pg_meta, *cols)
                ddl = str(CreateTable(sa_table).compile(dialect=pg_dialect))
                conn.execute(text(ddl))
                ok += 1
            except Exception as e:
                fail += 1
                errors.append(f"  {table.name}: {e}")
        conn.commit()

    return domain, ok, fail, errors


def pg_bulk_verify(pg_engine, domains_meta: list[tuple[str, MetaData]]) -> tuple[dict, list]:
    """Verify all PG tables at once using a bulk catalog query.
    Tables are in public schema with prefixed names: {domain_prefix}__{dataset_id}.
    Returns (totals_dict, errors_list)."""

    # Build expected: {prefixed_table_name: {col_name: sa_type_family}}
    expected = {}
    for domain, meta in domains_meta:
        prefix = domain_to_schema(domain)
        for table in meta.tables.values():
            prefixed_name = f"{prefix}__{table.name}"
            expected[prefixed_name] = {
                c.name: type_family(type(c.type).__name__)
                for c in table.columns
            }

    # Bulk query: all columns from public schema with our prefixed table names
    # Use a prefix filter — all our tables start with "s_"
    with pg_engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name LIKE 's\\_%'
            ORDER BY table_name, ordinal_position
        """)).fetchall()

    # Group readback by table_name
    actual = defaultdict(dict)
    for tbl, col, dtype in rows:
        actual[tbl][col] = type_family(dtype)

    # Compare
    ok = fail = 0
    errors = []

    for prefixed_name, exp_cols in expected.items():
        if prefixed_name not in actual:
            # Table wasn't created (create error) — skip, already counted
            continue

        act_cols = actual[prefixed_name]
        exp_names = set(exp_cols.keys())
        act_names = set(act_cols.keys())

        if exp_names != act_names:
            missing = exp_names - act_names
            extra = act_names - exp_names
            fail += 1
            errors.append(f"  {prefixed_name}: column mismatch missing={missing} extra={extra}")
        else:
            type_mismatches = []
            for col_name, exp_fam in exp_cols.items():
                act_fam = act_cols[col_name]
                if exp_fam != act_fam:
                    type_mismatches.append(f"{col_name}:{exp_fam}->{act_fam}")
            if type_mismatches:
                fail += 1
                errors.append(f"  {prefixed_name}: type mismatches: {', '.join(type_mismatches[:5])}")
            else:
                ok += 1

    return {"ok": ok, "fail": fail}, errors


def _run_dialect_parallel(name, worker_fn, domains_meta, max_workers, **kwargs):
    """Run a dialect's tests in parallel across domains."""
    totals = {"ok": 0, "fail": 0}
    all_errors = []
    last_print = time.time()

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
                elapsed = now - (last_print - (now - last_print)) if done == 1 else now - last_print
                print(f"    {name}: {done}/{len(domains_meta)} domains, "
                      f"{tables_done} tables ({totals['ok']} ok, {totals['fail']} fail)",
                      flush=True)
                last_print = now

    return totals, all_errors


def main():
    catalog_path = "raw/all_socrata_catalog.json"
    if not os.path.exists(catalog_path):
        print(f"ERROR: {catalog_path} not found. Run fetch_all_socrata.sh first.")
        sys.exit(1)

    # ── PG setup ──────────────────────────────────────────────────────
    pg_url = os.environ.get("PG_URL", "postgresql://localhost/rule4_test")
    pg_engine = None

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
        pg_engine = None

    # ── Load catalog ──────────────────────────────────────────────────
    print(f"Loading {catalog_path} ...", flush=True)
    by_domain = load_catalog_json(catalog_path)

    domains_meta = []
    for domain in sorted(by_domain, key=lambda d: -len(by_domain[d])):
        meta = build_metadata_for_domain(domain, by_domain[domain])
        if len(meta.tables) > 0:
            domains_meta.append((domain, meta))

    total_tables = sum(len(m.tables) for _, m in domains_meta)
    print(f"{len(domains_meta)} domains, {total_tables} tables\n", flush=True)

    # PG: assumes pg_setup.sh has been run to provide a clean database
    # No schemas — table names are prefixed with sanitized domain

    results = {}

    # ── DuckDB ────────────────────────────────────────────────────────
    t0 = time.time()
    print(f"DuckDB ({DUCK_WORKERS} workers) ...", flush=True)
    results["duckdb"], duck_errors = _run_dialect_parallel(
        "duckdb", _duckdb_worker, domains_meta, DUCK_WORKERS,
    )
    t_duck = time.time() - t0
    d = results["duckdb"]
    print(f"  DuckDB: {d['ok']}/{d['ok']+d['fail']} OK in {t_duck:.1f}s", flush=True)

    # ── SQLite ────────────────────────────────────────────────────────
    t0 = time.time()
    print(f"SQLite ({SQLITE_WORKERS} workers) ...", flush=True)
    results["sqlite"], sqlite_errors = _run_dialect_parallel(
        "sqlite", _sqlite_worker, domains_meta, SQLITE_WORKERS,
    )
    t_sqlite = time.time() - t0
    d = results["sqlite"]
    print(f"  SQLite: {d['ok']}/{d['ok']+d['fail']} OK in {t_sqlite:.1f}s", flush=True)

    # ── PostgreSQL: create phase ──────────────────────────────────────
    pg_create_errors = []
    pg_verify_errors = []
    t_pg = 0
    if pg_engine:
        t0 = time.time()
        print(f"PostgreSQL CREATE ({PG_WORKERS} workers) ...", flush=True)
        pg_create_totals, pg_create_errors = _run_dialect_parallel(
            "pg_create", _pg_create_worker, domains_meta, PG_WORKERS,
            pg_engine=pg_engine,
        )
        t_create = time.time() - t0
        print(f"  PG create: {pg_create_totals['ok']}/{pg_create_totals['ok']+pg_create_totals['fail']} "
              f"OK in {t_create:.1f}s "
              f"({pg_create_totals['fail']} create errors)", flush=True)

        # ── PostgreSQL: bulk verify phase ─────────────────────────────
        t1 = time.time()
        print(f"PostgreSQL VERIFY (bulk catalog query) ...", flush=True)
        pg_verify_totals, pg_verify_errors = pg_bulk_verify(pg_engine, domains_meta)
        t_verify = time.time() - t1
        t_pg = time.time() - t0

        results["pg_create"] = pg_create_totals
        results["pg_verify"] = pg_verify_totals
        print(f"  PG verify: {pg_verify_totals['ok']}/{pg_verify_totals['ok']+pg_verify_totals['fail']} "
              f"OK in {t_verify:.1f}s", flush=True)
        print(f"  PG total: {t_pg:.1f}s", flush=True)

    # ── Summary ───────────────────────────────────────────────────────
    t_total = t_duck + t_sqlite + t_pg
    print(f"\n{'='*70}")
    print(f"SUMMARY — {len(domains_meta)} domains, {total_tables} tables, {t_total:.1f}s")
    print(f"{'='*70}")

    for dialect, counts in results.items():
        total = counts["ok"] + counts["fail"]
        pct = 100 * counts["ok"] / total if total > 0 else 0
        print(f"  {dialect:12s}: {counts['ok']:6d} OK / {total:6d} total ({pct:.1f}%)")

    all_errors = {
        "duckdb": duck_errors,
        "sqlite": sqlite_errors,
        "pg_create": pg_create_errors,
        "pg_verify": pg_verify_errors,
    }
    for dialect, errs in all_errors.items():
        if errs:
            print(f"\n  Sample errors ({dialect}, first 10):")
            for e in errs[:10]:
                print(f"    {e}")

    if pg_engine:
        pg_engine.dispose()

    print(flush=True)


if __name__ == "__main__":
    main()
