"""
Test DuckLake catalog schema creation across DuckDB, SQLite, and PostgreSQL.

Creates the catalog tables via SQLAlchemy, then verifies round-trip by
inspecting the created schema and comparing table/column counts.

Usage:
    cd sql/ducklake/experiment_pg
    uv run python test_catalog_dialects.py
"""

import sys
import os
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))

import duckdb as _duckdb
from sqlalchemy import create_engine, inspect, text
from rule4.ducklake_catalog import _build_metadata, create_catalog

EXPECTED_TABLES = 28


def _verify_duckdb(db_path, schema=None):
    """Verify DuckLake tables in DuckDB using information_schema (more reliable
    than SA inspect for DuckDB)."""
    conn = _duckdb.connect(db_path)
    schema_filter = f"= '{schema}'" if schema else "= 'main'"
    rows = conn.execute(f"""
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema {schema_filter}
          AND table_name LIKE 'ducklake_%'
        ORDER BY table_name, ordinal_position
    """).fetchall()
    conn.close()

    from collections import defaultdict
    actual = defaultdict(list)
    for tbl, col in rows:
        actual[tbl].append(col)
    return dict(actual)


def test_dialect(name, engine, schema=None, db_path=None):
    """Create DuckLake catalog, verify table count, return (ok, detail)."""
    try:
        if engine is not None:
            create_catalog(engine, schema=schema)
        ref_meta = _build_metadata(schema=schema)

        if db_path and engine is None:
            actual = _verify_duckdb(db_path, schema=schema)
            n = len(actual)
            mismatches = []
            for table_name, table_obj in ref_meta.tables.items():
                bare_name = table_name.split(".")[-1]
                if bare_name not in actual:
                    mismatches.append(f"{bare_name}: missing")
                elif len(actual[bare_name]) != len(table_obj.columns):
                    mismatches.append(f"{bare_name}: expected {len(table_obj.columns)} cols, got {len(actual[bare_name])}")
        else:
            insp = inspect(engine)
            tables = insp.get_table_names(schema=schema)
            ducklake_tables = [t for t in tables if t.startswith("ducklake_")]
            n = len(ducklake_tables)
            mismatches = []
            for table_name, table_obj in ref_meta.tables.items():
                bare_name = table_name.split(".")[-1]
                try:
                    cols = insp.get_columns(bare_name, schema=schema)
                    if len(cols) != len(table_obj.columns):
                        mismatches.append(f"{bare_name}: expected {len(table_obj.columns)} cols, got {len(cols)}")
                except Exception as e:
                    mismatches.append(f"{bare_name}: {e}")

        if n == EXPECTED_TABLES and not mismatches:
            return True, f"{n} tables, all columns match"
        else:
            detail = f"{n}/{EXPECTED_TABLES} tables"
            if mismatches:
                detail += f", mismatches: {'; '.join(mismatches[:5])}"
            if n < EXPECTED_TABLES:
                # Find which tables are missing
                expected_names = {t.split(".")[-1] for t in ref_meta.tables}
                if db_path and engine is None:
                    actual_names = set(actual.keys())
                else:
                    actual_names = set(ducklake_tables)
                missing = expected_names - actual_names
                if missing:
                    detail += f", missing: {missing}"
            return False, detail
    except Exception as e:
        return False, str(e)


def main():
    results = {}

    # ── DuckDB ────────────────────────────────────────────────────────
    db_path = tempfile.mktemp(suffix=".duckdb")
    try:
        engine = create_engine(f"duckdb:///{db_path}")
        create_catalog(engine)
        engine.dispose()
        # Verify with native duckdb (SA engine must be closed first)
        ok, detail = test_dialect("duckdb", None, db_path=db_path)
        results["duckdb"] = (ok, detail)
    finally:
        for suffix in ["", ".wal"]:
            p = db_path + suffix
            if os.path.exists(p):
                os.unlink(p)

    # ── SQLite ────────────────────────────────────────────────────────
    db_path = tempfile.mktemp(suffix=".sqlite")
    try:
        engine = create_engine(f"sqlite:///{db_path}")
        ok, detail = test_dialect("sqlite", engine)
        results["sqlite"] = (ok, detail)
        engine.dispose()
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)

    # ── PostgreSQL ────────────────────────────────────────────────────
    pg_url = os.environ.get("PG_URL", "postgresql://localhost/rule4_test?gssencmode=disable")
    try:
        engine = create_engine(pg_url)
        with engine.connect() as conn:
            conn.execute(text('DROP SCHEMA IF EXISTS "ducklake" CASCADE'))
            conn.commit()
        ok, detail = test_dialect("postgresql", engine, schema="ducklake")
        results["postgresql"] = (ok, detail)
        engine.dispose()
    except Exception as e:
        results["postgresql"] = (False, f"connection failed: {e}")

    # ── Summary ───────────────────────────────────────────────────────
    print(f"{'='*60}")
    print(f"DuckLake catalog schema — round-trip test")
    print(f"{'='*60}")
    all_ok = True
    for dialect, (ok, detail) in results.items():
        status = "OK" if ok else "FAIL"
        print(f"  {dialect:12s}: {status:4s}  {detail}")
        if not ok:
            all_ok = False

    if all_ok:
        print(f"\nAll dialects passed.")
    else:
        print(f"\nSome dialects failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
