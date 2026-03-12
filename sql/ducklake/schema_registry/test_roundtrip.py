"""
Round-trip test: read Socrata schema from rule4 catalog, create tables
in DuckDB and SQLite, read back via their native catalogs, compare.
Also validate PostgreSQL DDL compilation (no running server needed).

Usage:
    cd sql/ducklake/schema_registry
    uv run python test_roundtrip.py
"""

import os
import tempfile
from collections import defaultdict

import duckdb
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.schema import CreateTable
from sqlalchemy.dialects import postgresql, sqlite

from rule4.catalog import open_catalog, type_family


def test_dialects_compile(catalog):
    """Verify DDL compiles without error for multiple dialects."""
    # Pick a sample of datasets with varying column types
    sample_ids = [
        "ic3t-wcy2",   # DOB Job Filings — 96 cols, mix of Text/Number
        "8wbx-tsch",   # FHV Active — 23 cols
        "dpec-ucu7",   # TLC Driver Status — 12 cols, Calendar date
    ]

    dialects = {
        "postgresql": postgresql.dialect(),
        "sqlite": sqlite.dialect(),
    }

    print("=== DDL Compilation Test ===")
    errors = []
    for did in sample_ids:
        t = catalog.table("data.cityofnewyork.us", "main", did, target_schema="socrata")
        for dialect_name, dialect in dialects.items():
            try:
                ddl = str(CreateTable(t).compile(dialect=dialect))
                assert len(ddl) > 0
                print(f"  OK  {did:12s} -> {dialect_name:12s} ({len(t.columns)} cols, {len(ddl)} chars)")
            except Exception as e:
                errors.append((did, dialect_name, str(e)))
                print(f"  FAIL {did:12s} -> {dialect_name:12s}: {e}")

    if errors:
        print(f"\n  {len(errors)} compilation errors!")
    else:
        print(f"\n  All {len(sample_ids) * len(dialects)} compilations OK")
    return errors


def roundtrip_duckdb(catalog, sample_ids):
    """Create tables in a fresh DuckDB, read back schema via information_schema, compare."""
    print("\n=== DuckDB Round-Trip ===")
    errors = []

    db_path = tempfile.mktemp(suffix=".duckdb")

    try:
        conn = duckdb.connect(db_path)

        for did in sample_ids:
            t = catalog.table("data.cityofnewyork.us", "main", did)

            # Generate DDL and execute it
            from sqlalchemy.schema import CreateTable
            from sqlalchemy.dialects import postgresql
            # DuckDB is close enough to PG for basic DDL
            ddl = str(CreateTable(t).compile(dialect=postgresql.dialect()))
            conn.execute(ddl)

            # Read back via information_schema
            cols_back = conn.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = '{did}'
                ORDER BY ordinal_position
            """).fetchall()
            readback_names = {r[0] for r in cols_back}
            original_names = {c.name for c in t.columns}

            if original_names != readback_names:
                missing = original_names - readback_names
                extra = readback_names - original_names
                errors.append((did, "duckdb", f"missing={missing}, extra={extra}"))
                print(f"  FAIL {did}: column name mismatch")
            else:
                print(f"  OK  {did:12s}: {len(original_names)} cols round-tripped")

            conn.execute(f'DROP TABLE IF EXISTS "{did}"')

        conn.close()
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)

    return errors


def roundtrip_sqlite(catalog, sample_ids):
    """Create tables in a fresh SQLite, read back schema, compare."""
    print("\n=== SQLite Round-Trip ===")
    errors = []

    db_path = tempfile.mktemp(suffix=".sqlite")

    try:
        engine = create_engine(f"sqlite:///{db_path}")

        for did in sample_ids:
            t = catalog.table("data.cityofnewyork.us", "main", did)
            t.metadata.create_all(engine)

            insp = inspect(engine)
            cols_back = insp.get_columns(did)

            original_cols = {c.name: c for c in t.columns}
            readback_cols = {c["name"]: c for c in cols_back}

            if set(original_cols.keys()) != set(readback_cols.keys()):
                missing = set(original_cols.keys()) - set(readback_cols.keys())
                extra = set(readback_cols.keys()) - set(original_cols.keys())
                errors.append((did, "sqlite", f"column mismatch: missing={missing}, extra={extra}"))
                print(f"  FAIL {did}: column name mismatch")
            else:
                print(f"  OK  {did:12s}: {len(original_cols)} cols round-tripped")

            with engine.connect() as conn:
                conn.execute(text(f'DROP TABLE IF EXISTS "{did}"'))
                conn.commit()

        engine.dispose()
    finally:
        os.unlink(db_path)

    return errors


def type_coverage_report(catalog):
    """Check type mapping coverage across the full Socrata catalog."""
    print("\n=== Type Coverage ===")
    conn = catalog._conn
    rows = conn.execute(f"""
        SELECT data_type, count(*) AS n
        FROM {catalog._lake}.rule4_column
        WHERE catalog_name = 'data.cityofnewyork.us'
        GROUP BY ALL
        ORDER BY n DESC
    """).fetchall()

    from rule4.catalog import _sa_type, String
    unmapped = []
    for dtype, count in rows:
        sa = _sa_type(dtype)
        status = "mapped" if sa is not String or dtype == "Text" else "DEFAULT->String"
        print(f"  {dtype:20s} {count:>6d}  -> {sa.__name__:15s} ({status})")
        if status == "DEFAULT->String" and dtype not in ("Text", "URL", "Url"):
            unmapped.append((dtype, count))

    if unmapped:
        print(f"\n  {len(unmapped)} types falling back to String (may need explicit mapping)")
    else:
        print(f"\n  All {len(rows)} types have explicit mappings")
    return unmapped


def detailed_type_roundtrip(catalog, sample_ids):
    """Compare not just column names but also type families after round-trip."""
    print("\n=== Detailed Type Round-Trip (DuckDB) ===")
    mismatches = []

    db_path = tempfile.mktemp(suffix=".duckdb")

    try:
        conn = duckdb.connect(db_path)

        for did in sample_ids:
            t = catalog.table("data.cityofnewyork.us", "main", did)

            from sqlalchemy.schema import CreateTable
            from sqlalchemy.dialects import postgresql
            ddl = str(CreateTable(t).compile(dialect=postgresql.dialect()))
            conn.execute(ddl)

            cols_back = conn.execute(f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = '{did}'
                ORDER BY ordinal_position
            """).fetchall()
            back_types = {r[0]: r[1] for r in cols_back}

            for col in t.columns:
                if col.name in back_types:
                    orig_family = type_family(type(col.type).__name__)
                    back_family = type_family(back_types[col.name])
                    if orig_family != back_family:
                        mismatches.append((did, col.name, type(col.type).__name__, back_types[col.name]))

            conn.execute(f'DROP TABLE IF EXISTS "{did}"')

        conn.close()
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)

    if mismatches:
        print(f"  {len(mismatches)} type mismatches:")
        for did, col, orig, back in mismatches[:20]:
            print(f"    {did}.{col}: {orig} -> {back}")
        if len(mismatches) > 20:
            print(f"    ... and {len(mismatches) - 20} more")
    else:
        print(f"  All types round-tripped correctly for {len(sample_ids)} tables")

    return mismatches


    # type_family is now imported from rule4.catalog


def main():
    catalog = open_catalog()

    # Use a broader sample for thorough testing
    sample_ids = [
        "ic3t-wcy2",   # 96 cols — Text, Number
        "8wbx-tsch",   # 23 cols — Text, Calendar date, Point
        "dpec-ucu7",   # 12 cols — Text, Calendar date
        "vx8i-nprf",   # 20 cols — Text, Number, Calendar date
        "xjfq-wh2d",   # 7 cols — Text, Calendar date
    ]

    # 1. Type coverage across full catalog
    type_coverage_report(catalog)

    # 2. DDL compiles for multiple dialects
    test_dialects_compile(catalog)

    # 3. Round-trip: DuckDB
    roundtrip_duckdb(catalog, sample_ids)

    # 4. Round-trip: SQLite
    roundtrip_sqlite(catalog, sample_ids)

    # 5. Detailed type round-trip
    detailed_type_roundtrip(catalog, sample_ids)

    print("\n=== Done ===")


if __name__ == "__main__":
    main()
