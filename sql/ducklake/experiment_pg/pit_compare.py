"""
PIT (point-in-time) query comparison: DuckDB vs PostgreSQL vs SQLite catalog backends,
with both local filesystem and S3 (MinIO) storage.

Creates identical DuckLake catalogs in all three backends, populates them with the
same Socrata metadata and data files, then runs the same queries through
the DuckLake facade and verifies identical results.

Two storage modes are tested:
  1. Local filesystem (Parquet files on disk)
  2. S3 via MinIO (same Parquet files in an S3-compatible bucket)

Usage:
    cd sql/ducklake/experiment_pg
    uv run python pit_compare.py
"""

import json
import struct
import os
import sys
import tempfile
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))

import duckdb
from sqlalchemy import create_engine, text
from rule4.ducklake_catalog import create_catalog, DUCKLAKE_VERSION

PG_URL = os.environ.get("PG_URL", "postgresql://localhost/rule4_test?gssencmode=disable")
PG_SCHEMA_LOCAL = "ducklake_pit"
PG_SCHEMA_S3 = "ducklake_pit_s3"
EXPERIMENT_DIR = Path(__file__).resolve().parent.parent / "experiment"
LOCAL_DATA_PATH = str(EXPERIMENT_DIR / "data") + "/"
S3_DATA_PATH = "s3://ducklake-data/"
CATALOG_JSON = EXPERIMENT_DIR / "raw" / "data.cityofnewyork.us" / "catalog.json"

# MinIO defaults
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_KEY = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")


def load_datasets():
    with open(CATALOG_JSON) as f:
        catalog = json.load(f)
    datasets = []
    for r in catalog["results"]:
        res = r["resource"]
        datasets.append({
            "id": res["id"],
            "name": res["name"],
            "updated_at": res.get("data_updated_at"),
            "field_names": res.get("columns_field_name", []),
        })
    return datasets


def _ts_literal(ts_str, dialect):
    """Return a timestamp literal appropriate for the dialect."""
    if dialect == "sqlite":
        return f"'{ts_str}'"
    else:
        return f"'{ts_str}'::timestamptz"


def _now_literal(dialect):
    """Return a NOW() expression appropriate for the dialect."""
    if dialect == "sqlite":
        return "datetime('now')"
    else:
        return "NOW()"


def _qualify(table_name, schema):
    """Qualify a table name with schema if provided."""
    if schema:
        return f'"{schema}".{table_name}'
    return table_name


def populate_catalog_sa(engine, schema, datasets, data_path, dialect="postgresql"):
    """Populate DuckLake metadata via SQLAlchemy (works for DuckDB, PG, and SQLite)."""

    with engine.connect() as conn:
        q_meta = _qualify("ducklake_metadata", schema)
        q_snap = _qualify("ducklake_snapshot", schema)
        q_schema = _qualify("ducklake_schema", schema)
        q_snap_changes = _qualify("ducklake_snapshot_changes", schema)
        q_schema_ver = _qualify("ducklake_schema_versions", schema)
        q_table = _qualify("ducklake_table", schema)
        q_column = _qualify("ducklake_column", schema)
        q_data_file = _qualify("ducklake_data_file", schema)

        conn.execute(text(f"""
            INSERT INTO {q_meta} (key, value)
            VALUES ('version', '{DUCKLAKE_VERSION}'),
                   ('created_by', 'rule4 PIT test'),
                   ('data_path', '{data_path}'),
                   ('encrypted', 'false')
        """))
        conn.execute(text(f"""
            INSERT INTO {q_snap}
            VALUES (0, {_ts_literal('2024-01-01T00:00:00Z', dialect)}, 0, 1, 0)
        """))
        conn.execute(text(f"""
            INSERT INTO {q_schema}
            VALUES (0, '{uuid4()}', 0, NULL, 'main', 'main/', true)
        """))
        conn.execute(text(f"""
            INSERT INTO {q_snap_changes}
            VALUES (0, 'created_schema:"main"', NULL, NULL, NULL)
        """))
        conn.execute(text(f"""
            INSERT INTO {q_schema_ver}
            VALUES (0, 0, NULL)
        """))

        next_cat_id = 1
        next_snap = 1
        schema_ver = 1
        table_ids = {}

        for ds in datasets:
            table_id = next_cat_id
            next_cat_id += 1
            snap = next_snap
            next_snap += 1
            table_ids[ds["id"]] = (table_id, snap)

            col_ids = []
            for i, fname in enumerate(ds["field_names"]):
                col_id = next_cat_id
                next_cat_id += 1
                col_ids.append((col_id, i + 1, fname))

            ts = ds["updated_at"] or "2024-01-01T00:00:00Z"

            conn.execute(text(f"""
                INSERT INTO {q_snap}
                VALUES ({snap}, {_ts_literal(ts, dialect)}, {schema_ver}, {next_cat_id}, 0)
            """))
            conn.execute(text(f"""
                INSERT INTO {q_schema_ver}
                VALUES ({snap}, {schema_ver}, NULL)
            """))
            schema_ver += 1

            conn.execute(text(f"""
                INSERT INTO {q_table}
                VALUES ({table_id}, '{uuid4()}', {snap}, NULL, 0,
                        '{ds["id"]}', '{ds["id"]}/', true)
            """))

            for col_id, col_order, fname in col_ids:
                safe = fname.replace("'", "''")
                conn.execute(text(f"""
                    INSERT INTO {q_column}
                    (column_id, begin_snapshot, end_snapshot, table_id, column_order,
                     column_name, column_type, nulls_allowed)
                    VALUES ({col_id}, {snap}, NULL, {table_id}, {col_order},
                            '{safe}', 'varchar', true)
                """))

            conn.execute(text(f"""
                INSERT INTO {q_snap_changes}
                VALUES ({snap}, 'created_table:"main"."{ds["id"]}"',
                        'rule4', 'Import {ds["name"].replace("'", "''")}', NULL)
            """))

        # Register data files — use local Parquet to get file sizes regardless of storage mode
        next_file_id = 0
        for ds in datasets:
            pq = EXPERIMENT_DIR / "data" / "main" / ds["id"] / "data_0.parquet"
            if not pq.exists():
                continue
            file_size = pq.stat().st_size
            with open(pq, "rb") as f:
                f.seek(-8, 2)
                footer_size = struct.unpack("<i", f.read(4))[0]

            table_id, _ = table_ids[ds["id"]]
            snap = next_snap
            next_snap += 1

            conn.execute(text(f"""
                INSERT INTO {q_snap}
                VALUES ({snap}, {_now_literal(dialect)}, {schema_ver - 1}, {next_cat_id}, {next_file_id + 1})
            """))
            conn.execute(text(f"""
                INSERT INTO {q_snap_changes}
                VALUES ({snap}, 'inserted_into_table:{table_id}',
                        'rule4', 'Load {ds["id"]}', NULL)
            """))
            conn.execute(text(f"""
                INSERT INTO {q_data_file}
                (data_file_id, table_id, begin_snapshot, end_snapshot,
                 path, path_is_relative, file_format, record_count,
                 file_size_bytes, footer_size, row_id_start)
                VALUES ({next_file_id}, {table_id}, {snap}, NULL,
                        'data_0.parquet', true, 'parquet', 500,
                        {file_size}, {footer_size}, 0)
            """))
            next_file_id += 1

        conn.commit()

    return table_ids


def run_queries(conn, lake_name):
    """Run a set of queries and return results as a dict for comparison."""
    results = {}

    # Q1: Table discovery
    rows = conn.execute(f"""
        SELECT table_name, file_count, file_size_bytes
        FROM ducklake_table_info('{lake_name}')
        ORDER BY table_name
    """).fetchall()
    results["table_info"] = [(r[0], r[1], r[2]) for r in rows]

    # Q2: Row counts per table
    counts = []
    for tbl, _, _ in results["table_info"]:
        cnt = conn.execute(f'SELECT count(*) FROM {lake_name}."{tbl}"').fetchone()[0]
        counts.append((tbl, cnt))
    results["row_counts"] = counts

    # Q3: Sample data from xjfq-wh2d (sorted for determinism)
    rows = conn.execute(f"""
        SELECT name, type, license_number
        FROM {lake_name}."xjfq-wh2d"
        ORDER BY license_number
        LIMIT 5
    """).fetchall()
    results["sample_data"] = [tuple(r) for r in rows]

    # Q4: Snapshot count and IDs
    rows = conn.execute(f"""
        SELECT snapshot_id, schema_version, author
        FROM ducklake_snapshots('{lake_name}')
        ORDER BY snapshot_id
    """).fetchall()
    results["snapshots"] = [(r[0], r[1], r[2]) for r in rows]

    # Q5: File listing for a specific table
    rows = conn.execute(f"""
        SELECT data_file, data_file_size_bytes
        FROM ducklake_list_files('{lake_name}', 'xjfq-wh2d')
    """).fetchall()
    results["files"] = [(r[0], r[1]) for r in rows]

    # Q6: Table changes (PIT query — insertions between snapshots 0 and latest)
    max_snap = conn.execute(f"""
        SELECT max(snapshot_id)
        FROM ducklake_snapshots('{lake_name}')
    """).fetchone()[0]
    rows = conn.execute(f"""
        SELECT snapshot_id, change_type, license_number
        FROM ducklake_table_changes('{lake_name}', 'main', 'xjfq-wh2d', 0, {max_snap})
        ORDER BY license_number
        LIMIT 5
    """).fetchall()
    results["table_changes"] = [tuple(r) for r in rows]

    return results


def compare_results(backends, label):
    """Compare query results across backends. Returns True if all match."""
    ref_name = list(backends.keys())[0]
    ref_results = backends[ref_name]
    compare_names = [n for n in backends if n != ref_name]

    print(f"\n{'='*60}")
    print(f"{label}")
    print(f"Reference: {ref_name} — comparing: {', '.join(compare_names)}")
    print(f"{'='*60}")

    all_match = True
    for key in ref_results:
        ref_val = ref_results[key]
        matches = {}
        for name in compare_names:
            matches[name] = backends[name][key] == ref_val

        if all(matches.values()):
            print(f"\n  {key}: ALL MATCH")
            if isinstance(ref_val, list) and ref_val:
                print(f"    ({len(ref_val)} entries, e.g. {ref_val[0]})")
        else:
            all_match = False
            print(f"\n  {key}: MISMATCH")
            print(f"    {ref_name}: {ref_val[:3]}{'...' if len(ref_val) > 3 else ''}")
            for name in compare_names:
                status = "MATCH" if matches[name] else "DIFF"
                val = backends[name][key]
                print(f"    {name} ({status}): {val[:3]}{'...' if len(val) > 3 else ''}")

    print(f"\n{'='*60}")
    if all_match:
        print("ALL QUERIES MATCH.")
    else:
        print("MISMATCH DETECTED.")
    return all_match


def setup_catalog(label, engine, schema, datasets, data_path, dialect):
    """Create and populate a DuckLake catalog."""
    print(f"Setting up {label} ...", flush=True)
    if schema:
        with engine.connect() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            conn.commit()
    create_catalog(engine, schema=schema)
    populate_catalog_sa(engine, schema, datasets, data_path, dialect=dialect)


def main():
    print("=== DuckLake PIT Query Comparison ===", flush=True)
    print("  Catalog backends: DuckDB, SQLite, PostgreSQL", flush=True)
    print("  Storage backends: local filesystem, S3 (MinIO)\n", flush=True)

    datasets = load_datasets()
    print(f"Loaded {len(datasets)} datasets\n", flush=True)

    # Temp paths for file-based catalogs
    duck_local_path = tempfile.mktemp(suffix=".duckdb")
    duck_s3_path = tempfile.mktemp(suffix=".duckdb")
    sqlite_local_path = tempfile.mktemp(suffix=".sqlite")
    sqlite_s3_path = tempfile.mktemp(suffix=".sqlite")
    duck_local_schema = "ducklake_cat"
    duck_s3_schema = "ducklake_cat"

    # ── Phase 1: Local filesystem storage ─────────────────────────────
    print("── Phase 1: Local filesystem storage ──\n", flush=True)

    duck_engine = create_engine(f"duckdb:///{duck_local_path}")
    setup_catalog("DuckDB/local", duck_engine, duck_local_schema, datasets, LOCAL_DATA_PATH, "duckdb")
    duck_engine.dispose()

    sqlite_engine = create_engine(f"sqlite:///{sqlite_local_path}")
    setup_catalog("SQLite/local", sqlite_engine, None, datasets, LOCAL_DATA_PATH, "sqlite")
    sqlite_engine.dispose()

    pg_engine = create_engine(PG_URL)
    setup_catalog("PG/local", pg_engine, PG_SCHEMA_LOCAL, datasets, LOCAL_DATA_PATH, "postgresql")
    pg_engine.dispose()

    # ── Phase 2: S3 (MinIO) storage ──────────────────────────────────
    print("\n── Phase 2: S3 (MinIO) storage ──\n", flush=True)

    duck_engine = create_engine(f"duckdb:///{duck_s3_path}")
    setup_catalog("DuckDB/S3", duck_engine, duck_s3_schema, datasets, S3_DATA_PATH, "duckdb")
    duck_engine.dispose()

    sqlite_engine = create_engine(f"sqlite:///{sqlite_s3_path}")
    setup_catalog("SQLite/S3", sqlite_engine, None, datasets, S3_DATA_PATH, "sqlite")
    sqlite_engine.dispose()

    pg_engine = create_engine(PG_URL)
    setup_catalog("PG/S3", pg_engine, PG_SCHEMA_S3, datasets, S3_DATA_PATH, "postgresql")
    pg_engine.dispose()

    # ── Query all backends ────────────────────────────────────────────
    print("\nAttaching all backends to DuckLake facade ...\n", flush=True)

    conn = duckdb.connect()
    conn.execute("LOAD ducklake")
    conn.execute("LOAD postgres_scanner")

    # S3 secret for MinIO
    conn.execute(f"""
        CREATE SECRET (
            TYPE S3,
            KEY_ID '{MINIO_KEY}',
            SECRET '{MINIO_SECRET}',
            ENDPOINT '{MINIO_ENDPOINT}',
            USE_SSL false,
            URL_STYLE 'path'
        )
    """)

    # Local filesystem backends
    conn.execute(f"""
        ATTACH 'ducklake:{duck_local_path}' AS lake_duck_local (
            DATA_PATH '{LOCAL_DATA_PATH}',
            METADATA_SCHEMA '{duck_local_schema}'
        )
    """)
    conn.execute(f"""
        ATTACH 'ducklake:sqlite:{sqlite_local_path}' AS lake_sqlite_local (
            DATA_PATH '{LOCAL_DATA_PATH}'
        )
    """)
    conn.execute(f"""
        ATTACH 'ducklake:postgres:dbname=rule4_test host=localhost gssencmode=disable' AS lake_pg_local (
            DATA_PATH '{LOCAL_DATA_PATH}',
            METADATA_SCHEMA '{PG_SCHEMA_LOCAL}'
        )
    """)

    # S3 (MinIO) storage backends
    conn.execute(f"""
        ATTACH 'ducklake:{duck_s3_path}' AS lake_duck_s3 (
            DATA_PATH '{S3_DATA_PATH}',
            METADATA_SCHEMA '{duck_s3_schema}'
        )
    """)
    conn.execute(f"""
        ATTACH 'ducklake:sqlite:{sqlite_s3_path}' AS lake_sqlite_s3 (
            DATA_PATH '{S3_DATA_PATH}'
        )
    """)
    conn.execute(f"""
        ATTACH 'ducklake:postgres:dbname=rule4_test host=localhost gssencmode=disable' AS lake_pg_s3 (
            DATA_PATH '{S3_DATA_PATH}',
            METADATA_SCHEMA '{PG_SCHEMA_S3}'
        )
    """)

    # Run queries on all six combinations
    print("Running queries ...\n", flush=True)

    local_results = {}
    for name, lake in [("DuckDB", "lake_duck_local"), ("SQLite", "lake_sqlite_local"), ("PostgreSQL", "lake_pg_local")]:
        local_results[name] = run_queries(conn, lake)

    s3_results = {}
    for name, lake in [("DuckDB", "lake_duck_s3"), ("SQLite", "lake_sqlite_s3"), ("PostgreSQL", "lake_pg_s3")]:
        s3_results[name] = run_queries(conn, lake)

    conn.close()

    # ── Compare ───────────────────────────────────────────────────────
    ok1 = compare_results(local_results, "LOCAL FILESYSTEM — 3 catalog backends")
    ok2 = compare_results(s3_results, "S3 (MinIO) — 3 catalog backends")

    # Cross-storage comparison: DuckDB/local vs DuckDB/S3
    # Files query will differ (absolute paths vs s3:// paths), so skip it
    print(f"\n{'='*60}")
    print("CROSS-STORAGE — local vs S3 (excluding file paths)")
    print(f"{'='*60}")

    cross_ok = True
    for key in local_results["DuckDB"]:
        if key == "files":
            print(f"\n  {key}: SKIPPED (paths differ by design)")
            continue
        local_val = local_results["DuckDB"][key]
        s3_val = s3_results["DuckDB"][key]
        match = local_val == s3_val
        if match:
            print(f"\n  {key}: MATCH")
        else:
            cross_ok = False
            print(f"\n  {key}: DIFF")
            print(f"    local: {local_val[:3]}{'...' if len(local_val) > 3 else ''}")
            print(f"    S3:    {s3_val[:3]}{'...' if len(s3_val) > 3 else ''}")

    print(f"\n{'='*60}")
    if cross_ok:
        print("CROSS-STORAGE MATCH — local and S3 produce identical query results.")
    else:
        print("CROSS-STORAGE MISMATCH.")

    # ── Cleanup ───────────────────────────────────────────────────────
    for path in [duck_local_path, duck_s3_path]:
        for suffix in ["", ".wal"]:
            p = path + suffix
            if os.path.exists(p):
                os.unlink(p)
    for path in [sqlite_local_path, sqlite_s3_path]:
        if os.path.exists(path):
            os.unlink(path)

    all_ok = ok1 and ok2 and cross_ok
    print(f"\n{'#'*60}")
    if all_ok:
        print("ALL TESTS PASSED — 3 catalog backends x 2 storage backends = identical results.")
    else:
        print("SOME TESTS FAILED.")
        sys.exit(1)


if __name__ == "__main__":
    main()
