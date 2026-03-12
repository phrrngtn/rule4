"""
Full DuckLake pipeline with PostgreSQL catalog backend.

1. Create DuckLake catalog tables in PG via SQLAlchemy
2. Populate metadata from Socrata (same 5 NYC datasets as experiment/)
3. Register Parquet data files (reusing existing ones from experiment/)
4. Query through DuckLake facade via ducklake:postgres: ATTACH

Usage:
    cd sql/ducklake/experiment_pg
    uv run python populate_and_query.py
"""

import json
import struct
import os
import sys
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))

import duckdb
from sqlalchemy import create_engine, text
from rule4.ducklake_catalog import create_catalog, DUCKLAKE_VERSION

PG_URL = os.environ.get("PG_URL", "postgresql://localhost/rule4_test?gssencmode=disable")
SCHEMA = "ducklake"
EXPERIMENT_DIR = Path(__file__).resolve().parent.parent / "experiment"
DATA_PATH = str(EXPERIMENT_DIR / "data") + "/"

# Absolute path so DuckLake can find the Parquet files
CATALOG_JSON = EXPERIMENT_DIR / "raw" / "data.cityofnewyork.us" / "catalog.json"


def main():
    print("=== DuckLake PostgreSQL Backend Experiment ===\n", flush=True)

    # ── 1. Create catalog schema in PG ───────────────────────────────
    print("1. Creating catalog schema in PostgreSQL ...", flush=True)
    engine = create_engine(PG_URL)
    with engine.connect() as conn:
        conn.execute(text(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE'))
        conn.commit()

    meta = create_catalog(engine, schema=SCHEMA)
    print(f"   {len(meta.tables)} tables created in schema '{SCHEMA}'", flush=True)

    # ── 2. Populate metadata from Socrata ────────────────────────────
    print("\n2. Populating catalog metadata ...", flush=True)

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
            "datatypes": res.get("columns_datatype", []),
        })

    print(f"   {len(datasets)} datasets from catalog", flush=True)

    with engine.connect() as conn:
        # Bootstrap: snapshot 0, schema 'main', metadata entries
        conn.execute(text(f"""
            INSERT INTO "{SCHEMA}".ducklake_metadata (key, value)
            VALUES
                ('version', '{DUCKLAKE_VERSION}'),
                ('created_by', 'rule4 SA model'),
                ('data_path', '{DATA_PATH}'),
                ('encrypted', 'false')
        """))

        conn.execute(text(f"""
            INSERT INTO "{SCHEMA}".ducklake_snapshot
            (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
            VALUES (0, NOW(), 0, 1, 0)
        """))

        conn.execute(text(f"""
            INSERT INTO "{SCHEMA}".ducklake_schema
            (schema_id, schema_uuid, begin_snapshot, end_snapshot, schema_name, path, path_is_relative)
            VALUES (0, '{uuid4()}', 0, NULL, 'main', 'main/', true)
        """))

        conn.execute(text(f"""
            INSERT INTO "{SCHEMA}".ducklake_snapshot_changes
            (snapshot_id, changes_made, author, commit_message, commit_extra_info)
            VALUES (0, 'created_schema:"main"', NULL, NULL, NULL)
        """))

        conn.execute(text(f"""
            INSERT INTO "{SCHEMA}".ducklake_schema_versions
            (begin_snapshot, schema_version, table_id)
            VALUES (0, 0, NULL)
        """))

        conn.commit()

        # Allocate IDs for tables and columns
        next_cat_id = 1
        next_snapshot = 1
        schema_ver = 1

        table_ids = {}  # dataset_id -> table_id
        all_columns = []  # (table_id, snapshot_id, col_order, field_name)

        for ds in datasets:
            table_id = next_cat_id
            next_cat_id += 1
            snapshot_id = next_snapshot
            next_snapshot += 1
            table_ids[ds["id"]] = (table_id, snapshot_id)

            for i, fname in enumerate(ds["field_names"]):
                col_id = next_cat_id
                next_cat_id += 1
                all_columns.append((col_id, table_id, snapshot_id, i + 1, fname))

        # Insert snapshots (one per table creation)
        for ds in datasets:
            table_id, snapshot_id = table_ids[ds["id"]]
            ts = ds["updated_at"] or "2024-01-01T00:00:00Z"
            conn.execute(text(f"""
                INSERT INTO "{SCHEMA}".ducklake_snapshot
                (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
                VALUES ({snapshot_id}, '{ts}'::timestamptz, {schema_ver}, {next_cat_id}, 0)
            """))
            schema_ver += 1

        # Schema versions
        for ds in datasets:
            _, snapshot_id = table_ids[ds["id"]]
            conn.execute(text(f"""
                INSERT INTO "{SCHEMA}".ducklake_schema_versions
                (begin_snapshot, schema_version, table_id)
                VALUES ({snapshot_id}, {snapshot_id}, NULL)
            """))

        # Tables
        for ds in datasets:
            table_id, snapshot_id = table_ids[ds["id"]]
            conn.execute(text(f"""
                INSERT INTO "{SCHEMA}".ducklake_table
                (table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, table_name, path, path_is_relative)
                VALUES ({table_id}, '{uuid4()}', {snapshot_id}, NULL, 0, '{ds["id"]}', '{ds["id"]}/', true)
            """))

        # Columns (all varchar, matching the experiment)
        for col_id, table_id, snapshot_id, col_order, fname in all_columns:
            safe_fname = fname.replace("'", "''")
            conn.execute(text(f"""
                INSERT INTO "{SCHEMA}".ducklake_column
                (column_id, begin_snapshot, end_snapshot, table_id, column_order,
                 column_name, column_type, initial_default, default_value,
                 nulls_allowed, parent_column, default_value_type, default_value_dialect)
                VALUES ({col_id}, {snapshot_id}, NULL, {table_id}, {col_order},
                        '{safe_fname}', 'varchar', NULL, NULL,
                        true, NULL, NULL, NULL)
            """))

        # Snapshot changes
        for ds in datasets:
            table_id, snapshot_id = table_ids[ds["id"]]
            conn.execute(text(f"""
                INSERT INTO "{SCHEMA}".ducklake_snapshot_changes
                (snapshot_id, changes_made, author, commit_message, commit_extra_info)
                VALUES ({snapshot_id},
                        'created_table:"main"."{ds["id"]}"',
                        'rule4_socrata_sync',
                        'Socrata import: {ds["name"].replace("'", "''")}',
                        '{{"source":"socrata","dataset_id":"{ds["id"]}"}}')
            """))

        conn.commit()
        print(f"   {len(datasets)} tables, {len(all_columns)} columns registered", flush=True)

    # ── 3. Register Parquet data files ───────────────────────────────
    print("\n3. Registering Parquet data files ...", flush=True)

    next_file_id = 0
    file_records = []

    for ds in datasets:
        parquet_path = EXPERIMENT_DIR / "data" / "main" / ds["id"] / "data_0.parquet"
        if not parquet_path.exists():
            print(f"   SKIP {ds['id']}: no Parquet file", flush=True)
            continue

        file_size = parquet_path.stat().st_size
        with open(parquet_path, "rb") as f:
            f.seek(-8, 2)
            footer_size = struct.unpack("<i", f.read(4))[0]

        table_id, _ = table_ids[ds["id"]]
        file_records.append((next_file_id, table_id, ds["id"], file_size, footer_size))
        next_file_id += 1

    with engine.connect() as conn:
        for file_id, table_id, dataset_id, file_size, footer_size in file_records:
            snap_id = next_snapshot
            next_snapshot += 1

            conn.execute(text(f"""
                INSERT INTO "{SCHEMA}".ducklake_snapshot
                (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
                VALUES ({snap_id}, NOW(), {schema_ver - 1}, {next_cat_id}, {file_id + 1})
            """))

            conn.execute(text(f"""
                INSERT INTO "{SCHEMA}".ducklake_snapshot_changes
                (snapshot_id, changes_made, author, commit_message, commit_extra_info)
                VALUES ({snap_id}, 'inserted_into_table:{table_id}',
                        'rule4_socrata_sync', 'Load data for {dataset_id}', NULL)
            """))

            conn.execute(text(f"""
                INSERT INTO "{SCHEMA}".ducklake_data_file
                (data_file_id, table_id, begin_snapshot, end_snapshot, file_order,
                 path, path_is_relative, file_format, record_count,
                 file_size_bytes, footer_size, row_id_start, partition_id,
                 encryption_key, mapping_id, partial_max)
                VALUES ({file_id}, {table_id}, {snap_id}, NULL, NULL,
                        'data_0.parquet', true, 'parquet', 500,
                        {file_size}, {footer_size}, 0, NULL,
                        NULL, NULL, NULL)
            """))

        conn.commit()

    print(f"   {len(file_records)} Parquet files registered", flush=True)
    engine.dispose()

    # ── 4. Query through DuckLake facade ─────────────────────────────
    print("\n4. Querying through DuckLake facade ...", flush=True)

    conn = duckdb.connect()
    conn.execute("LOAD ducklake")
    conn.execute("LOAD postgres_scanner")

    pg_conn_str = f"dbname=rule4_test host=localhost gssencmode=disable"
    attach_uri = f"ducklake:postgres:{pg_conn_str}"

    conn.execute(f"""
        ATTACH '{attach_uri}' AS lake (
            DATA_PATH '{DATA_PATH}',
            METADATA_SCHEMA '{SCHEMA}'
        )
    """)

    print("\n   --- Table discovery ---")
    rows = conn.execute("SELECT table_name, file_count, file_size_bytes FROM ducklake_table_info('lake')").fetchall()
    for name, fc, sz in rows:
        print(f"   {name:20s}  files={fc}  size={sz}")

    print("\n   --- Sample data (FHV Active Drivers) ---")
    rows = conn.execute("""
        SELECT name, type, license_number
        FROM lake."xjfq-wh2d"
        LIMIT 5
    """).fetchall()
    for row in rows:
        print(f"   {row}")

    print("\n   --- Row counts ---")
    for ds in datasets:
        try:
            cnt = conn.execute(f'SELECT count(*) FROM lake."{ds["id"]}"').fetchone()[0]
            print(f"   {ds['id']}: {cnt} rows")
        except Exception as e:
            print(f"   {ds['id']}: ERROR {e}")

    print("\n   --- Snapshot history ---")
    rows = conn.execute("""
        SELECT snapshot_id, snapshot_time, schema_version, author, commit_message
        FROM ducklake_snapshots('lake')
    """).fetchall()
    for row in rows:
        print(f"   {row}")

    conn.close()
    print("\n=== All tests passed ===")


if __name__ == "__main__":
    main()
