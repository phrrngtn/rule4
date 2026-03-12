"""
Stream Socrata datasets into MinIO as Parquet via DuckDB.

Pipeline per dataset: curl TSV | DuckDB read_csv('/dev/stdin') -> COPY TO s3://

No intermediate files on disk. Parallelized with concurrent.futures.

Usage:
    cd sql/ducklake/experiment_pg
    uv run python ingest_to_minio.py
"""

import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))

EXPERIMENT_DIR = Path(__file__).resolve().parent.parent / "experiment"
CATALOG_JSON = EXPERIMENT_DIR / "raw" / "data.cityofnewyork.us" / "catalog.json"

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_KEY = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
S3_BUCKET = "ducklake-data"
S3_PREFIX = "main"

# Large datasets get more rows
LARGE_IDS = {"erm2-nwe9", "pvqr-7yc4"}
DEFAULT_LIMIT = 50000
LARGE_LIMIT = 500000

WORKERS = 4


def ingest_one(ds_id, limit):
    """Stream one dataset: curl TSV -> DuckDB -> Parquet on MinIO."""
    url = f"https://data.cityofnewyork.us/resource/{ds_id}.tsv?$limit={limit}"
    s3_path = f"s3://{S3_BUCKET}/{S3_PREFIX}/{ds_id}/data_0.parquet"

    duckdb_sql = f"""
        CREATE SECRET (TYPE S3, KEY_ID '{MINIO_KEY}', SECRET '{MINIO_SECRET}',
                       ENDPOINT '{MINIO_ENDPOINT}', USE_SSL false, URL_STYLE 'path');
        COPY (
            SELECT * FROM read_csv('/dev/stdin', delim='\t', header=true,
                                   all_varchar=true, ignore_errors=true)
        ) TO '{s3_path}' (FORMAT PARQUET);
        SELECT count(*) AS cnt FROM read_parquet('{s3_path}');
    """

    curl = subprocess.Popen(
        ["curl", "-sf", "--max-time", "120", url],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    duck = subprocess.Popen(
        ["uv", "run", "python3", "-c",
         f"import duckdb; conn = duckdb.connect(); "
         f"conn.execute(\"\"\"CREATE SECRET (TYPE S3, KEY_ID '{MINIO_KEY}', SECRET '{MINIO_SECRET}', "
         f"ENDPOINT '{MINIO_ENDPOINT}', USE_SSL false, URL_STYLE 'path')\"\"\"); "
         f"conn.execute(\"\"\"COPY (SELECT * FROM read_csv('/dev/stdin', delim='\\t', header=true, "
         f"all_varchar=true, ignore_errors=true)) TO '{s3_path}' (FORMAT PARQUET)\"\"\"); "
         f"r = conn.execute(\"SELECT count(*) FROM read_parquet('{s3_path}')\").fetchone(); "
         f"print(r[0]); conn.close()"],
        stdin=curl.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    curl.stdout.close()

    out, err = duck.communicate(timeout=180)
    curl.wait()

    if duck.returncode != 0:
        return ds_id, 0, 0, err.decode()[:200]

    cnt = int(out.decode().strip())
    # Get file size from MinIO
    mc_out = subprocess.run(
        ["mc", "stat", "--json", f"local/{S3_BUCKET}/{S3_PREFIX}/{ds_id}/data_0.parquet"],
        capture_output=True, text=True,
    )
    size = 0
    if mc_out.returncode == 0:
        import json as _json
        try:
            size = _json.loads(mc_out.stdout).get("size", 0)
        except Exception:
            pass

    return ds_id, cnt, size, None


def main():
    with open(CATALOG_JSON) as f:
        catalog = json.load(f)

    datasets = []
    for r in catalog["results"]:
        ds_id = r["resource"]["id"]
        limit = LARGE_LIMIT if ds_id in LARGE_IDS else DEFAULT_LIMIT
        datasets.append((ds_id, r["resource"]["name"][:50], limit))

    print(f"Ingesting {len(datasets)} datasets -> MinIO s3://{S3_BUCKET}/{S3_PREFIX}/")
    print(f"  {WORKERS} parallel workers, default {DEFAULT_LIMIT} rows, large {LARGE_LIMIT} rows\n")

    t_start = time.time()
    total_rows = 0
    total_bytes = 0
    ok = 0
    fail = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {}
        for ds_id, name, limit in datasets:
            f = pool.submit(ingest_one, ds_id, limit)
            futures[f] = (ds_id, name, limit)

        for f in as_completed(futures):
            ds_id, name, limit = futures[f]
            done = ok + fail + 1
            try:
                rid, cnt, size, err = f.result()
                if err:
                    fail += 1
                    print(f"  [{done:>3}/{len(datasets)}] {rid} FAIL: {err[:80]}", flush=True)
                else:
                    ok += 1
                    total_rows += cnt
                    total_bytes += size
                    if done % 10 == 0 or limit == LARGE_LIMIT:
                        elapsed = time.time() - t_start
                        print(f"  [{done:>3}/{len(datasets)}] {rid} {cnt:>7,} rows {size/1024/1024:>5.1f}MB  {name}  [{elapsed:.0f}s]", flush=True)
            except Exception as e:
                fail += 1
                print(f"  [{done:>3}/{len(datasets)}] {ds_id} EXCEPTION: {e}", flush=True)

    elapsed = time.time() - t_start
    print(f"\n{'='*60}")
    print(f"Done: {ok} ok, {fail} fail")
    print(f"  {total_rows:,} total rows, {total_bytes/1024/1024:.0f}MB on MinIO")
    print(f"  {elapsed:.0f}s elapsed")


if __name__ == "__main__":
    main()
