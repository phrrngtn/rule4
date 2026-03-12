"""
Incremental sync from Socrata to MinIO Parquet, DuckLake-native.

Each sync produces a new Parquet delta file registered as a DuckLake snapshot.
PIT time travel works because each snapshot references only the files visible
at that point in time. No files are overwritten or consolidated.

System fields (maintained by Socrata, not user payload):
  :id          — stable row identifier, used as UPSERT key at query time
  :updated_at  — when Socrata last touched the row

Delta sync workflow:
  1. Query local data for max(:updated_at) → high-water mark (hwm)
  2. Fetch rows from Socrata where :updated_at > hwm
  3. Write delta as a new Parquet file on MinIO
  4. Register the file in DuckLake as a new snapshot with
     snapshot_time = max(:updated_at) from the batch (pessimistic)

Current-state queries resolve duplicates at read time:
  SELECT * FROM (
      SELECT *, ROW_NUMBER() OVER (
          PARTITION BY ":id" ORDER BY ":updated_at" DESC
      ) AS _rn FROM dataset
  ) WHERE _rn = 1

This is merge-on-read, same as Iceberg. DuckLake accumulates delta files;
PIT queries see only the files registered up to the requested snapshot.

Strategies per dataset:
  - INCREMENTAL: :updated_at spans days/years → delta sync viable
  - FULL_REFRESH: :updated_at span < 1 hour → publisher does bulk replace
  - INITIAL: no data on MinIO yet → full fetch as first snapshot

Usage:
    cd sql/ducklake/experiment_pg
    uv run python incremental_sync.py                    # dry-run: show plan
    uv run python incremental_sync.py --execute          # execute sync
    uv run python incremental_sync.py --execute --full   # force full refresh
    uv run python incremental_sync.py --execute --ids erm2-nwe9 ic3t-wcy2
"""

import json
import os
import subprocess
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

EXPERIMENT_DIR = Path(__file__).resolve().parent.parent / "experiment"
CATALOG_JSON = EXPERIMENT_DIR / "raw" / "data.cityofnewyork.us" / "catalog.json"
RESOURCES_JSON = Path(__file__).resolve().parent / "resources.json"

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_KEY = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
S3_BUCKET = "ducklake-data"
S3_PREFIX = "main"

LARGE_IDS = {"erm2-nwe9", "pvqr-7yc4"}
DEFAULT_LIMIT = 50000
LARGE_LIMIT = 500000

# :updated_at span threshold: below this = bulk replace, above = incremental
INCREMENTAL_SPAN_THRESHOLD = timedelta(hours=1)

WORKERS = 4


def _bump_ts(ts_str, seconds=1):
    """Add seconds to an ISO timestamp string, return as ISO string."""
    # Handle both 'Z' suffix and '+00:00' timezone formats
    cleaned = ts_str.replace("Z", "+00:00")
    dt = datetime.fromisoformat(cleaned)
    bumped = dt + timedelta(seconds=seconds)
    # Return in the same format Socrata uses
    return bumped.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _s3_secret_sql():
    return (f"CREATE SECRET (TYPE S3, KEY_ID '{MINIO_KEY}', SECRET '{MINIO_SECRET}', "
            f"ENDPOINT '{MINIO_ENDPOINT}', USE_SSL false, URL_STYLE 'path')")


def _socrata_api(domain, ds_id, select, where=None, limit=1):
    """Query Socrata SODA2 API. Returns parsed JSON list."""
    url = (f"https://{domain}/resource/{ds_id}.json"
           f"?$select={urllib.request.quote(select)}&$limit={limit}")
    if where:
        url += f"&$where={urllib.request.quote(where)}"
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.loads(resp.read())


def classify_dataset(domain, ds_id):
    """Determine sync strategy by probing :updated_at span on Socrata."""
    try:
        data = _socrata_api(domain, ds_id,
            "min(:updated_at) as min_upd, max(:updated_at) as max_upd, count(:updated_at) as total")
        d = data[0]
        min_upd = datetime.fromisoformat(d["min_upd"].replace("Z", "+00:00"))
        max_upd = datetime.fromisoformat(d["max_upd"].replace("Z", "+00:00"))
        total = int(d["total"])
        span = max_upd - min_upd
        strategy = "INCREMENTAL" if span > INCREMENTAL_SPAN_THRESHOLD else "FULL_REFRESH"
        return ds_id, strategy, min_upd, max_upd, total, span
    except Exception as e:
        return ds_id, f"ERROR: {e}", None, None, 0, timedelta(0)


def get_local_state(conn, ds_id):
    """Get local state from Parquet files on MinIO.

    Returns (hwm_str_or_None, row_count, has_id_col, n_files).
    hwm is max(:updated_at) across all files if the column exists.
    """
    base = f"s3://{S3_BUCKET}/{S3_PREFIX}/{ds_id}/"
    try:
        files = conn.execute(
            f"SELECT file FROM glob('{base}*.parquet')").fetchall()
        if not files:
            return None, 0, False, 0

        file_list = [f[0] for f in files]
        cols = conn.execute(
            f"SELECT name FROM parquet_schema('{file_list[0]}')").fetchall()
        col_names = {c[0] for c in cols}

        total_rows = sum(
            conn.execute(f"SELECT count(*) FROM read_parquet('{f}')").fetchone()[0]
            for f in file_list
        )

        has_id = ":id" in col_names
        hwm = None
        if ":updated_at" in col_names:
            for f in file_list:
                # Use ORDER BY DESC LIMIT 1 instead of max() — DuckDB's max()
                # on varchar Parquet columns can return truncated values from
                # Parquet statistics instead of reading actual data.
                r = conn.execute(
                    f'SELECT ":updated_at" FROM read_parquet(\'{f}\') '
                    f'ORDER BY ":updated_at" DESC LIMIT 1').fetchone()[0]
                if r and (hwm is None or r > hwm):
                    hwm = r

        return hwm, total_rows, has_id, len(file_list)
    except Exception:
        return None, 0, False, 0


def _stream_to_s3(domain, ds_id, s3_path, where_clause, limit):
    """Stream TSV from Socrata to a Parquet file on MinIO.

    Runs a single Python subprocess that:
      1. urllib fetches TSV (no curl, no shell quoting issues)
      2. Writes TSV to a pipe fd
      3. DuckDB reads from pipe fd, writes Parquet to S3

    Always includes :id and :updated_at system fields.
    Returns (row_count, max_updated_at_str, error_str_or_None).
    """
    import tempfile

    # Pass params as JSON env var — completely avoids quoting hell
    params = json.dumps({
        "ds_id": ds_id, "domain": domain, "s3_path": s3_path, "limit": limit,
        "where_clause": where_clause or "",
        "s3_secret_sql": _s3_secret_sql(),
    })
    worker_path = Path(__file__).resolve().parent / "_sync_worker.py"

    env = os.environ.copy()
    env["SYNC_PARAMS"] = params

    proc = subprocess.Popen(
        ["uv", "run", "--quiet", "python3", str(worker_path)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env,
    )
    out, err = proc.communicate(timeout=600)

    if proc.returncode != 0:
        return 0, None, err.decode()[:300]

    # Output format: count|hwm
    parts = out.decode().strip().split("|", 1)
    cnt = int(parts[0])
    hwm = parts[1] if len(parts) > 1 and parts[1] != "None" else None
    return cnt, hwm, None


def sync_one(domain, ds_id, action, limit, hwm=None):
    """Sync a single dataset.

    For FULL: write as data_0.parquet (initial load or bulk replace).
    For DELTA: fetch rows WHERE :updated_at >= hwm ORDER BY :updated_at ASC
               LIMIT {limit}. The >= ensures we don't miss rows sharing the
               hwm timestamp; the overlap rows (at least one) become UPDATEs
               when resolved at query time via :id dedup.
               Written as a new delta file — DuckLake accumulates these for PIT.

    Current-state query resolves duplicates across all files:
        SELECT * EXCLUDE (_rn) FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY ":id" ORDER BY ":updated_at" DESC
            ) AS _rn FROM read_parquet('s3://.../{ds_id}/*.parquet')
        ) WHERE _rn = 1

    Returns (ds_id, action, rows_fetched, new_hwm, error).
    """
    if action == "DELTA" and hwm:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        filename = f"data_delta_{ts}"
        s3_path = f"s3://{S3_BUCKET}/{S3_PREFIX}/{ds_id}/{filename}.parquet"

        # Half-open interval [hwm, now()-1s) — catches up to the present in
        # one fetch. The -1s avoids a race with rows Socrata is still writing.
        # >= hwm re-fetches boundary rows (resolved as UPDATEs via :id dedup).
        hwm_upper = (datetime.now(timezone.utc) - timedelta(seconds=1)).strftime(
            "%Y-%m-%dT%H:%M:%S.000Z")
        where = f":updated_at >= '{hwm}' AND :updated_at < '{hwm_upper}'"
        # No row limit — the time window is the bound
        # Stage to a temp location, then prune no-op updates before
        # writing the real delta file.
        staging_path = f"s3://{S3_BUCKET}/staging/{ds_id}/{filename}.parquet"
        cnt, new_hwm, err = _stream_to_s3(domain, ds_id, staging_path, where, 1000000)
        if err:
            return ds_id, "DELTA", 0, None, err
        if cnt == 0:
            _delete_s3(staging_path)
            return ds_id, "DELTA", 0, hwm, None

        # Prune no-op updates: JOIN staging against existing on :id,
        # keep only genuinely new or changed rows.
        real_cnt = _prune_and_promote(ds_id, staging_path, s3_path)
        _delete_s3(staging_path)

        if real_cnt == 0:
            return ds_id, "DELTA", 0, new_hwm, None

        return ds_id, "DELTA", real_cnt, new_hwm, None
    else:
        s3_path = f"s3://{S3_BUCKET}/{S3_PREFIX}/{ds_id}/data_0.parquet"
        cnt, new_hwm, err = _stream_to_s3(domain, ds_id, s3_path, None, limit)
        if err:
            return ds_id, "FULL", 0, None, err
        return ds_id, "FULL", cnt, new_hwm, None


def _prune_and_promote(ds_id, staging_path, dest_path):
    """Compare staged delta against existing data, write only real changes.

    JOINs staging against existing on :id (Socrata PK). A row survives if:
      (a) its :id is new (LEFT JOIN miss → INSERT), or
      (b) its :id exists but non-timestamp columns differ (genuine UPDATE).

    Rows where :id matches and all non-timestamp columns are identical
    (timestamp-only refresh) are pruned.

    Returns row count written to dest_path, or 0 if nothing to write.
    """
    import duckdb

    base = f"s3://{S3_BUCKET}/{S3_PREFIX}/{ds_id}"
    existing_glob = f"{base}/data_*.parquet"

    conn = duckdb.connect()
    conn.execute(_s3_secret_sql())

    try:
        all_files = conn.execute(
            f"SELECT file FROM glob('{existing_glob}')").fetchall()
        existing_files = [f[0] for f in all_files]

        if not existing_files:
            # No prior data — promote staging as-is
            cnt = conn.execute(
                f"SELECT count(*) FROM read_parquet('{staging_path}')").fetchone()[0]
            conn.execute(
                f"COPY (SELECT * FROM read_parquet('{staging_path}'))"
                f" TO '{dest_path}' (FORMAT PARQUET)")
            return cnt

        # Get columns excluding :updated_at and virtual columns for value comparison
        VIRTUAL_COLS = {":updated_at", "duckdb_schema"}
        cols = conn.execute(
            f"SELECT name FROM parquet_schema('{staging_path}')").fetchall()
        compare_cols = [c[0] for c in cols if c[0] not in VIRTUAL_COLS]

        # Build IS DISTINCT FROM comparison on each non-timestamp column.
        # If ANY column differs, the row is a genuine update.
        value_differs = " OR ".join(
            f'staging."{c}" IS DISTINCT FROM existing."{c}"'
            for c in compare_cols
        )

        # Existing data: read all files, deduplicate to latest version per :id
        existing_union = " UNION ALL ".join(
            f"SELECT * FROM read_parquet('{f}')" for f in existing_files)
        existing_cte = f"""
            DEDUPED AS (
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY ":id" ORDER BY ":updated_at" DESC
                    ) AS _rn
                    FROM ({existing_union})
                ) WHERE _rn = 1
            )
        """

        survivors_sql = f"""
            WITH {existing_cte}
            SELECT staging.*
            FROM read_parquet('{staging_path}') AS staging
            LEFT OUTER JOIN DEDUPED AS existing
                ON staging.":id" = existing.":id"
            WHERE existing.":id" IS NULL
               OR ({value_differs})
        """

        cnt = conn.execute(
            f"SELECT count(*) FROM ({survivors_sql})").fetchone()[0]

        if cnt == 0:
            return 0

        conn.execute(
            f"COPY ({survivors_sql}) TO '{dest_path}' (FORMAT PARQUET)")
        return cnt

    except Exception as e:
        # On error, promote staging as-is — duplicates are harmless,
        # resolved at query time via :id dedup
        print(f"    PRUNE WARNING for {ds_id}: {e}", flush=True)
        try:
            conn.execute(
                f"COPY (SELECT * FROM read_parquet('{staging_path}'))"
                f" TO '{dest_path}' (FORMAT PARQUET)")
            return conn.execute(
                f"SELECT count(*) FROM read_parquet('{dest_path}')").fetchone()[0]
        except Exception:
            return 0
    finally:
        conn.close()


def _delete_s3(s3_path):
    """Delete a file from MinIO via mc."""
    path = s3_path.replace("s3://", "")
    subprocess.run(["mc", "rm", f"local/{path}"], capture_output=True, timeout=10)


def _load_resources(only_ids=None):
    """Load resource list from resources.json (multi-domain) or fall back to catalog.json.

    Returns list of dicts with keys: domain, ds_id, name.
    """
    if RESOURCES_JSON.exists():
        with open(RESOURCES_JSON) as f:
            resources = json.load(f)
    elif CATALOG_JSON.exists():
        # Legacy single-domain catalog
        with open(CATALOG_JSON) as f:
            catalog = json.load(f)
        resources = [
            {"domain": "data.cityofnewyork.us",
             "ds_id": r["resource"]["id"],
             "name": r["resource"]["name"][:50]}
            for r in catalog["results"]
        ]
    else:
        raise FileNotFoundError(f"No resource list found at {RESOURCES_JSON} or {CATALOG_JSON}")

    if only_ids:
        resources = [r for r in resources if r["ds_id"] in only_ids]
    return resources


def plan_sync(force_full=False, only_ids=None):
    """Build sync plan for all datasets."""
    import duckdb

    resources = _load_resources(only_ids)

    print(f"Classifying {len(resources)} datasets...")
    classifications = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(classify_dataset, r["domain"], r["ds_id"]): r["ds_id"]
                   for r in resources}
        for f in as_completed(futures):
            ds_id, strategy, min_upd, max_upd, remote_total, span = f.result()
            classifications[ds_id] = {
                "strategy": strategy, "min_upd": min_upd,
                "max_upd": max_upd, "remote_total": remote_total, "span": span,
            }

    ds_domains = {r["ds_id"]: r["domain"] for r in resources}
    ds_names = {r["ds_id"]: r["name"] for r in resources}

    print("Checking local state on MinIO...")
    conn = duckdb.connect()
    conn.execute(_s3_secret_sql())

    plan = []
    for r in resources:
        ds_id = r["ds_id"]
        domain = ds_domains[ds_id]
        c = classifications[ds_id]
        name = ds_names.get(ds_id, ds_id)

        if c["strategy"].startswith("ERROR"):
            plan.append({"ds_id": ds_id, "domain": domain, "name": name,
                         "action": "SKIP", "reason": c["strategy"]})
            continue

        hwm, local_rows, has_id, n_files = get_local_state(conn, ds_id)
        limit = LARGE_LIMIT if ds_id in LARGE_IDS else DEFAULT_LIMIT

        if force_full or local_rows == 0:
            plan.append({"ds_id": ds_id, "domain": domain, "name": name,
                         "action": "FULL", "limit": limit,
                         "local_rows": local_rows, "remote_total": c["remote_total"],
                         "n_files": n_files,
                         "reason": "forced" if force_full else "initial load"})

        elif c["strategy"] == "FULL_REFRESH":
            # For bulk-replace datasets, compare remote max(:updated_at)
            # against local hwm. If identical, nothing has changed — skip.
            # remote max_upd is datetime, hwm is ISO string from Parquet.
            remote_max_str = (c["max_upd"].strftime("%Y-%m-%dT%H:%M:%S")
                              if c["max_upd"] else None)
            local_hwm_str = hwm[:19] if hwm else None
            if local_hwm_str and remote_max_str and local_hwm_str == remote_max_str:
                plan.append({"ds_id": ds_id, "domain": domain, "name": name,
                             "action": "SKIP",
                             "local_rows": local_rows, "remote_total": c["remote_total"],
                             "n_files": n_files,
                             "reason": f"unchanged (remote max={remote_max_str})"})
            else:
                plan.append({"ds_id": ds_id, "domain": domain, "name": name,
                             "action": "FULL", "limit": limit,
                             "local_rows": local_rows, "remote_total": c["remote_total"],
                             "n_files": n_files,
                             "reason": f"bulk replace (span {c['span']})"})

        elif c["strategy"] == "INCREMENTAL" and hwm and has_id:
            # Same check: if remote max == local hwm, nothing new
            remote_max_str2 = (c["max_upd"].strftime("%Y-%m-%dT%H:%M:%S")
                               if c["max_upd"] else None)
            local_hwm_str2 = hwm[:19] if hwm else None
            if local_hwm_str2 and remote_max_str2 and local_hwm_str2 == remote_max_str2:
                plan.append({"ds_id": ds_id, "domain": domain, "name": name,
                             "action": "SKIP",
                             "local_rows": local_rows, "remote_total": c["remote_total"],
                             "n_files": n_files,
                             "reason": f"unchanged (remote max={remote_max_str2})"})
            else:
                plan.append({"ds_id": ds_id, "domain": domain, "name": name,
                             "action": "DELTA", "limit": limit,
                             "hwm": hwm, "local_rows": local_rows,
                             "remote_total": c["remote_total"],
                             "n_files": n_files,
                             "reason": f"delta from hwm={hwm[:19]}"})

        else:
            plan.append({"ds_id": ds_id, "domain": domain, "name": name,
                         "action": "FULL", "limit": limit,
                         "local_rows": local_rows, "remote_total": c["remote_total"],
                         "n_files": n_files,
                         "reason": "re-fetch: local lacks :id/:updated_at"})

    conn.close()
    return plan


def execute_plan(plan):
    """Execute the sync plan."""
    actionable = [p for p in plan if p["action"] != "SKIP"]
    skip_items = [p for p in plan if p["action"] == "SKIP"]

    n_full = sum(1 for p in actionable if p["action"] == "FULL")
    n_delta = sum(1 for p in actionable if p["action"] == "DELTA")
    print(f"\nExecuting: {n_full} FULL, {n_delta} DELTA, {len(skip_items)} SKIP")
    print(f"Workers: {WORKERS}\n")

    t_start = time.time()
    ok = fail = 0
    total_fetched = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {}
        for p in actionable:
            f = pool.submit(sync_one, p["domain"], p["ds_id"], p["action"],
                            p["limit"], p.get("hwm"))
            futures[f] = p

        done = 0
        for f in as_completed(futures):
            p = futures[f]
            done += 1
            try:
                ds_id, action, fetched, new_hwm, err = f.result()
                if err:
                    fail += 1
                    print(f"  [{done:>3}/{len(actionable)}] {ds_id} {action:>5} FAIL: {err[:80]}",
                          flush=True)
                else:
                    ok += 1
                    total_fetched += fetched
                    elapsed = time.time() - t_start
                    hwm_str = f"  hwm={new_hwm[:19]}" if new_hwm else ""
                    print(f"  [{done:>3}/{len(actionable)}] {ds_id} {action:>5} "
                          f"{fetched:>8,} rows{hwm_str}  "
                          f"{p['name'][:35]}  [{elapsed:.0f}s]", flush=True)
            except Exception as e:
                fail += 1
                print(f"  [{done:>3}/{len(actionable)}] {p['ds_id']} EXCEPTION: {e}",
                      flush=True)

    elapsed = time.time() - t_start
    print(f"\n{'='*60}")
    print(f"Done: {ok} ok, {fail} fail, {total_fetched:,} rows fetched, {elapsed:.0f}s")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Incremental Socrata → MinIO sync")
    parser.add_argument("--execute", action="store_true", help="Execute (default: dry-run)")
    parser.add_argument("--full", action="store_true", help="Force full refresh for all")
    parser.add_argument("--ids", nargs="*", help="Sync only these dataset IDs")
    args = parser.parse_args()

    plan = plan_sync(force_full=args.full, only_ids=set(args.ids) if args.ids else None)

    # Print plan summary
    actions = {}
    for p in plan:
        actions.setdefault(p["action"], []).append(p)

    print(f"\n{'='*80}")
    print(f"Sync Plan: {len(plan)} datasets")
    print(f"{'='*80}")

    for action in ["DELTA", "FULL", "SKIP"]:
        items = actions.get(action, [])
        if not items:
            continue
        print(f"\n{action} ({len(items)}):")
        for p in items:
            local = f"{p.get('local_rows', 0):>8,}" if p.get("local_rows") else "       -"
            remote = f"{p.get('remote_total', 0):>10,}" if p.get("remote_total") else "         -"
            files = f"{p.get('n_files', 0)} files" if p.get("n_files") else ""
            print(f"  {p['ds_id']}  local={local}  remote={remote}  {files:>8}  {p['reason']}")

    if not args.execute:
        print(f"\nDry run. Use --execute to run.")
        return

    execute_plan(plan)


if __name__ == "__main__":
    main()
