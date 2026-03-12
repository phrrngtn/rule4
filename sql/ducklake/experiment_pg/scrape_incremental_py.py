"""
Variant 1: Pure Python — incremental Socrata catalog scrape into PG TTST.

Uses psycopg2 for PG, urllib for Socrata HTTP. The intern_catalog() sproc
does all shredding and TTST interning. Python is just a fetch-and-feed loop.

Incremental strategy:
  1. Read per-domain watermark from PG: max(metadata_updated_at) of current rows
  2. Probe Socrata: order=updatedAt DESC, limit=1 — anything newer than hwm?
  3. If yes, page forward until updatedAt < hwm (early termination)
  4. Feed changed resources to intern_catalog(domain, results, p_incremental=true)
  5. Periodically (--full) do a complete scrape to catch deletions

Usage:
    cd sql/ducklake/experiment_pg
    uv run python scrape_incremental_py.py                        # incremental
    uv run python scrape_incremental_py.py --full                 # full scrape (catches deletions)
    uv run python scrape_incremental_py.py --domains data.texas.gov
"""

import argparse
import json
import os
import time
import urllib.request

import psycopg2

PG_DSN = os.environ.get("PG_URL", "dbname=rule4_test host=localhost gssencmode=disable")
CATALOG_API = "https://api.us.socrata.com/api/catalog/v1"
PAGE_SIZE = 100


def _api_url(domain, limit, offset, order=None):
    url = (f"{CATALOG_API}"
           f"?domains={urllib.request.quote(domain)}"
           f"&only=datasets"
           f"&limit={limit}&offset={offset}")
    if order:
        url += f"&order={urllib.request.quote(order)}"
    return url


def _fetch_page(url):
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.loads(resp.read())


def get_watermarks(conn):
    """Per-domain high-water mark: max metadata_updated_at of current rows."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT domain, max(metadata_updated_at) AS hwm, count(*) AS n
            FROM socrata.resource
            WHERE tt_end = '9999-12-31'
            GROUP BY domain
        """)
        return {row[0]: {"hwm": row[1], "n": row[2]} for row in cur.fetchall()}


def get_domains(conn):
    """All known domains."""
    with conn.cursor() as cur:
        cur.execute("SELECT domain FROM socrata.domain ORDER BY resource_count DESC")
        return [row[0] for row in cur.fetchall()]


def probe_domain(domain):
    """Check the most-recently-updated resource on a domain. Returns updatedAt string or None."""
    url = _api_url(domain, limit=1, offset=0, order="updatedAt DESC")
    try:
        data = _fetch_page(url)
        results = data.get("results", [])
        if results:
            return results[0]["resource"].get("updatedAt")
    except Exception as e:
        print(f"  WARN: probe {domain}: {e}")
    return None


def fetch_incremental(domain, hwm):
    """Fetch resources updated after hwm, using ordered pagination with early termination.

    Returns the list of resources whose updatedAt > hwm.
    """
    resources = []
    offset = 0

    while True:
        url = _api_url(domain, limit=PAGE_SIZE, offset=offset, order="updatedAt DESC")
        try:
            data = _fetch_page(url)
        except Exception as e:
            print(f"  WARN: {domain} offset={offset}: {e}")
            break

        batch = data.get("results", [])
        if not batch:
            break

        for r in batch:
            updated_at = r["resource"].get("updatedAt", "")
            if hwm and updated_at <= hwm:
                # Passed the watermark — everything from here is old
                return resources
            resources.append(r)

        offset += len(batch)
        total = data.get("resultSetSize", 0)
        if offset >= total:
            break

    return resources


def fetch_full(domain):
    """Fetch all resources for a domain (no early termination)."""
    resources = []
    offset = 0
    total = 0

    while True:
        url = _api_url(domain, limit=PAGE_SIZE, offset=offset)
        try:
            data = _fetch_page(url)
        except Exception as e:
            print(f"  WARN: {domain} offset={offset}: {e}")
            break

        batch = data.get("results", [])
        if not batch:
            break
        resources.extend(batch)

        total = data.get("resultSetSize", 0)
        offset += len(batch)
        if offset >= total:
            break

    return resources, total


def intern(conn, domain, results, incremental):
    """Feed results to the intern_catalog sproc."""
    with conn.cursor() as cur:
        cur.execute(
            "CALL socrata.intern_catalog(%(domain)s, %(payload)s::jsonb, %(incremental)s)",
            {"domain": domain, "payload": json.dumps(results), "incremental": incremental}
        )
    conn.commit()


def main():
    parser = argparse.ArgumentParser(description="Incremental Socrata catalog → PG TTST")
    parser.add_argument("--domains", nargs="*", help="Specific domains (default: all from socrata.domain)")
    parser.add_argument("--full", action="store_true", help="Full scrape (catches deletions)")
    args = parser.parse_args()

    conn = psycopg2.connect(PG_DSN)
    watermarks = get_watermarks(conn)

    if args.domains:
        domains = args.domains
    else:
        domains = get_domains(conn)
        if not domains:
            print("No domains in socrata.domain. Run scrape_catalog.py --discover first.")
            conn.close()
            return

    t_start = time.time()
    total_fetched = 0
    total_changed = 0

    for domain in domains:
        t0 = time.time()
        wm = watermarks.get(domain, {})
        hwm = wm.get("hwm")
        local_n = wm.get("n", 0)

        if args.full or not hwm:
            # Full scrape
            print(f"\n{'='*60}")
            print(f"{domain} — FULL (local={local_n:,})")
            results, total = fetch_full(domain)
            print(f"  Fetched {len(results):,} of {total:,} in {time.time()-t0:.0f}s")

            if results:
                intern(conn, domain, results, incremental=False)
                total_fetched += len(results)
                total_changed += 1
                print(f"  Interned in {time.time()-t0:.0f}s")
            else:
                print(f"  No results, skipping.")
        else:
            # Incremental: probe first
            latest = probe_domain(domain)
            if not latest or latest <= hwm:
                print(f"  {domain:>35}  unchanged (hwm={hwm[:19]})")
                continue

            print(f"\n{'='*60}")
            print(f"{domain} — INCREMENTAL (hwm={hwm[:19]}, latest={latest[:19]})")
            results = fetch_incremental(domain, hwm)
            print(f"  Fetched {len(results):,} changed resources in {time.time()-t0:.0f}s")

            if results:
                intern(conn, domain, results, incremental=True)
                total_fetched += len(results)
                total_changed += 1
                print(f"  Interned in {time.time()-t0:.0f}s")
            else:
                print(f"  Nothing new after filtering.")

    elapsed = time.time() - t_start
    print(f"\n{'='*60}")
    print(f"Done: {total_fetched:,} resources from {total_changed}/{len(domains)} domains in {elapsed:.0f}s")
    conn.close()


if __name__ == "__main__":
    main()
