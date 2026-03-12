#!/bin/bash
# Fetch ALL Socrata dataset metadata (across all domains) from the Discovery API.
# No row data is downloaded — only schema metadata (table names, column names, types, descriptions).
# The API caps at 10,000 results. Paginates with limit=1000.
#
# Usage: ./fetch_all_socrata.sh [output_dir]

set -euo pipefail

OUTDIR="${1:-$(dirname "$0")/raw}"
mkdir -p "$OUTDIR"

LIMIT=1000
OFFSET=0
PAGE=1
OUTFILE="$OUTDIR/all_socrata_catalog.json"
TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

echo "Fetching ALL Socrata dataset metadata (all domains, no row data) ..."

while true; do
  URL="https://api.us.socrata.com/api/catalog/v1?only=datasets&limit=${LIMIT}&offset=${OFFSET}&order=updated_at"
  PAGEFILE="$TMPDIR/page_${PAGE}.json"

  HTTP_CODE=$(curl -sf -w "%{http_code}" -o "$PAGEFILE" "$URL" 2>/dev/null || echo "000")

  if [ "$HTTP_CODE" != "200" ]; then
    echo "  page $PAGE: HTTP $HTTP_CODE — stopping"
    break
  fi

  COUNT=$(python3 -c "import json; d=json.load(open('$PAGEFILE')); print(len(d['results']))")
  TOTAL=$(python3 -c "import json; d=json.load(open('$PAGEFILE')); print(d['resultSetSize'])")

  echo "  page $PAGE: got $COUNT results (offset=$OFFSET, total=$TOTAL)"

  if [ "$COUNT" -eq 0 ]; then
    break
  fi

  OFFSET=$((OFFSET + LIMIT))
  PAGE=$((PAGE + 1))

  # Be polite
  sleep 0.5
done

# Merge all pages into a single JSON array
python3 -c "
import json, glob, os
pages = sorted(glob.glob('$TMPDIR/page_*.json'))
all_results = []
for p in pages:
    with open(p) as f:
        data = json.load(f)
        all_results.extend(data['results'])
# Deduplicate by resource.id (some may appear on multiple pages)
seen = set()
unique = []
for r in all_results:
    rid = r.get('resource', {}).get('id', '')
    if rid and rid not in seen:
        seen.add(rid)
        unique.append(r)
with open('$OUTFILE', 'w') as f:
    json.dump(unique, f)
print(f'Wrote {len(unique)} unique datasets to $OUTFILE')
# Domain summary
from collections import Counter
domains = Counter(r.get('metadata', {}).get('domain', 'unknown') for r in unique)
for d, c in domains.most_common(20):
    print(f'  {d}: {c}')
if len(domains) > 20:
    print(f'  ... and {len(domains) - 20} more domains')
"
