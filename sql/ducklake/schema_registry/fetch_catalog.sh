#!/bin/bash
# Fetch Socrata Discovery API catalog for a domain.
# Paginates through all results and writes to a single JSON array.
#
# Usage: ./fetch_catalog.sh [domain] [output_dir]
#   domain defaults to data.cityofnewyork.us
#   output_dir defaults to ./raw

set -euo pipefail

DOMAIN="${1:-data.cityofnewyork.us}"
OUTDIR="${2:-$(dirname "$0")/raw}"
mkdir -p "$OUTDIR"

LIMIT=100
OFFSET=0
PAGE=1
OUTFILE="$OUTDIR/${DOMAIN}_catalog.json"
TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

echo "Fetching catalog for $DOMAIN ..."

while true; do
  URL="https://api.us.socrata.com/api/catalog/v1?domains=${DOMAIN}&only=datasets&limit=${LIMIT}&offset=${OFFSET}&order=updated_at"
  PAGEFILE="$TMPDIR/page_${PAGE}.json"
  curl -sf "$URL" > "$PAGEFILE"

  COUNT=$(python3 -c "import json; d=json.load(open('$PAGEFILE')); print(len(d['results']))")
  TOTAL=$(python3 -c "import json; d=json.load(open('$PAGEFILE')); print(d['resultSetSize'])")

  echo "  page $PAGE: got $COUNT results (offset=$OFFSET, total=$TOTAL)"

  if [ "$COUNT" -eq 0 ]; then
    break
  fi

  OFFSET=$((OFFSET + LIMIT))
  PAGE=$((PAGE + 1))
done

# Merge all pages into a single JSON array of results
python3 -c "
import json, glob, os
pages = sorted(glob.glob('$TMPDIR/page_*.json'))
all_results = []
for p in pages:
    with open(p) as f:
        data = json.load(f)
        all_results.extend(data['results'])
with open('$OUTFILE', 'w') as f:
    json.dump(all_results, f)
print(f'Wrote {len(all_results)} results to $OUTFILE')
"
