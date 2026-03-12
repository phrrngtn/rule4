#!/bin/bash
# ============================================================================
# 00_fetch_socrata.sh — Fetch Socrata metadata and data samples via curl
# ============================================================================
#
# Downloads catalog metadata and small data samples from a Socrata domain.
# All responses cached as JSON/CSV in raw/ so subsequent steps are offline.
#
# Usage:
#   cd sql/ducklake/experiment
#   bash 00_fetch_socrata.sh [domain] [limit]
#
# Defaults: domain=data.cityofnewyork.us, limit=10 datasets
# ============================================================================

set -euo pipefail

DOMAIN="${1:-data.cityofnewyork.us}"
LIMIT="${2:-10}"
DATA_SAMPLE_ROWS=500

RAW_DIR="raw/${DOMAIN}"
mkdir -p "${RAW_DIR}/views" "${RAW_DIR}/data"

echo "=== Fetching catalog for ${DOMAIN} (limit=${LIMIT}) ==="

# 1. Discovery API — dataset catalog
CATALOG_FILE="${RAW_DIR}/catalog.json"
if [ ! -f "${CATALOG_FILE}" ]; then
    curl -s "https://api.us.socrata.com/api/catalog/v1?domains=${DOMAIN}&limit=${LIMIT}&only=datasets" \
        -o "${CATALOG_FILE}"
    echo "  catalog: $(python3 -c "import json; d=json.load(open('${CATALOG_FILE}')); print(len(d['results']), 'datasets of', d.get('resultSetSize','?'), 'total')")"
else
    echo "  catalog: cached"
fi

# 2. Extract dataset IDs
DATASET_IDS=$(python3 -c "
import json
d = json.load(open('${CATALOG_FILE}'))
for r in d['results']:
    print(r['resource']['id'])
")

# 3. For each dataset: fetch view metadata and a data sample
for ID in ${DATASET_IDS}; do
    VIEW_FILE="${RAW_DIR}/views/${ID}.json"
    DATA_FILE="${RAW_DIR}/data/${ID}.csv"

    # View metadata (column definitions, cached stats)
    if [ ! -f "${VIEW_FILE}" ]; then
        echo "  view ${ID}: fetching..."
        curl -s "https://${DOMAIN}/api/views/${ID}.json" -o "${VIEW_FILE}"
    else
        echo "  view ${ID}: cached"
    fi

    # Data sample as CSV (SODA2)
    if [ ! -f "${DATA_FILE}" ]; then
        echo "  data ${ID}: fetching ${DATA_SAMPLE_ROWS} rows..."
        curl -s "https://${DOMAIN}/resource/${ID}.csv?\$limit=${DATA_SAMPLE_ROWS}" -o "${DATA_FILE}"
    else
        echo "  data ${ID}: cached"
    fi
done

echo ""
echo "=== Done. Raw files in ${RAW_DIR}/ ==="
echo "  catalog:  ${CATALOG_FILE}"
echo "  views:    ${RAW_DIR}/views/*.json"
echo "  data:     ${RAW_DIR}/data/*.csv"
