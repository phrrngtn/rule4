#!/bin/bash
# Reset the rule4_test database: kill connections, drop, recreate, create schemas.
# Run this BEFORE build_all_dialects.py
#
# Usage: ./pg_setup.sh

set -euo pipefail

PSQL="/opt/homebrew/opt/postgresql@17/bin/psql"
DB="rule4_test"

echo "Terminating connections to $DB ..."
$PSQL -d postgres -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '$DB' AND pid <> pg_backend_pid();" > /dev/null 2>&1 || true

echo "Dropping $DB ..."
$PSQL -d postgres -c "DROP DATABASE IF EXISTS $DB;" 2>&1

echo "Creating $DB ..."
$PSQL -d postgres -c "CREATE DATABASE $DB;" 2>&1

# Recreate ramdisk tablespace if the mount exists
if [ -d /Volumes/RAMDisk ]; then
  mkdir -p /Volumes/RAMDisk/pgdata
  chmod 700 /Volumes/RAMDisk/pgdata
  $PSQL -d $DB -c "CREATE TABLESPACE ramdisk LOCATION '/Volumes/RAMDisk/pgdata';" 2>&1 || true
  echo "Ramdisk tablespace ready"
fi

echo "Done — $DB is clean"
