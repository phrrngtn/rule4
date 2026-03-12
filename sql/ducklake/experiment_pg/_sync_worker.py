"""
Worker subprocess for incremental_sync.py.

Reads parameters from SYNC_PARAMS env var (JSON), fetches TSV from Socrata
via urllib, pipes it to DuckDB which writes Parquet to S3/MinIO.

Output on stdout: "row_count|max_updated_at"
"""

import json
import os
import sys
import threading
import urllib.request

import duckdb

params = json.loads(os.environ["SYNC_PARAMS"])
ds_id = params["ds_id"]
domain = params["domain"]
s3_path = params["s3_path"]
limit = params["limit"]
where_clause = params["where_clause"]
s3_secret_sql = params["s3_secret_sql"]

# Build Socrata URL with system fields :id and :updated_at
select = "*,:id,:updated_at"
url = (
    f"https://{domain}/resource/{ds_id}.tsv"
    f"?$select={urllib.request.quote(select)}"
    f"&$limit={limit}"
)
if where_clause:
    url += f"&$where={urllib.request.quote(where_clause)}"
url += f"&$order={urllib.request.quote(':updated_at ASC')}"

# Pipe: writer thread pushes HTTP response bytes, DuckDB reads from the fd
read_fd, write_fd = os.pipe()


def fetch_to_pipe():
    try:
        with urllib.request.urlopen(url, timeout=300) as resp:
            wf = os.fdopen(write_fd, "wb")
            while True:
                chunk = resp.read(65536)
                if not chunk:
                    break
                wf.write(chunk)
            wf.close()
    except Exception as e:
        try:
            os.close(write_fd)
        except OSError:
            pass
        print(f"FETCH_ERROR: {e}", file=sys.stderr)
        sys.exit(1)


t = threading.Thread(target=fetch_to_pipe, daemon=True)
t.start()

pipe_path = f"/dev/fd/{read_fd}"

conn = duckdb.connect()
conn.execute(s3_secret_sql)

# DuckDB's COPY and read_csv don't support bind parameters for file paths
# and table functions, so we use f-strings here for the file paths only.
# The paths are generated internally (not user input), so this is safe.
conn.execute(
    f"COPY ("
    f"  SELECT * FROM read_csv('{pipe_path}', delim='\\t', header=true,"
    f"                         all_varchar=true, ignore_errors=true)"
    f") TO '{s3_path}' (FORMAT PARQUET)"
)
os.close(read_fd)
t.join(timeout=5)

# Use bind parameter for the file path in read_parquet via a prepared approach.
# DuckDB read_parquet() requires a string literal, not a bind param, so we
# query via a macro-style approach. File paths are internal, not user-supplied.
cnt = conn.execute(
    f"SELECT count(*) FROM read_parquet('{s3_path}')"
).fetchone()[0]

# ORDER BY DESC LIMIT 1 instead of max() — DuckDB's max() on varchar
# Parquet columns can return truncated values from Parquet column statistics.
hwm_row = conn.execute(
    f'SELECT ":updated_at" FROM read_parquet(\'{s3_path}\')'
    f' ORDER BY ":updated_at" DESC LIMIT 1'
).fetchone()
hwm = hwm_row[0] if hwm_row else None

print(f"{cnt}|{hwm}")
conn.close()
