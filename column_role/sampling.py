"""A sampling table — the DuckLake-resident control plane that self-hosts replica creation.

The set of sources to replicate is *data*, not code. Each row of the `sampling` DuckLake
table declares a source `(dataserver, database, schema, object)`, how to replicate it
(`mode`, `key_column`), and where it lands (`target_table`). `provision()` reads that
table, pulls each source's columns from the **column_role** registry (`schema_as_of`),
and materialises the DuckLake replica — creating it, or evolving it via
`reconcile_columns`. The replicas land in the *same* DuckLake catalog that holds the
sampling table: a lake that stores the plan that builds the lake's own tables. Point a
spec at `sampling`/`column_role` themselves and it can reconstruct its own topology.
"""
import os

import duckdb

import ducklake_oob_writer as dl
from schema_evolution import desired_columns

_DDL = [("dataserver", "varchar"), ("database", "varchar"), ("source_schema", "varchar"),
        ("source_object", "varchar"), ("key_column", "varchar"), ("mode", "varchar"),
        ("target_table", "varchar"), ("enabled", "boolean")]
_PARQUET_DDL = ("dataserver VARCHAR, database VARCHAR, source_schema VARCHAR, "
                "source_object VARCHAR, key_column VARCHAR, mode VARCHAR, "
                "target_table VARCHAR, enabled BOOLEAN")
_FIELDS = [n for n, _ in _DDL]

# Connections — ODBC *components* (never a password; integrated security) in the catalog.
_CONN_DDL = [("dataserver", "varchar"), ("database", "varchar"), ("dialect", "varchar"),
             ("odbc_driver", "varchar"), ("odbc_server", "varchar"),
             ("odbc_database", "varchar"), ("trusted", "boolean"), ("extra", "varchar")]
_CONN_PARQUET_DDL = ("dataserver VARCHAR, database VARCHAR, dialect VARCHAR, "
                     "odbc_driver VARCHAR, odbc_server VARCHAR, odbc_database VARCHAR, "
                     "trusted BOOLEAN, extra VARCHAR")
_CONN_FIELDS = [n for n, _ in _CONN_DDL]


class SamplingPlan:
    """The sampling table, stored in DuckLake. The same catalog hosts the replicas it
    provisions (self-hosting)."""

    def __init__(self, catalog_path, data_path):
        from sqlalchemy import create_engine
        self.catalog_path, self.data_path = catalog_path, data_path
        self._eng = create_engine(f"sqlite:///{catalog_path}")
        dl.create_catalog(self._eng)
        w = dl.DuckLakeWriter(self._eng, dl.DUCKLAKE_METADATA)
        w.init_catalog(data_path=data_path)
        w.create_table("main", "sampling", _DDL)
        w.create_table("main", "connections", _CONN_DDL)
        self.writer = w
        os.makedirs(os.path.join(data_path, "main", "sampling"), exist_ok=True)
        os.makedirs(os.path.join(data_path, "main", "connections"), exist_ok=True)

    def declare(self, specs, sample_time):
        """Append replica specs (dicts) as one snapshot of the sampling table."""
        rows = [(s["dataserver"], s["database"], s.get("source_schema", "main"),
                 s["source_object"], s["key_column"], s.get("mode", "net"),
                 s.get("target_table", s["source_object"]), s.get("enabled", True))
                for s in specs]
        tag = f"sampling__{sample_time:%Y%m%dT%H%M%S}"
        pq = os.path.join(self.data_path, "main", "sampling", f"{tag}.parquet")
        d = duckdb.connect()
        d.execute(f"CREATE TABLE s ({_PARQUET_DDL})")
        d.executemany(f"INSERT INTO s VALUES ({','.join('?' * len(_FIELDS))})", rows)
        d.execute(f"COPY s TO '{pq}' (FORMAT PARQUET)")
        d.close()
        self.writer.register_parquet("sampling", pq, rel_path=f"{tag}.parquet",
                                     snapshot_time=sample_time)

    def specs(self):
        """The enabled replica specs currently in the plan."""
        with dl.attach_lake(f"sqlite:{self.catalog_path}", self.data_path) as c:
            return c.execute(
                "SELECT dataserver, database, source_schema, source_object, key_column, "
                "mode, target_table FROM lake.sampling WHERE enabled "
                "ORDER BY target_table").fetchall()

    def declare_connection(self, conns, sample_time):
        """Append ODBC connection specs (dicts) — components only, never a password."""
        rows = [(c["dataserver"], c["database"], c.get("dialect", "sqlserver"),
                 c["odbc_driver"], c["odbc_server"], c["odbc_database"],
                 c.get("trusted", True), c.get("extra", "")) for c in conns]
        tag = f"connections__{sample_time:%Y%m%dT%H%M%S}"
        pq = os.path.join(self.data_path, "main", "connections", f"{tag}.parquet")
        d = duckdb.connect()
        d.execute(f"CREATE TABLE c ({_CONN_PARQUET_DDL})")
        d.executemany(f"INSERT INTO c VALUES ({','.join('?' * len(_CONN_FIELDS))})", rows)
        d.execute(f"COPY c TO '{pq}' (FORMAT PARQUET)")
        d.close()
        self.writer.register_parquet("connections", pq, rel_path=f"{tag}.parquet",
                                     snapshot_time=sample_time)

    def connection_string(self, dataserver, database):
        """Assemble the ODBC connection string for a source from its stored components."""
        from tailing import odbc_connection_string
        with dl.attach_lake(f"sqlite:{self.catalog_path}", self.data_path) as c:
            row = c.execute(
                "SELECT odbc_driver, odbc_server, odbc_database, trusted, extra "
                "FROM lake.connections WHERE dataserver = ? AND database = ?",
                [dataserver, database]).fetchone()
        if row is None:
            raise KeyError(f"no connection for {dataserver}/{database}")
        driver, server, db, trusted, extra = row
        return odbc_connection_string(driver, server, db, trusted=bool(trusted), extra=extra or "")

    def dispose(self):
        self._eng.dispose()


def provision(plan, column_role_registry, when, *, replica_writer=None, target_schema="main"):
    """Read the sampling plan and, for each enabled spec, create or evolve the DuckLake
    replica with the source's columns as column_role knew them at `when`. Replicas land in
    `replica_writer` (default: the plan's own catalog — self-hosting). Returns a report per
    spec (target / mode / key / column count / action)."""
    w = replica_writer or plan.writer
    report = []
    for (srv, db, ss, obj, key, mode, tgt) in plan.specs():
        desired = desired_columns(column_role_registry, srv, db, obj, when, schema=ss)
        if not desired:
            report.append({"target": tgt, "action": "skipped (no schema in column_role)"})
            continue
        existing = {t["table_name"] for t in w.current_tables()}
        if tgt not in existing:
            w.create_table(target_schema, tgt, desired, snapshot_time=when)
            action = "created"
        else:
            added = w.reconcile_columns(tgt, desired, schema_name=target_schema, snapshot_time=when)
            action = f"evolved (+{len(added)})" if added else "current"
        report.append({"target": tgt, "mode": mode, "key": key,
                       "columns": len(desired), "action": action})
    return report
