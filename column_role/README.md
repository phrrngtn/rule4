# column_role

*A database is a thing with attributes.* One long, denormalized, **append-only** relation
in which every row is a member (a column or a parameter) playing a role in a grouping —
`table | view | tvf | proc | pk | uk | fk | index | parameter`. The schema (and the
procedural surface) reduced to columns-in-roles: Codd's Rule 4 taken to its conclusion.

- **[`docs/metamodel.md`](docs/metamodel.md)** — the relation, the `grouping_kind`
  taxonomy, the expression edge, and the per-dialect *common-denominator vs exceptions*
  table.
- **`sql/<dialect>.sql`** — the idiosyncratic catalog projections, one shape out:
  - `duckdb.sql`, `sqlite.sql` — **verified** against a live sample (columns, pk/uk, fk
    with referenced columns, index). Included not as CDC sources but to find the
    cross-dialect common denominator (and the exceptions).
  - `postgresql.sql`, `sqlserver.sql` — strawmen pending a server. SQL Server is the rich
    one that actually drives CDC/CT federation (`is_tracked_by_cdc`, change tracking,
    routine parameters).

The relation is the stable interface; the front-end is swappable — these dialect queries
today (set-based, federated via DuckDB's ODBC extension or run through SQLAlchemy), the
same rows feeding `create_table` (build), `pk` (diff/identity), `fk` (dimension signal).
Store it *in a DuckLake* and schema history is bitemporal for free.
