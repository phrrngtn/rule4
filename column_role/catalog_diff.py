"""Signal-free change detection — the EXCEPT-diff tier.

When an essence has no ``change_signal`` (PostgreSQL / SQLite / DuckDB have no per-object
modify_date), you can't cheap-tail. But because the catalog is *data*, "what changed" is a set
operation: the current full scrape ``EXCEPT`` the previously-stored one (and the reverse) is the
changeset. ``EXCEPT`` keys on the **whole row**, so no PK is needed — any changed attribute makes
the row differ.

Done set-based in DuckDB via **replacement scan**: register the scrape results as an Arrow table
and diff them against the stored copy — no Python row-by-row. Registering an Arrow table is a
driver-level feature, so this is the one place native ``duckdb.connect()`` is right rather than
the SA engine. ``added`` + ``removed`` for the same logical row = an update (after / before
images); either alone = an insert / delete. Identity (a PK / object_id) is what would let you
*pair* them into keyed updates — its absence is the bounded cost of the signal-free tier.
"""
import duckdb
import pyarrow as pa


def _arrow(rows, columns):
    return pa.table({c: [r[i] for r in rows] for i, c in enumerate(columns)})


def except_diff(current, previous, columns):
    """``current``/``previous`` = lists of row tuples for one essence; ``columns`` = the attr
    names (same order as the tuples). Returns ``{'added': …, 'removed': …}`` — added = rows
    new-or-changed in current, removed = rows gone-or-changed. Whole-row EXCEPT in DuckDB."""
    con = duckdb.connect()
    try:
        con.register("cur", _arrow(current, columns))
        con.register("prev", _arrow(previous, columns))
        added = [tuple(r) for r in con.execute("SELECT * FROM cur EXCEPT SELECT * FROM prev").fetchall()]
        removed = [tuple(r) for r in con.execute("SELECT * FROM prev EXCEPT SELECT * FROM cur").fetchall()]
    finally:
        con.close()
    return {"added": added, "removed": removed}
