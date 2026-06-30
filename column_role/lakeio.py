"""Write rows to a Parquet file via a SQLAlchemy duckdb engine.

Generic SQL (the staging table + its insert) is SA Core — a prepared statement with bound
parameters; only the duckdb-specific ``COPY`` is ``text()``, executed on the same SA
connection. The native ``duckdb`` driver is reserved for driver-level (non-SQL) needs.
"""
from sqlalchemy import Column, MetaData, Table, create_engine, text


def write_parquet(colspecs, rows, path, *, name="staging"):
    """``colspecs`` = ``[(name, sa_type), …]``; ``rows`` = list of tuples. Stages the rows in
    a temp table (SA Core create + insert) and ``COPY``s it to ``path`` as Parquet."""
    eng = create_engine("duckdb:///:memory:")
    stg = Table(name, MetaData(), *[Column(n, t) for n, t in colspecs])
    names = [n for n, _ in colspecs]
    with eng.begin() as conn:
        stg.create(conn)
        if rows:
            conn.execute(stg.insert(), [dict(zip(names, r)) for r in rows])
        conn.execute(text(f"COPY {name} TO '{path}' (FORMAT PARQUET)"))
    eng.dispose()
