"""Schema migration DDL from a column-collection changeset.

Two ``ColumnCollection``\\ s are two *revisions* of one table's schema — e.g. column_role's
``schema_as_of(T_{n-1})`` and ``schema_as_of(T_n)`` (so the revisions come straight from the
schema time-series). Their **difference**, read as ``ALTER TABLE`` statements, migrates the
schema from one revision to the other — in *either* direction (``n-1 -> n`` forward, or
``n -> n-3`` rollback), since a migration is just ``diff(from, to)`` with the arguments
swapped.

A capture *records* schema drift; this *replays* it as DDL — the asymmetric inverse. Scope,
by the user's call, is **schema-only**: a type change emits ``ALTER COLUMN`` with no
data-preserving ``USING``/cast (data loss is accepted). Two things a column *set* can't carry
are deliberately not attempted: **renames** (a rename is indistinguishable from drop+add over
a set of names) and **column order** (most engines can't reposition a column in place — order
differences are ignored, not migrated).

The changeset is computed once (dialect-independent) and *rendered* per target dialect, so
the same drift becomes T-SQL, PostgreSQL, DuckDB, or (additive-only) SQLite DDL. Types are
emitted as the captured ``source_type`` verbatim — the revisions are captures of one source,
so the migration targets that source's own dialect by default.
"""


class ColumnDelta:
    """One column-level change between two revisions. ``kind`` ∈ {add, drop, retype}.
    ``before``/``after`` are the :class:`~column_collection.Col` on each side (``before`` is
    ``None`` for add, ``after`` is ``None`` for drop)."""

    def __init__(self, kind, name, *, before=None, after=None):
        self.kind, self.name, self.before, self.after = kind, name, before, after

    def __repr__(self):
        if self.kind == "retype":
            return f"<retype {self.name}: {self.before.source_type} -> {self.after.source_type}>"
        return f"<{self.kind} {self.name}>"


def schema_diff(from_cc, to_cc):
    """The ordered changeset taking ``from_cc``'s schema to ``to_cc``'s: columns added
    (in ``to`` order), retyped (``source_type`` differs), then dropped (in ``from`` order).
    Renames are not detected; column order is not compared."""
    src = {c.name: c for c in from_cc.columns}
    dst = {c.name: c for c in to_cc.columns}
    adds = [ColumnDelta("add", c.name, after=c) for c in to_cc.columns if c.name not in src]
    retypes = [ColumnDelta("retype", c.name, before=src[c.name], after=c)
               for c in to_cc.columns
               if c.name in src and c.source_type != src[c.name].source_type]
    drops = [ColumnDelta("drop", c.name, before=c) for c in from_cc.columns if c.name not in dst]
    return adds + retypes + drops


# per-dialect rendering: identifier quoting + statement templates. retype=None => the engine
# can't change a column's type in place (SQLite) -> emitted as a skipped-comment.
def _q_bracket(n):
    return f"[{n}]"


def _q_dquote(n):
    return f'"{n}"'


_DIALECTS = {
    "sqlserver": dict(q=_q_bracket,
                      add="ALTER TABLE {t} ADD {col} {ty}",
                      drop="ALTER TABLE {t} DROP COLUMN {col}",
                      retype="ALTER TABLE {t} ALTER COLUMN {col} {ty}"),
    "postgresql": dict(q=_q_dquote,
                       add="ALTER TABLE {t} ADD COLUMN {col} {ty}",
                       drop="ALTER TABLE {t} DROP COLUMN {col}",
                       retype="ALTER TABLE {t} ALTER COLUMN {col} TYPE {ty}"),
    "duckdb": dict(q=_q_dquote,
                   add="ALTER TABLE {t} ADD COLUMN {col} {ty}",
                   drop="ALTER TABLE {t} DROP COLUMN {col}",
                   retype="ALTER TABLE {t} ALTER COLUMN {col} TYPE {ty}"),
    "sqlite": dict(q=_q_dquote,
                   add="ALTER TABLE {t} ADD COLUMN {col} {ty}",
                   drop="ALTER TABLE {t} DROP COLUMN {col}",
                   retype=None),   # SQLite has no ALTER COLUMN type; a rebuild is required
}


def _table_ref(cc, q):
    return f"{q(cc.schema)}.{q(cc.name)}" if cc.schema else q(cc.name)


def render(deltas, table_cc, dialect):
    """Render a changeset as DDL statements for ``dialect``. ``table_cc`` supplies the
    (schema-qualified) table name and the quoting target."""
    d = _DIALECTS[dialect]
    q, t = d["q"], _table_ref(table_cc, _DIALECTS[dialect]["q"])
    out = []
    for delta in deltas:
        col = q(delta.name)
        if delta.kind == "add":
            out.append(d["add"].format(t=t, col=col, ty=delta.after.source_type) + ";")
        elif delta.kind == "drop":
            out.append(d["drop"].format(t=t, col=col) + ";")
        else:  # retype
            if d["retype"] is None:
                out.append(f"-- {dialect}: cannot ALTER COLUMN {col} type in place "
                           f"({delta.before.source_type} -> {delta.after.source_type}); "
                           f"table rebuild required -- skipped")
            else:
                out.append(d["retype"].format(t=t, col=col, ty=delta.after.source_type) + ";")
    return out


def recreate_ddl(to_cc, dialect):
    """``DROP TABLE`` + ``CREATE TABLE`` — the DDL for a detected drop-and-recreate (the
    object_id identity changed), where an ALTER changeset would be wrong."""
    d = _DIALECTS[dialect]
    t = _table_ref(to_cc, d["q"])
    cols = ", ".join(f"{d['q'](c.name)} {c.source_type}" for c in to_cc.columns)
    return [f"DROP TABLE {t};", f"CREATE TABLE {t} ({cols});"]


def migration_ddl(from_cc, to_cc, *, dialect=None):
    """The DDL migrating ``from_cc``'s schema to ``to_cc``'s. Normally an ``ALTER TABLE``
    changeset — but if the two revisions carry **different object_id**s (the identity LT),
    the object was dropped and recreated, not evolved, so emit ``DROP``+``CREATE`` instead.
    ``dialect`` defaults to the source dialect. Schema-only; renames and column order are not
    migrated (see the module docstring)."""
    dialect = dialect or from_cc.dialect
    if (from_cc.object_id is not None and to_cc.object_id is not None
            and from_cc.object_id != to_cc.object_id):
        return recreate_ddl(to_cc, dialect)
    return render(schema_diff(from_cc, to_cc), to_cc, dialect)
