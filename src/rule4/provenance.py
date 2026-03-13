"""
rule4.provenance — Connection pool preamble for OTEL trace context injection.

Injects OpenTelemetry trace context into database sessions at connection
checkout time, using each dialect's native session context mechanism:

    SQL Server:   sp_set_session_context
    PostgreSQL:   SET LOCAL (custom GUC variables)
    DuckDB:       SET VARIABLE
    SQLite:       application-defined session_get() function

Provenance triggers (SQL Server, PostgreSQL) or application-layer code
(DuckDB, SQLite) can then read the trace context and record it alongside
data changes.

Usage:

    from rule4.provenance import install_provenance_hooks

    engine = create_engine("postgresql://...")
    install_provenance_hooks(engine)

    # All subsequent connections will have trace context injected.
    # If OTEL is active, trace_id/span_id come from the current span.
    # If not, values are NULL (triggers can detect "no trace context").

    # To set application-level context (service name, user ID):
    from rule4.provenance import set_app_context
    set_app_context(service_name="rule4-sync", user_id="jane@example.com")
"""

from __future__ import annotations

import threading
from typing import Optional

from sqlalchemy import event, text


# ── Application context (thread-local) ──────────────────────────────────

_app_context = threading.local()


def set_app_context(
    service_name: Optional[str] = None,
    user_id: Optional[str] = None,
):
    """Set application-level context that will be injected into sessions.

    These values persist in the calling thread until changed. They are
    independent of OTEL — you can use them without OTEL instrumentation.

    Args:
        service_name: Name of the service/process making changes.
        user_id: Application-level user identity.
    """
    if service_name is not None:
        _app_context.service_name = service_name
    if user_id is not None:
        _app_context.user_id = user_id


def _get_app_context():
    """Read current application context."""
    return {
        "service_name": getattr(_app_context, "service_name", None),
        "user_id": getattr(_app_context, "user_id", None),
    }


# ── OTEL trace context extraction ──────────────────────────────────────

def _get_traceparent() -> Optional[str]:
    """Extract W3C traceparent from the current OTEL span, if available.

    Returns the 55-character traceparent string, or None if OTEL is not
    active or no span is in context.
    """
    try:
        from opentelemetry import trace
        span = trace.get_current_span()
        ctx = span.get_span_context()
        if ctx.trace_id == 0:
            return None
        return f"00-{ctx.trace_id:032x}-{ctx.span_id:016x}-{ctx.trace_flags:02x}"
    except ImportError:
        return None
    except Exception:
        return None


# ── SQLite session_get() function ──────────────────────────────────────

# Per-connection context for SQLite. Keyed by connection id().
_sqlite_contexts: dict[int, dict[str, Optional[str]]] = {}


def _sqlite_session_get(key: str) -> Optional[str]:
    """SQLite scalar function that reads from connection-scoped context.

    Registered via create_function on each physical connection. The
    context dict is updated on every checkout via _sqlite_contexts.

    Note: this function is called by SQLite's C engine, which passes
    us the key. We look up context via the current thread's connection.
    """
    # In SQLite, the function is bound to a specific connection at
    # registration time. We use thread-local storage to find the
    # right context dict, since SQLite connections are single-threaded.
    ctx = getattr(_app_context, "_sqlite_ctx", None)
    if ctx is None:
        return None
    return ctx.get(key)


# ── Dialect-specific injection ─────────────────────────────────────────

def _inject_mssql(dbapi_conn, traceparent, app_ctx):
    """SQL Server: sp_set_session_context.

    We do not use @read_only=1 here because the pool may reuse the same
    physical connection for multiple logical checkouts without calling
    sp_reset_connection in between (which is the only way to clear
    read_only keys). The values are overwritten on each checkout anyway.
    """
    cursor = dbapi_conn.cursor()
    try:
        cursor.execute(
            "EXEC sp_set_session_context "
            "@key = N'traceparent', @value = ?",
            (traceparent,)
        )
        cursor.execute(
            "EXEC sp_set_session_context "
            "@key = N'service.name', @value = ?",
            (app_ctx["service_name"],)
        )
        cursor.execute(
            "EXEC sp_set_session_context "
            "@key = N'user.id', @value = ?",
            (app_ctx["user_id"],)
        )
    finally:
        cursor.close()


def _inject_postgresql(dbapi_conn, traceparent, app_ctx):
    """PostgreSQL: SET LOCAL custom GUC variables (transaction-scoped)."""
    cursor = dbapi_conn.cursor()
    try:
        # Use select set_config() with is_local=true for transaction scope.
        # This is safe with PgBouncer in any pooling mode.
        cursor.execute(
            "SELECT set_config('otel.traceparent', %s, true)",
            (traceparent,)
        )
        cursor.execute(
            "SELECT set_config('otel.service_name', %s, true)",
            (app_ctx["service_name"] or "",)
        )
        cursor.execute(
            "SELECT set_config('otel.user_id', %s, true)",
            (app_ctx["user_id"] or "",)
        )
    finally:
        cursor.close()


def _inject_duckdb(dbapi_conn, traceparent, app_ctx):
    """DuckDB: SET VARIABLE (session-scoped)."""
    cursor = dbapi_conn.cursor()
    try:
        cursor.execute("SET VARIABLE traceparent = ?", (traceparent or "",))
        cursor.execute(
            "SET VARIABLE service_name = ?",
            (app_ctx["service_name"] or "",)
        )
        cursor.execute(
            "SET VARIABLE user_id = ?",
            (app_ctx["user_id"] or "",)
        )
    finally:
        cursor.close()


def _inject_sqlite(dbapi_conn, traceparent, app_ctx):
    """SQLite: update the connection-scoped context dict."""
    ctx = {
        "traceparent": traceparent,
        "service.name": app_ctx["service_name"],
        "user.id": app_ctx["user_id"],
    }
    conn_id = id(dbapi_conn)
    _sqlite_contexts[conn_id] = ctx
    # Also set on thread-local so session_get() can find it
    _app_context._sqlite_ctx = ctx


# ── Event listeners ────────────────────────────────────────────────────

def _on_connect_sqlite(dbapi_conn, connection_record):
    """Register session_get() on new SQLite physical connections."""
    dbapi_conn.create_function("session_get", 1, _sqlite_session_get)


def _on_checkout(dbapi_conn, connection_record, connection_proxy):
    """Inject trace context on every connection checkout."""
    traceparent = _get_traceparent()
    app_ctx = _get_app_context()

    dialect = connection_record.dialect.name if hasattr(connection_record, 'dialect') else None

    # Detect dialect from the dbapi connection if not available from record
    if dialect is None:
        dialect = _detect_dialect(dbapi_conn)

    if dialect == "mssql":
        _inject_mssql(dbapi_conn, traceparent, app_ctx)
    elif dialect == "postgresql":
        _inject_postgresql(dbapi_conn, traceparent, app_ctx)
    elif dialect == "duckdb":
        _inject_duckdb(dbapi_conn, traceparent, app_ctx)
    elif dialect == "sqlite":
        _inject_sqlite(dbapi_conn, traceparent, app_ctx)


def _detect_dialect(dbapi_conn) -> str:
    """Detect dialect from the raw DBAPI connection type."""
    module = type(dbapi_conn).__module__
    if "psycopg2" in module or "psycopg" in module:
        return "postgresql"
    elif "pyodbc" in module or "pymssql" in module:
        return "mssql"
    elif "duckdb" in module:
        return "duckdb"
    elif "sqlite" in module:
        return "sqlite"
    # Fallback: check class name
    cls_name = type(dbapi_conn).__name__.lower()
    if "duckdb" in cls_name:
        return "duckdb"
    return "unknown"


def _on_checkin_sqlite(dbapi_conn, connection_record):
    """Clean up SQLite context on checkin."""
    conn_id = id(dbapi_conn)
    _sqlite_contexts.pop(conn_id, None)


# ── Public API ─────────────────────────────────────────────────────────

def install_provenance_hooks(engine):
    """Install connection pool event listeners for provenance injection.

    Call once per engine. All subsequent connections from this engine's
    pool will have trace context injected at checkout time.

    Args:
        engine: SQLAlchemy engine (any supported dialect)
    """
    dialect_name = engine.dialect.name

    # Store dialect on the pool for the checkout handler
    engine.pool._dialect_name = dialect_name

    if dialect_name == "sqlite":
        event.listen(engine, "connect", _on_connect_sqlite)
        event.listen(engine.pool, "checkin", _on_checkin_sqlite)

    # Use engine-level connect event to get dialect info into checkout
    @event.listens_for(engine.pool, "checkout")
    def checkout_handler(dbapi_conn, connection_record, connection_proxy):
        traceparent = _get_traceparent()
        app_ctx = _get_app_context()

        if dialect_name == "mssql":
            _inject_mssql(dbapi_conn, traceparent, app_ctx)
        elif dialect_name == "postgresql":
            _inject_postgresql(dbapi_conn, traceparent, app_ctx)
        elif dialect_name == "duckdb":
            _inject_duckdb(dbapi_conn, traceparent, app_ctx)
        elif dialect_name == "sqlite":
            _inject_sqlite(dbapi_conn, traceparent, app_ctx)


def read_session_context(conn, dialect_name=None):
    """Read back the current session context from a live connection.

    Useful for testing and for application-layer provenance capture
    (DuckDB, where there are no triggers).

    Args:
        conn: SQLAlchemy Connection object
        dialect_name: Override dialect detection (optional)

    Returns:
        dict with traceparent, service_name, user_id (any may be None)
    """
    if dialect_name is None:
        dialect_name = conn.dialect.name

    if dialect_name == "mssql":
        row = conn.execute(text(
            "SELECT "
            "CAST(SESSION_CONTEXT(N'traceparent') AS VARCHAR(55)) AS traceparent, "
            "CAST(SESSION_CONTEXT(N'service.name') AS VARCHAR(128)) AS service_name, "
            "CAST(SESSION_CONTEXT(N'user.id') AS VARCHAR(128)) AS user_id"
        )).fetchone()
        return {
            "traceparent": row[0],
            "service_name": row[1],
            "user_id": row[2],
        }

    elif dialect_name == "postgresql":
        row = conn.execute(text(
            "SELECT "
            "current_setting('otel.traceparent', true) AS traceparent, "
            "current_setting('otel.service_name', true) AS service_name, "
            "current_setting('otel.user_id', true) AS user_id"
        )).fetchone()
        return {
            "traceparent": row[0] or None,
            "service_name": row[1] or None,
            "user_id": row[2] or None,
        }

    elif dialect_name == "duckdb":
        row = conn.execute(text(
            "SELECT "
            "getvariable('traceparent') AS traceparent, "
            "getvariable('service_name') AS service_name, "
            "getvariable('user_id') AS user_id"
        )).fetchone()
        return {
            "traceparent": row[0] or None,
            "service_name": row[1] or None,
            "user_id": row[2] or None,
        }

    elif dialect_name == "sqlite":
        row = conn.execute(text(
            "SELECT "
            "session_get('traceparent') AS traceparent, "
            "session_get('service.name') AS service_name, "
            "session_get('user.id') AS user_id"
        )).fetchone()
        return {
            "traceparent": row[0],
            "service_name": row[1],
            "user_id": row[2],
        }

    return {"traceparent": None, "service_name": None, "user_id": None}
