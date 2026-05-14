"""Database layer — supports both PostgreSQL (production) and SQLite (tests/local).

Production uses PostgreSQL via SQLAlchemy with connection pooling.
Tests use in-memory SQLite with a compatibility shim.

Usage:
    # Get a new connection/session for a request/operation
    db = get_db_engine()   # returns Engine (SQLite) or Session (PostgreSQL)
    # Use execute_sql(db, text, params) for all SQL operations
    # Call commit_tx(db) to commit, rollback_tx(db) to rollback
"""

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Optional, Union

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import QueuePool

from fusion_council_service.logging_utils import get_logger

logger = get_logger("fusion_council_service.db")

# ── Global engine/session factory (initialized once) ──
_engine = None
_SessionFactory = None
_is_postgresql = False


def _detect_dialect():
    """Check if we're configured for PostgreSQL or SQLite."""
    global _is_postgresql
    db_url = os.environ.get("DATABASE_URL", "")
    db_path = os.environ.get("DATABASE_PATH", "")

    if db_url and (db_url.startswith("postgresql") or db_url.startswith("postgres")):
        _is_postgresql = True
        return True
    # SQLite mode (path or :memory:)
    _is_postgresql = False
    return False


def get_engine():
    """Get the SQLAlchemy Engine (initialized once)."""
    global _engine, _SessionFactory

    if _engine is not None:
        return _engine

    _detect_dialect()

    if _is_postgresql:
        db_url = os.environ["DATABASE_URL"]
        _engine = create_engine(
            db_url,
            poolclass=QueuePool,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            pool_timeout=30,
        )
        _SessionFactory = sessionmaker(bind=_engine)
        logger.info("PostgreSQL engine initialized", db_url=db_url.split("@")[0] + "@***")
    else:
        db_path = os.environ.get("DATABASE_PATH", ":memory:")
        if db_path != ":memory:":
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        _engine = create_engine(
            f"sqlite:///{db_path}",
            connect_args={"timeout": 30, "check_same_thread": False},
        )
        logger.info(f"SQLite engine initialized: {db_path}")

    return _engine


def new_session() -> Union[Session, sqlite3.Connection]:
    """Get a new database handle.

    For PostgreSQL: returns a SQLAlchemy Session (caller must commit/close).
    For SQLite: returns a raw sqlite3.Connection (backward compat).
    """
    if _is_postgresql:
        if _SessionFactory is None:
            get_engine()  # initializes _SessionFactory
        return _SessionFactory()
    else:
        # SQLite: return raw connection for backward compat with existing code
        db_path = os.environ.get("DATABASE_PATH", ":memory:")
        db = sqlite3.connect(db_path, timeout=30)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=DELETE")
        db.execute("PRAGMA synchronous=FULL")
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA busy_timeout=30000")
        return db


def initialize_schema(db=None) -> None:
    """Read and execute schema.sql to create tables if they do not exist."""
    if db is None:
        db = new_session()
        _own_session = True
    else:
        _own_session = False

    schema_path = Path(__file__).parent / "schema.sql"
    sql = schema_path.read_text()

    # Strip SQLite-specific PRAGMAs for PostgreSQL
    if _is_postgresql:
        lines = []
        for line in sql.split("\n"):
            stripped = line.strip()
            if stripped.startswith("PRAGMA "):
                continue
            # Convert AUTOINCREMENT to just auto-increment (PostgreSQL SERIAL)
            # Handled in schema.sql with conditional comments
            lines.append(line)
        sql = "\n".join(lines)

    if _is_postgresql:
        # Execute each statement separately for PostgreSQL
        from sqlalchemy.exc import ProgrammingError
        statements = [s.strip() for s in sql.split(";") if s.strip()]
        for stmt in statements:
            try:
                db.execute(text(stmt))
            except ProgrammingError:
                db.rollback()
                raise
        db.commit()
    else:
        db.executescript(sql)

    if _own_session and _is_postgresql:
        db.close()

    logger.info("Schema initialized")


def commit_tx(db) -> None:
    """Commit transaction."""
    if _is_postgresql:
        db.commit()
    # SQLite: auto-commits by default with executescript


def rollback_tx(db) -> None:
    """Rollback transaction."""
    if _is_postgresql:
        db.rollback()


def close_db(db) -> None:
    """Close database connection/session."""
    if _is_postgresql:
        db.close()
    else:
        db.close()


def execute_sql(db, sql: str, params: Optional[dict] = None):
    """Execute SQL and return result.

    For PostgreSQL: uses SQLAlchemy text() with named params (:param).
    For SQLite: uses raw execute with positional params (?).
    """
    if _is_postgresql:
        return db.execute(text(sql), params or {})
    else:
        # Convert named params (:param) to positional (?) for SQLite
        if params:
            import re
            param_names = list(params.keys())
            positional_sql = re.sub(r':(\w+)', '?', sql)
            positional_params = [params[k] for k in param_names]
            return db.execute(positional_sql, positional_params)
        return db.execute(sql)


def execute_sql_scalar(db, sql: str, params: Optional[dict] = None):
    """Execute SQL and return single scalar value."""
    result = execute_sql(db, sql, params)
    row = result.fetchone()
    return row[0] if row else None


def execute_sql_one(db, sql: str, params: Optional[dict] = None):
    """Execute SQL and return one row as dict."""
    result = execute_sql(db, sql, params)
    row = result.fetchone()
    if row is None:
        return None
    if _is_postgresql:
        return dict(row._mapping)
    return dict(row)


def execute_sql_all(db, sql: str, params: Optional[dict] = None):
    """Execute SQL and return all rows as list of dicts."""
    result = execute_sql(db, sql, params)
    rows = result.fetchall()
    if _is_postgresql:
        return [dict(r._mapping) for r in rows]
    return [dict(r) for r in rows]


def begin_immediate(db):
    """Begin an immediate/exclusive transaction for atomic operations.

    PostgreSQL: Uses pg_advisory_xact_lock for mutual exclusion.
    SQLite: Uses BEGIN IMMEDIATE.
    """
    if _is_postgresql:
        # Advisory lock ensures only one worker claims a run at a time
        db.execute(text("SELECT pg_advisory_xact_lock(hashtext('claim_next_run'))"))
    # SQLite: BEGIN IMMEDIATE handled via direct execute in repository


def is_postgresql() -> bool:
    """Check if current dialect is PostgreSQL."""
    _detect_dialect()
    return _is_postgresql
