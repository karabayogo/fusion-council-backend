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
import re
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Optional, Union

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import QueuePool

from fusion_council_service.logging_utils import get_logger

logger = get_logger("fusion_council_service.db")

# ── Global engine/session factory (initialized once) ──
_engine = None
_SessionFactory = None
_is_postgresql = False


def _render_schema_sql_for_active_dialect(raw_sql: str) -> str:
    """Normalize schema.sql so one source file can serve SQLite and PostgreSQL."""
    if not _is_postgresql:
        return raw_sql

    lines = []
    for line in raw_sql.split("\n"):
        if line.strip().startswith("PRAGMA "):
            continue
        lines.append(line)
    sql = "\n".join(lines)

    # SQLite autoincrement syntax is invalid in PostgreSQL.
    return re.sub(
        r"\bINTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT\b",
        "BIGSERIAL PRIMARY KEY",
        sql,
        flags=re.IGNORECASE,
    )


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
        # Handle *** placeholder — substitute POSTGRES_PASSWORD from env
        # (same pattern as checkpoint-retention CronJob; needed because
        # psycopg2/libpq does NOT auto-substitute PGPASSWORD when password
        # is explicitly set in the connection URL)
        if ":***@" in db_url:
            pg_password = os.environ.get("POSTGRES_PASSWORD", "")
            if pg_password:
                db_url = db_url.replace(":***@", f":{pg_password}@")
        _engine = create_engine(
            db_url,
            poolclass=QueuePool,
            pool_size=3,
            max_overflow=5,
            pool_pre_ping=True,
            pool_timeout=30,
        )
        _SessionFactory = sessionmaker(bind=_engine)
        logger.info(f"PostgreSQL engine initialized: {db_url.split(chr(64))[0]}@***")
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
    if not _engine:
        _detect_dialect()
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
    """Read schema.sql, create tables, and apply Alembic migrations."""
    if db is None:
        db = new_session()
        _own_session = True
    else:
        _own_session = False

    schema_path = Path(__file__).parent / "schema.sql"
    sql = _render_schema_sql_for_active_dialect(schema_path.read_text())

    if _is_postgresql:
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

    # Run Alembic migrations (replaces the old apply_schema_migrations)
    _run_alembic_migrations()

    if _own_session and _is_postgresql:
        db.close()


def _run_alembic_migrations() -> None:
    """Apply pending Alembic migrations on top of schema.sql base.

    Alembic's own alembic_version table tracks which revisions have been
    applied. On existing databases without alembic_version (pre-Alembic),
    the initial stamp (001) is applied and subsequent migrations (002+)
    are idempotent — they check for column existence before ALTER.

    SKIPPED for SQLite :memory: databases — each SQLAlchemy engine gets
    its own in-memory instance, so Alembic's engine can't see tables
    created by the app's engine. Use schema.sql directly for tests.
    """
    from alembic import command
    from alembic.config import Config as AlembicConfig

    # Skip for in-memory SQLite (tests) — each engine gets its own DB
    db_path = os.environ.get("DATABASE_PATH", "")
    if db_path == ":memory:":
        return

    alembic_cfg_path_env = os.environ.get("ALEMBIC_CFG_PATH", "")
    if alembic_cfg_path_env:
        alembic_cfg_path = Path(alembic_cfg_path_env)
    else:
        alembic_cfg_path = Path(__file__).parents[2] / "alembic.ini"

    if not alembic_cfg_path.exists():
        logger.warning("alembic.ini not found — skipping Alembic migrations")
        return

    alembic_cfg = AlembicConfig(str(alembic_cfg_path))
    alembic_cfg.set_main_option("script_location", str(alembic_cfg_path.parent / "migrations"))

    try:
        command.upgrade(alembic_cfg, "head")
        logger.info("Alembic migrations applied successfully")
    except Exception as exc:
        logger.error("Alembic migrations failed: %s", exc, exc_info=True)
        raise


def _table_columns(db, table_name: str) -> set[str]:
    """Return column names for a table in the active dialect."""
    if _is_postgresql:
        result = db.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = :table_name
                """
            ),
            {"table_name": table_name},
        )
        return {row[0] for row in result.fetchall()}
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table_name):
        raise ValueError(f"Invalid table name: {table_name!r}")
    result = db.execute("PRAGMA table_info(" + table_name + ")")
    return {row[1] for row in result.fetchall()}


def _migration_applied(db, version: str) -> bool:
    execute_sql(
        db,
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
          version TEXT PRIMARY KEY,
          applied_at TEXT NOT NULL
        )
        """,
    )
    commit_tx(db)
    row = execute_sql_one(
        db,
        "SELECT version FROM schema_migrations WHERE version = :version",
        {"version": version},
    )
    return row is not None


def _mark_migration_applied(db, version: str) -> None:
    sql = "INSERT INTO schema_migrations (version, applied_at) VALUES (:version, :applied_at)"
    if _is_postgresql:
        sql += " ON CONFLICT (version) DO NOTHING"
    execute_sql(
        db,
        sql,
        {"version": version, "applied_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")},
    )
    commit_tx(db)


def _backfill_candidate_execution_order(db) -> None:
    """Backfill deterministic per-run execution_order for historical candidates."""
    stage_order = {
        "generation": 10,
        "first_opinion": 20,
        "peer_review": 30,
        "debate": 40,
        "synthesis": 50,
        "verification": 60,
    }
    rows = execute_sql_all(
        db,
        """
        SELECT candidate_id, run_id, stage, created_at
        FROM run_candidates
        WHERE execution_order IS NULL
        ORDER BY run_id, created_at, candidate_id
        """,
    )
    by_run: dict[str, list[dict]] = {}
    for row in rows:
        by_run.setdefault(row["run_id"], []).append(row)
    for run_rows in by_run.values():
        run_rows.sort(key=lambda r: (stage_order.get(r.get("stage"), 999), r.get("created_at") or "", r.get("candidate_id") or ""))
        for idx, row in enumerate(run_rows, start=1):
            execute_sql(
                db,
                "UPDATE run_candidates SET execution_order = :execution_order WHERE candidate_id = :candidate_id",
                {"execution_order": idx, "candidate_id": row["candidate_id"]},
            )
    commit_tx(db)


def _migration_20260516_candidate_execution_order(db) -> None:
    columns = _table_columns(db, "run_candidates")
    if "execution_order" not in columns:
        execute_sql(db, "ALTER TABLE run_candidates ADD COLUMN execution_order INTEGER")
        commit_tx(db)
    _backfill_candidate_execution_order(db)
    execute_sql(
        db,
        "CREATE INDEX IF NOT EXISTS idx_run_candidates_run_order ON run_candidates(run_id, execution_order, created_at, candidate_id)",
    )
    commit_tx(db)


def apply_schema_migrations(db) -> None:
    """Apply idempotent versioned schema migrations after base schema creation."""
    migrations = [
        ("20260516_0001_candidate_execution_order", _migration_20260516_candidate_execution_order),
    ]
    if _is_postgresql:
        execute_sql(db, "SELECT pg_advisory_lock(hashtext(:lock_name))", {"lock_name": "fusion_council_schema_migrations"})
    try:
        for version, migration in migrations:
            if _migration_applied(db, version):
                continue
            migration(db)
            _mark_migration_applied(db, version)
    finally:
        if _is_postgresql:
            execute_sql(db, "SELECT pg_advisory_unlock(hashtext(:lock_name))", {"lock_name": "fusion_council_schema_migrations"})
            commit_tx(db)


def commit_tx(db) -> None:
    """Commit transaction."""
    db.commit()


def rollback_tx(db) -> None:
    """Rollback transaction."""
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
