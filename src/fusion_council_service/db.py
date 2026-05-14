"""Database connection and schema initialization."""

import os
import sqlite3
from pathlib import Path

from fusion_council_service.logging_utils import get_logger

logger = get_logger("fusion_council_service.db")

_DEFAULT_BUSY_TIMEOUT = 30  # seconds


def open_db_connection(database_path: str) -> sqlite3.Connection:
    """Open SQLite connection with foreign keys and busy timeout.

    Default to rollback-journal mode instead of WAL. WAL is unsafe on the RWX
    Longhorn/NFS-style mount used by the dev API + worker pods because readers
    on different nodes can observe inconsistent WAL state. Operators can still
    override this with SQLITE_JOURNAL_MODE for local-disk deployments.
    """
    # Reject obvious network filesystem paths. Kubernetes PVC mount paths do not
    # expose the backing filesystem type in the path, so journal_mode must also
    # be safe by default.
    _reject_network_path(database_path)

    # Ensure parent directory exists
    Path(database_path).parent.mkdir(parents=True, exist_ok=True)

    timeout = int(os.environ.get("SQLITE_BUSY_TIMEOUT", str(_DEFAULT_BUSY_TIMEOUT)))
    journal_mode = os.environ.get("SQLITE_JOURNAL_MODE", "DELETE").strip().upper()
    synchronous = os.environ.get("SQLITE_SYNCHRONOUS", "FULL").strip().upper()

    db = sqlite3.connect(database_path, timeout=timeout)
    db.row_factory = sqlite3.Row
    db.execute(f"PRAGMA journal_mode={journal_mode}")
    db.execute(f"PRAGMA synchronous={synchronous}")
    db.execute("PRAGMA foreign_keys=ON")
    db.execute(f"PRAGMA busy_timeout={timeout * 1000}")
    logger.info(f"SQLite busy_timeout set to {timeout}s")

    actual_journal_mode = db.execute("PRAGMA journal_mode").fetchone()[0]
    fk_status = db.execute("PRAGMA foreign_keys").fetchone()[0]
    logger.info(
        f"DB opened: journal_mode={actual_journal_mode}, synchronous={synchronous}, "
        f"foreign_keys={fk_status}"
    )

    return db


def initialize_schema(db: sqlite3.Connection) -> None:
    """Read and execute schema.sql to create tables if they do not exist."""
    schema_path = Path(__file__).parent / "schema.sql"
    sql = schema_path.read_text()
    db.executescript(sql)
    logger.info("Schema initialized")


def _reject_network_path(database_path: str) -> None:
    """Reject database paths that appear to be on network filesystems."""
    lower = database_path.lower()
    forbidden = ["nfs", "smb", "//", "/net/"]
    for token in forbidden:
        if token in lower:
            raise ValueError(
                f"DATABASE_PATH '{database_path}' appears to be a network filesystem path. "
                f"SQLite must be on local disk. Found '{token}' in path."
            )