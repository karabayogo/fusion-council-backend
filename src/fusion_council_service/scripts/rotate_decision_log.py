"""Decision log rotation script — prunes old resolved entries to prevent unbounded growth.

Run as a Kubernetes CronJob (e.g. weekly) to enforce the DECISION_LOG_MAX_ENTRIES
cap on resolved decision log rows.

Usage:
    python -m fusion_council_service.scripts.rotate_decision_log

Environment variables:
    DATABASE_URL           — Postgres or SQLite connection string
    DATABASE_PATH          — SQLite file path (used when DATABASE_URL is not set)
    DECISION_LOG_MAX_ENTRIES  — Max resolved entries to retain (default: 500)
    LOG_LEVEL              — Python log level (default: INFO)
"""

from __future__ import annotations

import logging
import os
import sys

from fusion_council_service.db import initialize_schema, new_session, close_db
from fusion_council_service.domain.decision_log import rotate_decision_log

logging.basicConfig(
    level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO")),
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def main() -> int:
    """Run decision log rotation and report results."""
    max_entries = int(os.environ.get("DECISION_LOG_MAX_ENTRIES", "500"))
    logger.info(
        "Starting decision log rotation (max_resolved_entries=%d)...",
        max_entries,
    )

    db = new_session()
    try:
        initialize_schema(db)
        deleted = rotate_decision_log(db, max_resolved_entries=max_entries)
        logger.info("Rotation complete: %d entries purged", deleted)
        return 0
    except Exception as exc:
        logger.error("Decision log rotation failed: %s", exc, exc_info=True)
        return 1
    finally:
        close_db(db)


if __name__ == "__main__":
    raise SystemExit(main())
