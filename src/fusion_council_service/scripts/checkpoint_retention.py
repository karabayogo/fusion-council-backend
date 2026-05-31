"""Checkpoint retention cronjob — purges LangGraph checkpoint/thread rows older than RETENTION_DAYS.

Run as a Kubernetes CronJob (e.g. daily) to prevent unbounded growth of the
checkpoint tables. Targets the same Postgres DB used by LANGGRAPH_CHECKPOINT_DB_URL.

Usage:
    python -m fusion_council_service.scripts.checkpoint_retention

Environment variables:
    DATABASE_URL           — Postgres connection string (postgresql://...)
    CHECKPOINT_RETENTION_DAYS  — Days to keep checkpoints (default: 7)
    LOG_LEVEL              — Python log level (default: INFO)
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from datetime import UTC, datetime, timedelta

import asyncpg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


async def purge_old_checkpoints(conn: asyncpg.pool.PoolConnectionProxy, retention_days: int) -> int:
    """
    Delete checkpoint and checkpoint_metadata rows older than retention_days.

    The LangGraph checkpoint tables (checkpoint, checkpoint_metadata) grow
    unbounded if not pruned. This function removes rows whose created_at
    is older than the cutoff, matching the retention policy.

    Returns the total number of rows deleted.
    """
    cutoff = datetime.now(tz=UTC) - timedelta(days=retention_days)
    cutoff_str = cutoff.isoformat().replace("+00:00", "Z")

    # Count before delete for logging
    count_checkpoints = await conn.fetchval(
        "SELECT COUNT(*) FROM checkpoint WHERE created_at < $1",
        cutoff_str,
    )
    count_meta = await conn.fetchval(
        "SELECT COUNT(*) FROM checkpoint_metadata WHERE created_at < $1",
        cutoff_str,
    )

    total_before = (count_checkpoints or 0) + (count_meta or 0)
    if total_before == 0:
        logger.info(
            "checkpoint_retention: no rows older than %d days (cutoff %s)",
            retention_days,
            cutoff_str,
        )
        return 0

    # Delete checkpoint rows (child first — checkpoint has more rows typically).
    await conn.execute("DELETE FROM checkpoint WHERE created_at < $1", cutoff_str)
    deleted_checkpoints = count_checkpoints or 0

    await conn.execute("DELETE FROM checkpoint_metadata WHERE created_at < $1", cutoff_str)
    deleted_meta = count_meta or 0

    deleted_total = deleted_checkpoints + deleted_meta

    logger.info(
        "checkpoint_retention: deleted %d checkpoint rows + %d metadata rows "
        "(retention_days=%d, cutoff=%s)",
        deleted_checkpoints,
        deleted_meta,
        retention_days,
        cutoff_str,
    )
    return deleted_total


async def main() -> int:
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        logger.error("DATABASE_URL not set — cannot connect to checkpoint DB")
        return 1

    # Substitute POSTGRES_PASSWORD into the *** placeholder in DATABASE_URL.
    # The Helm chart templates construct DATABASE_URL with a "***" placeholder
    # and separately inject POSTGRES_PASSWORD from a Kubernetes Secret.
    pg_password = os.environ.get("POSTGRES_PASSWORD", "")
    if pg_password and ":***@" in db_url:
        db_url = db_url.replace(":***@", f":{pg_password}@")

    retention_days = int(os.environ.get("CHECKPOINT_RETENTION_DAYS", "7"))
    if retention_days < 1:
        logger.error("CHECKPOINT_RETENTION_DAYS must be >= 1, got %d", retention_days)
        return 1

    # Strip asyncpg driver prefix for plain asyncpg connection
    dsn = db_url.replace("postgresql+asyncpg://", "postgresql://")

    try:
        pool = await asyncpg.create_pool(
            dsn,
            min_size=1,
            max_size=3,
            command_timeout=60,
        )
    except Exception as exc:
        logger.error("Failed to create connection pool: %s", exc)
        return 1

    try:
        async with pool.acquire() as conn:
            deleted = await purge_old_checkpoints(conn, retention_days)
            logger.info(
                "checkpoint_retention completed: %d rows deleted, retention_days=%d",
                deleted,
                retention_days,
            )
            return 0
    except Exception as exc:
        logger.error("checkpoint_retention failed: %s", exc)
        return 1
    finally:
        await pool.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))