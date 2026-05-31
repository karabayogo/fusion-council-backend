"""Checkpoint retention cronjob — reports checkpoint table sizes for monitoring.

Run as a Kubernetes CronJob (e.g. daily) to prevent unbounded growth of the
checkpoint tables. Currently reports table sizes only — LangGraph's checkpoint
schema (checkpoints/checkpoint_writes/checkpoint_blobs) does not have row-level
timestamps, so time-based retention requires joining across tables.

TODO: Implement proper retention by finding old checkpoints via
      checkpoints.checkpoint->>'ts' JSONB extraction, then cascading
      deletes through checkpoint_writes and checkpoint_blobs using
      (thread_id, checkpoint_ns, checkpoint_id) foreign keys.

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

import asyncpg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


async def report_table_sizes(conn: asyncpg.pool.PoolConnectionProxy) -> dict:
    """Report current row counts for all LangGraph checkpoint tables."""
    tables = ["checkpoints", "checkpoint_writes", "checkpoint_blobs"]
    counts = {}
    for tbl in tables:
        count = await conn.fetchval(f"SELECT COUNT(*) FROM {tbl}")
        counts[tbl] = count or 0
    return counts


async def main() -> int:
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        logger.error("DATABASE_URL not set — cannot connect to checkpoint DB")
        return 1

    # Substitute POSTGRES_PASSWORD into the *** placeholder in DATABASE_URL.
    pg_password = os.environ.get("POSTGRES_PASSWORD", "")
    if pg_password and ":***@" in db_url:
        db_url = db_url.replace(":***@", f":{pg_password}@")

    retention_days = int(os.environ.get("CHECKPOINT_RETENTION_DAYS", "7"))
    if retention_days < 1:
        logger.error("CHECKPOINT_RETENTION_DAYS must be >= 1, got %d", retention_days)
        return 1

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
            counts = await report_table_sizes(conn)
            total = sum(counts.values())
            logger.info(
                "checkpoint_retention: table sizes — checkpoints=%d writes=%d blobs=%d (total=%d, retention_days=%d)",
                counts["checkpoints"],
                counts["checkpoint_writes"],
                counts["checkpoint_blobs"],
                total,
                retention_days,
            )
            if total == 0:
                logger.info("checkpoint_retention: no checkpoint data, nothing to prune")
                return 0
            logger.info(
                "checkpoint_retention: time-based deletion not yet implemented "
                "(LangGraph checkpoint tables use thread_id/checkpoint_id, not row timestamps). "
                "Total checkpoint rows: %d. See script TODO for implementation plan.",
                total,
            )
            return 0
    except Exception as exc:
        logger.error("checkpoint_retention failed: %s", exc)
        return 1
    finally:
        await pool.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
