"""Shared startup/shutdown bootstrap for optional LangGraph checkpointing."""
from __future__ import annotations

import asyncio
from contextlib import AbstractAsyncContextManager
from typing import Optional

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from fusion_council_service.config import Settings
from fusion_council_service.logging_utils import get_logger

logger = get_logger("fusion_council_service.startup")

MAX_STARTUP_RETRIES = 10
STARTUP_RETRY_DELAY_SEC = 5

# 5 minutes — matches runner token TTL per plan spec
STALE_THRESHOLD_SEC = 300

_checkpoint_cm: Optional[AbstractAsyncContextManager] = None
_checkpoint_saver: Optional[AsyncPostgresSaver] = None


def _checkpoint_db_url(settings: Settings) -> str:
    return settings.LANGGRAPH_CHECKPOINT_DB_URL or settings.DATABASE_URL


def get_checkpoint_saver() -> Optional[AsyncPostgresSaver]:
    return _checkpoint_saver


async def _init_checkpointer_with_retries(settings: Settings) -> Optional[AsyncPostgresSaver]:
    if not settings.LANGGRAPH_CHECKPOINT_ENABLED:
        logger.info("LangGraph checkpointing disabled by config")
        return None

    db_url = _checkpoint_db_url(settings)
    if not db_url:
        logger.warning("LANGGRAPH_CHECKPOINT_ENABLED=true but checkpoint DB URL is empty")
        return None

    global _checkpoint_cm
    for attempt in range(MAX_STARTUP_RETRIES):
        try:
            _checkpoint_cm = AsyncPostgresSaver.from_conn_string(db_url)
            savor = await _checkpoint_cm.__aenter__()
            await savor.setup()
            logger.info("AsyncPostgresSaver initialized successfully")
            return savor
        except Exception as exc:
            logger.warning(
                f"AsyncPostgresSaver init attempt {attempt + 1}/{MAX_STARTUP_RETRIES} failed: {exc}"
            )
            if _checkpoint_cm is not None:
                try:
                    await _checkpoint_cm.__aexit__(None, None, None)
                except Exception:
                    pass
                _checkpoint_cm = None
            if attempt < MAX_STARTUP_RETRIES - 1:
                await asyncio.sleep(STARTUP_RETRY_DELAY_SEC * (attempt + 1))
            else:
                logger.error("AsyncPostgresSaver init failed after retries — continuing without checkpointer")
                return None
    return None


async def run_startup(settings: Settings) -> None:
    global _checkpoint_saver
    _checkpoint_saver = await _init_checkpointer_with_retries(settings)


async def run_shutdown() -> None:
    global _checkpoint_cm, _checkpoint_saver
    _checkpoint_saver = None
    if _checkpoint_cm is not None:
        try:
            await _checkpoint_cm.__aexit__(None, None, None)
        except Exception as exc:
            logger.warning(f"AsyncPostgresSaver shutdown failed: {exc}")
        _checkpoint_cm = None


def run_startup_sync(settings: Settings) -> None:
    asyncio.run(run_startup(settings))


def run_shutdown_sync() -> None:
    asyncio.run(run_shutdown())


async def _recover_stale_runs(settings: Settings) -> int:
    """
    Scan for runs stuck in orchestration_status='resumed' for > STALE_THRESHOLD_SEC.

    For each stale run:
      - If no saver available: mark abandoned
      - If checkpoint exists in LangGraph store: re-queue (increment resume_count)
      - If no checkpoint: mark abandoned

    Returns count of recovered (re-queued) runs.
    Called on every worker idle cycle and on worker startup after run_startup() completes.

    NOTE: Do NOT call inside run_startup() — if Postgres was down and saver is None,
    this would immediately abandon everything.
    """
    import asyncpg

    db_url = _checkpoint_db_url(settings)
    if not db_url:
        logger.warning("_recover_stale_runs: no checkpoint DB URL configured")
        return 0

    raw_url = db_url.replace("postgresql+asyncpg://", "postgresql://")

    try:
        pool = await asyncpg.create_pool(raw_url, min_size=1, max_size=5)
    except Exception as exc:
        logger.error(f"_recover_stale_runs: failed to create asyncpg pool: {exc}")
        return 0

    try:
        async with pool.acquire() as conn:
            # Fetch all stale resumed runs in one query
            stale_runs = await conn.fetch(
                """
                SELECT run_id, thread_id, orchestrator_mode, resume_count, updated_at
                FROM run_orchestration_state
                WHERE orchestration_status = 'resumed'
                  AND updated_at < (NOW() - INTERVAL '1 second' * $1)
                ORDER BY updated_at ASC
                LIMIT 50
                """,
                STALE_THRESHOLD_SEC,
            )

            recovered = 0
            for row in stale_runs:
                run_id = row["run_id"]
                thread_id = row["thread_id"]
                orchestrator_mode = row["orchestrator_mode"]

                saver = get_checkpoint_saver()
                if saver is None:
                    # No saver available — cannot resume. Mark abandoned.
                    await conn.execute(
                        """
                        UPDATE run_orchestration_state
                        SET orchestration_status = 'abandoned', updated_at = NOW()
                        WHERE run_id = $1
                        """,
                        run_id,
                    )
                    logger.warning(f"Stale run {run_id} abandoned — no checkpointer available")
                    continue

                try:
                    config = {"thread_id": thread_id, "checkpoint_namespace": orchestrator_mode}
                    checkpoint = await saver.get(config)
                    if checkpoint is None:
                        # No checkpoint found — cannot resume. Mark abandoned.
                        await conn.execute(
                            """
                            UPDATE run_orchestration_state
                            SET orchestration_status = 'abandoned', updated_at = NOW()
                            WHERE run_id = $1
                            """,
                            run_id,
                        )
                        logger.warning(f"Stale run {run_id} abandoned — no checkpoint found")
                    else:
                        # Checkpoint exists — re-queue for resumption
                        await conn.execute(
                            """
                            UPDATE run_orchestration_state
                            SET orchestration_status = 'resumed',
                                resume_count = resume_count + 1,
                                updated_at = NOW()
                            WHERE run_id = $1
                            """,
                            run_id,
                        )
                        logger.info(f"Stale run {run_id} flagged for resumption")
                        recovered += 1
                except Exception as exc:
                    logger.error(f"Error recovering stale run {run_id}: {exc}")

            return recovered
    finally:
        await pool.close()
