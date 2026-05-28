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
            saver = await _checkpoint_cm.__aenter__()
            await saver.setup()
            logger.info("AsyncPostgresSaver initialized successfully")
            return saver
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

