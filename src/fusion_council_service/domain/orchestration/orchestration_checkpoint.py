"""LangGraph checkpoint I/O — thread management, version gating, and table init."""
from __future__ import annotations

import uuid
import logging
from typing import TYPE_CHECKING

from fusion_council_service.config import Settings

if TYPE_CHECKING:
    import asyncpg
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

logger = logging.getLogger(__name__)

LANGGRAPH_ENGINE_VERSION = getattr(Settings(), "LANGGRAPH_ENGINE_VERSION", "v1")


class OrchestrationEngineVersionMismatch(Exception):
    """Raised when checkpoint engine_version does not match current LANGGRAPH_ENGINE_VERSION."""
    pass


async def get_or_create_thread_id(
    conn: "asyncpg.Connection",
    run_id: str,
    mode: str,
) -> tuple[dict, bool]:
    """
    Returns (langgraph_config, is_resume).

    langgraph_config: dict with keys "thread_id" and "checkpoint_namespace" —
                      pass to graph.compile().aget_state(config) or graph.compile().ainvoke(input, config)
    is_resume: True = replay from last checkpoint (aget_state), False = fresh run (ainvoke)

    Decision logic:
      - Query run_orchestration_state for row WHERE run_id = $1
      - If row exists AND orchestration_status IN ('resumed', 'started'):
          -> is_resume = True  (orphaned run recovery or mid-run resume)
          -> Return config = {"thread_id": row["thread_id"], "checkpoint_namespace": row["checkpoint_namespace"]}
      - If row does not exist OR orchestration_status IN ('completed', 'failed', 'abandoned'):
          -> is_resume = False (fresh run)
          -> Generate new thread_id = str(uuid.uuid4())
          -> Generate new checkpoint_ns = f"mode={mode}"
          -> INSERT new row with orchestration_status='started'
          -> Return config = {"thread_id": thread_id, "checkpoint_namespace": checkpoint_ns}
    """
    rows = await conn.fetch(
        """
        SELECT run_id, thread_id, checkpoint_ns, orchestration_status
        FROM run_orchestration_state
        WHERE run_id = $1
        """,
        run_id,
    )

    if rows and rows[0]["orchestration_status"] in ("resumed", "started"):
        # Resume path — checkpoint exists and is valid for replay
        row = rows[0]
        langgraph_config = {
            "thread_id": row["thread_id"],
            "checkpoint_namespace": row["checkpoint_namespace"],
        }
        logger.info(
            f"get_or_create_thread_id: resume run_id={run_id} "
            f"thread_id={row['thread_id']} status={row['orchestration_status']}"
        )
        return langgraph_config, True

    # Fresh run — generate new thread_id and insert row
    thread_id = str(uuid.uuid4())
    checkpoint_ns = f"mode={mode}"

    await conn.execute(
        """
        INSERT INTO run_orchestration_state
            (run_id, thread_id, checkpoint_ns, orchestration_status, resume_count, updated_at)
        VALUES ($1, $2, $3, 'started', 0, NOW())
        ON CONFLICT (run_id) DO NOTHING
        """,
        run_id,
        thread_id,
        checkpoint_ns,
    )

    logger.info(
        f"get_or_create_thread_id: fresh run_id={run_id} "
        f"thread_id={thread_id} checkpoint_ns={checkpoint_ns}"
    )
    return {"thread_id": thread_id, "checkpoint_namespace": checkpoint_ns}, False


async def ensure_langgraph_checkpoint_tables(conn: "asyncpg.Connection") -> None:
    """
    Ensure LangGraph checkpoint/thread tables exist in the Postgres database.

    Creates:
      - checkpoint (LangGraph internal — stores graph snapshots per thread_id/checkpoint_ns)
      - checkpoint_metadata (LangGraph internal — thread metadata)

    Idempotent: uses CREATE TABLE IF NOT EXISTS.
    """
    import asyncpg

    try:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS checkpoint (
                thread_id   TEXT NOT NULL,
                checkpoint_ns TEXT NOT NULL DEFAULT '',
                checkpoint_id TEXT NOT NULL,
                parent_checkpoint_id TEXT,
                state       JSONB NOT NULL,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id)
            )
            """
        )
    except asyncpg.DuplicateTableError:
        pass  # already exists

    try:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS checkpoint_metadata (
                thread_id      TEXT NOT NULL PRIMARY KEY,
                checkpoint_ns   TEXT NOT NULL DEFAULT '',
                created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                metadata       JSONB
            )
            """
        )
    except asyncpg.DuplicateTableError:
        pass  # already exists

    logger.info("ensure_langgraph_checkpoint_tables: tables verified/created")


def check_engine_version_compatible(stored_version: str, current_version: str) -> None:
    """
    Verify checkpoint engine version matches current LANGGRAPH_ENGINE_VERSION.

    Raises:
        OrchestrationEngineVersionMismatch: if versions differ
    """
    if stored_version != current_version:
        raise OrchestrationEngineVersionMismatch(
            f"Engine version mismatch: stored={stored_version!r}, current={current_version!r}. "
            "Refusing to replay. Mark run failed and do not auto-retry."
        )


def get_checkpoint_snapshot(
    saver: "AsyncPostgresSaver",
    config: dict,
) -> dict | None:
    """
    Fetch the latest checkpoint snapshot from the LangGraph checkpointer.

    Args:
        saver: AsyncPostgresSaver instance (from get_checkpoint_saver())
        config: LangGraph config dict with keys "thread_id" and "checkpoint_ns"

    Returns:
        The checkpoint's ``channel_values`` dict if found, else None.
        Returns None on any error (saver unavailable, no checkpoint, etc).

    Typical usage::

        from fusion_council_service.startup import get_checkpoint_saver
        from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
            get_checkpoint_snapshot,
        )

        saver = get_checkpoint_saver()
        if saver is not None:
            config = {"thread_id": thread_id, "checkpoint_ns": checkpoint_ns}
            snapshot = get_checkpoint_snapshot(saver, config)
            if snapshot is not None:
                # resume from snapshot["run_id"], etc.
    """
    import asyncio

    try:
        # aget is the async load method on AsyncPostgresSaver
        checkpoint = asyncio.run(saver.aget(config)) if asyncio.iscoroutinefunction(saver.aget) else None
    except Exception:
        return None

    if checkpoint is None:
        return None

    # The checkpoint dict uses "channel_values" for the actual state after LangGraph v1.
    if isinstance(checkpoint, dict) and "channel_values" in checkpoint:
        return checkpoint["channel_values"]
    # Fallback: some versions surface the state directly at the top level.
    if isinstance(checkpoint, dict):
        # Strip known LangGraph internal keys; pass through everything else as the snapshot.
        internal_keys = {"v", "ts", "id", "parent_checkpoint_id", "channel_versions", "versions_seen"}
        snapshot = {k: v for k, v in checkpoint.items() if k not in internal_keys}
        return snapshot if snapshot else None
    return None
