"""LangGraph orchestration engine.

This engine currently wraps existing run-mode implementations while persisting
durable orchestration metadata (thread IDs, status, checkpoint marker).
"""

from fusion_council_service.domain.orchestration.orchestration_state_repository import (
    get_orchestration_state,
    upsert_orchestration_state,
)
from fusion_council_service.domain.run_repository import get_run
from fusion_council_service.logging_utils import get_logger
from fusion_council_service.startup import get_checkpoint_saver

logger = get_logger("fusion_council_service.orchestration.langgraph_engine")


class LangGraphEngine:
    def __init__(self, thread_namespace: str = "fusion-council", engine_version: str = "v1"):
        self._thread_namespace = thread_namespace
        self._engine_version = engine_version

    def _thread_id(self, run_id: str, mode: str) -> str:
        return f"{self._thread_namespace}:{mode}:{run_id}"

    async def _run_mode(self, db, run: dict, worker_ctx: dict, mode: str) -> None:
        run_id = run["run_id"]
        thread_id = self._thread_id(run_id, mode)
        previous = get_orchestration_state(db, run_id)
        resume_increment = previous is not None and previous.get("orchestration_status") not in {
            "succeeded",
            "failed",
        }
        saver = get_checkpoint_saver()
        if saver is None:
            logger.warning("LangGraph checkpointer unavailable; continuing without persisted graph checkpoints", run_id=run_id)

        upsert_orchestration_state(
            db,
            run_id=run_id,
            thread_id=thread_id,
            orchestrator_engine="langgraph",
            orchestrator_mode=mode,
            engine_version=self._engine_version,
            orchestration_status="running",
            last_checkpoint_id=f"{mode}:start",
            resume_count_increment=resume_increment,
        )

        current = get_run(db, run_id)
        if current and current.get("status") in {"succeeded", "succeeded_degraded", "failed"}:
            upsert_orchestration_state(
                db,
                run_id=run_id,
                thread_id=thread_id,
                orchestrator_engine="langgraph",
                orchestrator_mode=mode,
                engine_version=self._engine_version,
                orchestration_status="succeeded",
                last_checkpoint_id=f"{mode}:noop_terminal",
            )
            return

        try:
            await worker_ctx["worker"].run_mode_legacy(db, run, mode)
            upsert_orchestration_state(
                db,
                run_id=run_id,
                thread_id=thread_id,
                orchestrator_engine="langgraph",
                orchestrator_mode=mode,
                engine_version=self._engine_version,
                orchestration_status="succeeded",
                last_checkpoint_id=f"{mode}:completed",
            )
        except Exception as exc:
            logger.error(
                f"LangGraph engine mode failed: {exc}",
                run_id=run_id,
                mode=mode,
            )
            upsert_orchestration_state(
                db,
                run_id=run_id,
                thread_id=thread_id,
                orchestrator_engine="langgraph",
                orchestrator_mode=mode,
                engine_version=self._engine_version,
                orchestration_status="failed",
                last_checkpoint_id=f"{mode}:failed",
                last_error_code="LANGGRAPH_MODE_EXCEPTION",
                last_error_message=str(exc),
            )
            raise

    async def run_single(self, db, run: dict, worker_ctx: dict) -> None:
        await self._run_mode(db, run, worker_ctx, "single")

    async def run_fusion(self, db, run: dict, worker_ctx: dict) -> None:
        await self._run_mode(db, run, worker_ctx, "fusion")

    async def run_council(self, db, run: dict, worker_ctx: dict) -> None:
        await self._run_mode(db, run, worker_ctx, "council")
