"""Routes runs to legacy/langgraph/shadow engines."""

from fusion_council_service.domain.orchestration.orchestration_state_repository import (
    insert_shadow_diff,
)
from fusion_council_service.domain.run_repository import get_run
from fusion_council_service.logging_utils import get_logger

logger = get_logger("fusion_council_service.orchestration.router")

SUPPORTED_MODES = {"single", "fusion", "council"}


def parse_langgraph_modes(value: str) -> set[str]:
    modes = {m.strip().lower() for m in (value or "").split(",") if m.strip()}
    return {m for m in modes if m in SUPPORTED_MODES}


class OrchestrationEngineRouter:
    def __init__(
        self,
        *,
        orchestrator_engine: str,
        langgraph_modes: set[str],
        legacy_engine,
        langgraph_engine,
    ):
        self._orchestrator_engine = (orchestrator_engine or "legacy").strip().lower()
        if self._orchestrator_engine not in {"legacy", "langgraph", "shadow"}:
            self._orchestrator_engine = "legacy"
        self._langgraph_modes = set(langgraph_modes)
        self._legacy_engine = legacy_engine
        self._langgraph_engine = langgraph_engine

    async def _execute_with_engine(self, engine, mode: str, db, run: dict, worker_ctx: dict) -> None:
        if mode == "single":
            await engine.run_single(db, run, worker_ctx)
        elif mode == "fusion":
            await engine.run_fusion(db, run, worker_ctx)
        else:
            await engine.run_council(db, run, worker_ctx)

    async def execute(self, *, db, run: dict, worker_ctx: dict) -> None:
        mode = (run.get("mode") or "").lower()
        if mode not in SUPPORTED_MODES:
            raise ValueError(f"Unknown mode: {mode}")

        if self._orchestrator_engine == "legacy":
            await self._execute_with_engine(self._legacy_engine, mode, db, run, worker_ctx)
            return

        langgraph_mode_enabled = mode in self._langgraph_modes

        if self._orchestrator_engine == "langgraph":
            if langgraph_mode_enabled:
                await self._execute_with_engine(self._langgraph_engine, mode, db, run, worker_ctx)
            else:
                await self._execute_with_engine(self._legacy_engine, mode, db, run, worker_ctx)
            return

        # shadow mode: legacy remains source-of-truth, langgraph runs best-effort comparison pass.
        await self._execute_with_engine(self._legacy_engine, mode, db, run, worker_ctx)

        if not langgraph_mode_enabled:
            return

        try:
            legacy_snapshot = get_run(db, run["run_id"]) or {}
        except Exception:
            legacy_snapshot = {}
        try:
            await self._execute_with_engine(self._langgraph_engine, mode, db, run, worker_ctx)
            try:
                langgraph_snapshot = get_run(db, run["run_id"]) or {}
            except Exception:
                langgraph_snapshot = {}
            try:
                insert_shadow_diff(
                    db,
                    run_id=run["run_id"],
                    engine="langgraph",
                    final_status=langgraph_snapshot.get("status"),
                    final_answer_present=bool(langgraph_snapshot.get("final_answer")),
                    stage_count=None,
                    stage_order_match=legacy_snapshot.get("current_stage") == langgraph_snapshot.get("current_stage"),
                    candidate_counts={},
                    error_codes=[],
                    diff_summary={
                        "legacy_status": legacy_snapshot.get("status"),
                        "langgraph_status": langgraph_snapshot.get("status"),
                    },
                )
            except Exception:
                pass
        except Exception as exc:
            logger.warning(
                f"Shadow langgraph execution failed: {exc}",
                run_id=run.get("run_id"),
            )
            try:
                insert_shadow_diff(
                    db,
                    run_id=run["run_id"],
                    engine="langgraph",
                    final_status="error",
                    final_answer_present=False,
                    stage_count=None,
                    stage_order_match=False,
                    candidate_counts={},
                    error_codes=["SHADOW_LANGGRAPH_EXCEPTION"],
                    diff_summary={"error": str(exc)},
                )
            except Exception:
                pass
