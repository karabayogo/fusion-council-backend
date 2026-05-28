"""Legacy orchestration engine wrapper."""

from fusion_council_service.domain.orchestration.orchestration_state_repository import (
    upsert_orchestration_state,
)


class LegacyEngine:
    async def _run(self, db, run: dict, worker_ctx: dict, mode: str) -> None:
        run_id = run["run_id"]
        upsert_orchestration_state(
            db,
            run_id=run_id,
            thread_id=f"legacy:{mode}:{run_id}",
            orchestrator_engine="legacy",
            orchestrator_mode=mode,
            engine_version="v1",
            orchestration_status="running",
            last_checkpoint_id=f"{mode}:start",
        )
        try:
            await worker_ctx["worker"].run_mode_legacy(db, run, mode)
            upsert_orchestration_state(
                db,
                run_id=run_id,
                thread_id=f"legacy:{mode}:{run_id}",
                orchestrator_engine="legacy",
                orchestrator_mode=mode,
                engine_version="v1",
                orchestration_status="succeeded",
                last_checkpoint_id=f"{mode}:completed",
            )
        except Exception as exc:
            upsert_orchestration_state(
                db,
                run_id=run_id,
                thread_id=f"legacy:{mode}:{run_id}",
                orchestrator_engine="legacy",
                orchestrator_mode=mode,
                engine_version="v1",
                orchestration_status="failed",
                last_checkpoint_id=f"{mode}:failed",
                last_error_code="LEGACY_MODE_EXCEPTION",
                last_error_message=str(exc),
            )
            raise

    async def run_single(self, db, run: dict, worker_ctx: dict) -> None:
        await self._run(db, run, worker_ctx, "single")

    async def run_fusion(self, db, run: dict, worker_ctx: dict) -> None:
        await self._run(db, run, worker_ctx, "fusion")

    async def run_council(self, db, run: dict, worker_ctx: dict) -> None:
        await self._run(db, run, worker_ctx, "council")
