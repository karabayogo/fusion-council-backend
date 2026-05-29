"""
LangGraph fine-grained nodes for single-mode orchestration (Option A).

Each async node performs its specific stage of work with idempotency guards:
  node_prepare_run      → validates run, selects model
  node_generation_call  → calls model API via provider registry
  node_generation_persist → persists candidate to run_candidates
  node_finalize_success → writes final_answer, emits events
  node_finalize_failure → writes error state, emits failure event

Worker dependencies are passed through LangGraph's RunnableConfig.configurable dict.
"""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional

from fusion_council_service.clock import utc_now_iso
from fusion_council_service.ids import new_candidate_id
from fusion_council_service.logging_utils import get_logger

if TYPE_CHECKING:
    from langgraph.types import RunnableConfig

    from fusion_council_service.domain.orchestration.orchestration_state import (
        OrchestrationState,
    )

logger = get_logger("fusion_council_service.orchestration.nodes_single")

# Default deadline for single-mode runs (seconds)
SINGLE_DEADLINE_SEC = 300


def _get_worker(config: RunnableConfig) -> Optional[object]:
    """Extract Worker from configurable dict."""
    if config and "configurable" in config:
        return config["configurable"].get("worker")
    return None


def _full_state(state: OrchestrationState, **overrides: object) -> OrchestrationState:
    """Return complete OrchestrationState — required for total=False TypedDict."""
    result: OrchestrationState = {  # type: ignore[typeddict-item]
        "run_id": state.get("run_id"),
        "mode": state.get("mode"),
        "engine": state.get("engine"),
        "engine_version": state.get("engine_version"),
        "thread_id": state.get("thread_id"),
        "checkpoint_namespace": state.get("checkpoint_namespace"),
        "resume_count": state.get("resume_count"),
        "current_stage": state.get("current_stage"),
        "candidate_ids": state.get("candidate_ids", []),
        "current_candidate_id": state.get("current_candidate_id"),
        "final_answer": state.get("final_answer"),
        "final_confidence": state.get("final_confidence"),
        "error_code": state.get("error_code"),
        "error_message": state.get("error_message"),
        "updated_at": state.get("updated_at"),
        "raw_response": state.get("raw_response"),
        "candidate_summaries": state.get("candidate_summaries"),
        "computed_final_answer": state.get("computed_final_answer"),
        "computed_final_confidence": state.get("computed_final_confidence"),
    }
    result.update(overrides)  # type: ignore[arg-type]
    return result


# ──────────────────────────────────────────────────────────────────
# Node implementations
# ──────────────────────────────────────────────────────────────────

async def node_prepare_run(
    state: OrchestrationState,
    config: RunnableConfig,
) -> OrchestrationState:
    """Entry node — validates run_id, selects model, computes budget."""
    run_id = state.get("run_id")
    if not run_id:
        return _full_state(
            state,
            current_stage="finalize_failure",
            error_code="RUN_ID_MISSING",
            error_message="run_id not found in state",
        )

    current = state.get("current_stage", "")
    if current.startswith(("generation", "finalize")):
        return state  # already past prepare — replay guard

    worker = _get_worker(config)
    if worker is None:
        logger.warning("node_prepare_run: no worker in config", run_id=run_id)
        return _full_state(state, current_stage="prepare_run")

    try:
        from fusion_council_service.domain.budget import (
            compute_budget,
            select_models_for_mode,
        )

        mode = state.get("mode", "single")
        # select_models_for_mode(mode, catalog)
        models = select_models_for_mode(mode, worker._catalog)
        if not models:
            return _full_state(
                state,
                current_stage="finalize_failure",
                error_code="NO_MODELS",
                error_message=f"No models for mode {mode}",
            )
        # compute_budget(mode, deadline_seconds)
        budget = compute_budget(mode, SINGLE_DEADLINE_SEC)
        logger.info(
            "node_prepare_run: models=%d stages=%d",
            len(models),
            len(budget.stages),
            run_id=run_id,
        )

        return _full_state(state, current_stage="prepare_run")
    except Exception as exc:
        logger.error(f"node_prepare_run failed: {exc}", run_id=run_id)
        return _full_state(
            state,
            current_stage="finalize_failure",
            error_code="PREPARE_RUN_ERROR",
            error_message=str(exc),
        )


async def node_generation_call(
    state: OrchestrationState,
    config: RunnableConfig,
) -> OrchestrationState:
    """Call model API to generate a response for single mode."""
    current = state.get("current_stage", "")
    if current.startswith(("generation_persist", "finalize")):
        return state  # already past — replay guard

    worker = _get_worker(config)
    if worker is None:
        return _full_state(
            state,
            current_stage="finalize_failure",
            error_code="NO_WORKER",
            error_message="No worker for generation call",
        )

    run_id = state["run_id"]
    try:
        from fusion_council_service.domain.model_selection import (
            select_healthy_stage_model,
        )
        from fusion_council_service.domain.types import ProviderGenerateRequest
        from fusion_council_service.domain.structured_output import (
            invoke_structured_or_freetext,
        )

        db = worker._get_db()
        # select_healthy_stage_model(*, db, catalog, run_id, role_order, avoid_aliases)
        model_info = select_healthy_stage_model(
            db=db,
            catalog=worker._catalog,
            run_id=run_id,
            role_order=["generation"],
        )
        if model_info is None:
            return _full_state(
                state,
                current_stage="finalize_failure",
                error_code="NO_HEALTHY_MODEL",
                error_message="No healthy model for generation",
            )

        alias = model_info.get("alias", "unknown")
        provider_name = model_info.get("provider", "")
        provider_model = model_info.get("provider_model", "")

        candidate_id = new_candidate_id()
        started_at = utc_now_iso()

        request = ProviderGenerateRequest(
            alias=alias,
            provider=provider_name,
            provider_model=provider_model,
            user_prompt=state.get("computed_final_answer") or "",
            system_prompt="",
            max_output_tokens=4096,
            temperature=0.7,
        )

        # Model call is blocking — run in thread pool
        loop = asyncio.get_running_loop()

        def _call_model():
            return invoke_structured_or_freetext(
                request,
                worker._registry,
                response_model=type(None),
            )

        result = await loop.run_in_executor(ThreadPoolExecutor(max_workers=1), _call_model)

        finished_at = utc_now_iso()
        raw_response = {
            "model": provider_model,
            "choices": [{"message": {"role": "assistant", "content": result.raw_text}}],
            "usage": {
                "prompt_tokens": result.input_tokens or 0,
                "completion_tokens": result.output_tokens or 0,
            },
        }

        return _full_state(
            state,
            current_stage="generation_call",
            current_candidate_id=candidate_id,
            raw_response=raw_response,
            computed_final_answer=result.raw_text,
            computed_final_confidence=0.85,
        )
    except Exception as exc:
        logger.error(f"node_generation_call failed: {exc}", run_id=run_id)
        return _full_state(
            state,
            current_stage="finalize_failure",
            error_code="GENERATION_ERROR",
            error_message=str(exc),
        )


async def node_generation_persist(
    state: OrchestrationState,
    config: RunnableConfig,
) -> OrchestrationState:
    """Persist candidate to run_candidates table (idempotent)."""
    current = state.get("current_stage", "")
    if current.startswith("finalize"):
        return state

    worker = _get_worker(config)
    candidate_id = state.get("current_candidate_id")
    existing_ids: list[str] = list(state.get("candidate_ids", []))

    if candidate_id and candidate_id not in existing_ids:
        updated_ids = existing_ids + [candidate_id]
    else:
        updated_ids = existing_ids

    if worker is not None and candidate_id:
        try:
            from fusion_council_service.domain.candidate_repository import (
                insert_candidate,
            )
            from fusion_council_service.domain.event_emitter import (
                emit_candidate_completed,
            )

            db = worker._get_db()
            run_id = state["run_id"]
            raw = state.get("raw_response") or {}
            model_name = "unknown"
            if isinstance(raw, dict):
                model_name = raw.get("model", "unknown")

            insert_candidate(
                db,
                run_id=run_id,
                candidate_id=candidate_id,
                alias="single-model",
                provider="langgraph",
                provider_model=model_name,
                stage="generation",
                status="succeeded",
                created_at=utc_now_iso(),
            )

            emit_candidate_completed(
                db,
                run_id=run_id,
                candidate_id=candidate_id,
                alias="single-model",
                stage="generation",
            )
            logger.info(f"Candidate {candidate_id} persisted", run_id=run_id)
        except Exception as exc:
            logger.error(f"node_generation_persist failed: {exc}", run_id=run_id)
            return _full_state(
                state,
                current_stage="finalize_failure",
                error_code="PERSIST_ERROR",
                error_message=str(exc),
            )

    return _full_state(
        state,
        current_stage="generation_persist",
        candidate_ids=updated_ids,
    )


async def node_finalize_success(
    state: OrchestrationState,
    config: RunnableConfig,
) -> OrchestrationState:
    """Write final_answer, update runs table, emit completion event."""
    current = state.get("current_stage", "")
    if current.startswith("finalize"):
        return state

    worker = _get_worker(config)
    final_answer = state.get("computed_final_answer") or ""
    final_confidence = float(state.get("computed_final_confidence") or 0.0)

    if worker is not None:
        try:
            from fusion_council_service.domain.event_emitter import (
                emit_run_completed,
            )
            from fusion_council_service.domain.run_repository import (
                update_run_status,
            )

            db = worker._get_db()
            run_id = state["run_id"]

            update_run_status(
                db,
                run_id,
                "succeeded",
                final_answer=final_answer,
                finished_at=utc_now_iso(),
            )
            emit_run_completed(db, run_id, final_answer, final_confidence)
        except Exception as exc:
            logger.error(f"node_finalize_success DB write failed: {exc}", run_id=state.get("run_id"))
            return _full_state(
                state,
                current_stage="finalize_failure",
                error_code="FINALIZE_DB_ERROR",
                error_message=str(exc),
            )

    return _full_state(
        state,
        current_stage="finalize_success",
        final_answer=final_answer,
        final_confidence=final_confidence,
    )


async def node_finalize_failure(
    state: OrchestrationState,
    config: RunnableConfig,
) -> OrchestrationState:
    """Write error state to runs table, emit failure event."""
    current = state.get("current_stage", "")
    if current.startswith("finalize"):
        return state

    worker = _get_worker(config)
    error_code = state.get("error_code", "UNKNOWN")
    error_message = state.get("error_message", "Finalization failed")

    if worker is not None:
        try:
            from fusion_council_service.domain.event_emitter import (
                emit_run_failed,
            )
            from fusion_council_service.domain.run_repository import (
                update_run_status,
            )

            db = worker._get_db()
            run_id = state["run_id"]

            update_run_status(
                db,
                run_id,
                "failed",
                error_code=error_code,
                finished_at=utc_now_iso(),
            )
            emit_run_failed(db, run_id=run_id, error_code=error_code, error_message=error_message)
        except Exception as exc:
            logger.error(f"node_finalize_failure DB write failed: {exc}", run_id=state.get("run_id"))

    return _full_state(
        state,
        current_stage="finalize_failure",
        error_code=error_code,
        error_message=error_message,
    )
