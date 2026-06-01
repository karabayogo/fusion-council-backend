"""
LangGraph nodes for fusion mode orchestration.

Fusion mode: multiple models run in parallel, then synthesize their answers.

Design pattern: Option A (async, do real work)
- Nodes are async and perform actual work (model calls, DB writes)
- Worker dependencies passed via RunnableConfig.configurable
- Each node has idempotency guards
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Optional

from fusion_council_service.domain.candidate_repository import insert_candidate
from fusion_council_service.domain.orchestration.orchestration_state import OrchestrationState
from fusion_council_service.domain.scoring import build_fusion_prompt
from fusion_council_service.domain.worker_loop import build_provider_request
from fusion_council_service.logging_utils import get_logger

if TYPE_CHECKING:
    from langchain_core.runnables import RunnableConfig

logger = get_logger(__name__)


def _full_state(state: OrchestrationState, **overrides: Any) -> OrchestrationState:
    """Return state with overrides applied."""
    result = dict(state)
    result.update(overrides)
    return result  # type: ignore[return-value]


def _get_worker(config: Optional[RunnableConfig]) -> Optional[Any]:
    """Extract worker from config."""
    if config and "configurable" in config:
        return config["configurable"].get("worker")
    return None


async def node_prepare_fusion(
    state: OrchestrationState,
    config: Optional[RunnableConfig] = None,
) -> OrchestrationState:
    """
    Entry node for fusion mode — validates run exists and sets initial stage.

    Idempotency: if current_stage already in generation or later, return unchanged.
    Error routing: if run_id is absent/empty, sets finalize_failure stage.
    """
    run_id = state.get("run_id")
    if not run_id:
        return _full_state(
            state,
            current_stage="finalize_failure",
            error_code="RUN_ID_MISSING",
            error_message="run_id not found in state — cannot proceed",
        )

    current = state.get("current_stage", "")
    if current.startswith(("generation", "synthesis", "verification", "finalize")):
        return state

    # Log fusion mode start
    logger.info("Preparing fusion mode", run_id=run_id)

    return _full_state(state, current_stage="prepare_fusion")


async def node_generation_parallel(
    state: OrchestrationState,
    config: Optional[RunnableConfig] = None,
) -> OrchestrationState:
    """
    Generation parallel call node — calls multiple models in parallel.

    Actual work:
    - Get models for fusion mode from catalog
    - Call each model in parallel (max 3 concurrent via semaphore)
    - Store results in state for downstream nodes
    """
    current = state.get("current_stage", "")
    if current.startswith(("synthesis", "verification", "finalize")):
        return state

    worker = _get_worker(config)
    if not worker:
        logger.warning("No worker in config, skipping parallel generation")
        return _full_state(state, current_stage="generation_parallel")

    run_id = state.get("run_id")
    logger.info("Starting parallel generation", run_id=run_id)

    # Get models for fusion mode
    try:
        models = worker.catalog.get_models_for_mode("fusion")
    except Exception as e:
        logger.error("Failed to get models for fusion mode", error=str(e))
        return _full_state(
            state,
            current_stage="finalize_failure",
            error_code="MODEL_CATALOG_ERROR",
            error_message=f"Failed to get fusion models: {e}",
        )

    if not models:
        return _full_state(
            state,
            current_stage="finalize_failure",
            error_code="NO_MODELS",
            error_message="No models available for fusion mode",
        )

    # Call models in parallel with semaphore (max 3 concurrent)
    semaphore = asyncio.Semaphore(3)

    async def call_model(model: dict) -> dict:
        async with semaphore:
            try:
                # E1 fix: real ProviderGenerateRequest via canonical helper
                request = build_provider_request(
                    model,
                    system_prompt=state.get("system_prompt"),
                    user_prompt=state.get("prompt", ""),
                    max_output_tokens=state.get("max_tokens", 4096),
                    temperature=state.get("temperature", 0.2),
                )
                # Use worker's async provider method
                success, raw_text, err_code, err_msg, lat_ms, in_tok, out_tok = await worker._call_provider_async(
                    request, worker.db, run_id
                )
                return {
                    "alias": model.get("alias"),
                    "success": success,
                    "raw_text": raw_text,
                    "error": err_msg,
                    "input_tokens": in_tok,
                    "output_tokens": out_tok,
                }
            except Exception as e:
                logger.error("Model call failed", model=model.get("alias"), error=str(e))
                return {
                    "alias": model.get("alias"),
                    "success": False,
                    "error": str(e),
                }

    # Execute all model calls in parallel
    tasks = [call_model(model) for model in models]
    results = await asyncio.gather(*tasks)

    # Store results in state
    candidate_results = [r for r in results if r.get("success")]
    logger.info(
        "Parallel generation complete",
        run_id=run_id,
        total_calls=len(results),
        successful=len(candidate_results),
    )

    return _full_state(
        state,
        current_stage="generation_parallel",
        candidate_results=candidate_results,
    )


async def node_generation_persist(
    state: OrchestrationState,
    config: Optional[RunnableConfig] = None,
) -> OrchestrationState:
    """
    Persist node — persists candidates to database.

    Idempotency: if candidate_id already in candidate_ids, skip.
    """
    current = state.get("current_stage", "")
    if current.startswith(("synthesis", "verification", "finalize")):
        return state

    worker = _get_worker(config)
    if not worker:
        logger.warning("No worker in config, skipping generation persist")
        return _full_state(state, current_stage="generation_persist")

    candidate_id = state.get("current_candidate_id")
    if not candidate_id:
        # No candidate to persist, just advance stage
        return _full_state(state, current_stage="generation_persist")

    existing_ids = list(state.get("candidate_ids", []))

    # Idempotency: don't duplicate
    if candidate_id in existing_ids:
        return _full_state(state, current_stage="generation_persist")

    # Persist candidate
    try:
        run_id = state.get("run_id", "")
        candidate_results = state.get("candidate_results", [])

        # Find matching result
        result_data = None
        for r in candidate_results:
            if r.get("alias") == candidate_id:
                result_data = r
                break

        if result_data:
            # Extract candidate data from results
            candidate_alias = result_data.get("alias", "unknown")
            candidate_provider = result_data.get("provider", "unknown")
            candidate_model = result_data.get("model", "unknown")
            candidate_status = "succeeded" if result_data.get("success") else "failed"
            candidate_raw_text = result_data.get("raw_text", "")
            candidate_input_tokens = result_data.get("input_tokens")
            candidate_output_tokens = result_data.get("output_tokens")

            insert_candidate(
                worker.db,
                run_id=run_id,
                candidate_id=candidate_id,
                alias=candidate_alias,
                provider=candidate_provider,
                provider_model=candidate_model,
                stage="generation",
                status=candidate_status,
                created_at=datetime.now(timezone.utc).isoformat(),
                execution_order=None,
            )
            logger.info("Persisted generation candidate", run_id=run_id, candidate_id=candidate_id)

    except Exception as e:
        logger.error("Failed to persist candidate", error=str(e), candidate_id=candidate_id)

    updated_ids = existing_ids + [candidate_id]
    return _full_state(
        state,
        current_stage="generation_persist",
        candidate_ids=updated_ids,
    )


async def node_synthesis_call(
    state: OrchestrationState,
    config: Optional[RunnableConfig] = None,
) -> OrchestrationState:
    """
    Synthesis call node — builds synthesis prompt and calls model.

    Actual work:
    - Build synthesis prompt from candidate answers
    - Call synthesis model
    - Store result in state
    """
    current = state.get("current_stage", "")
    if current.startswith(("verification", "finalize")):
        return state

    worker = _get_worker(config)
    if not worker:
        logger.warning("No worker in config, skipping synthesis call")
        return _full_state(state, current_stage="synthesis_call")

    run_id = state.get("run_id")
    logger.info("Starting synthesis call", run_id=run_id)

    # Get candidate answers
    candidate_results = state.get("candidate_results", [])
    candidate_answers = [r.get("raw_text", "") for r in candidate_results if r.get("success")]

    if not candidate_answers:
        return _full_state(
            state,
            current_stage="finalize_failure",
            error_code="NO_CANDIDATE_ANSWERS",
            error_message="No successful candidate answers for synthesis",
        )

    # Build synthesis prompt
    try:
        prompt = state.get("prompt", "")
        candidates = candidate_results
        memory_context = state.get("memory_context", "")

        synthesis_prompt = build_fusion_prompt(prompt, candidates, memory_context)
    except Exception as e:
        logger.error("Failed to build synthesis prompt", error=str(e))
        return _full_state(
            state,
            current_stage="finalize_failure",
            error_code="PROMPT_BUILD_ERROR",
            error_message=f"Failed to build synthesis prompt: {e}",
        )

    # Call synthesis model
    try:
        # Get first available model for synthesis
        models = worker.catalog.get_models_for_mode("fusion")
        if not models:
            raise ValueError("No models available for synthesis")

        synthesis_model = models[0]
        # E1 fix: real ProviderGenerateRequest via canonical helper
        request = build_provider_request(
            synthesis_model,
            system_prompt=None,
            user_prompt=synthesis_prompt,
            max_output_tokens=state.get("max_tokens", 4096),
            temperature=state.get("temperature", 0.2),
        )
        # Use worker's async provider method
        success, raw_text, err_code, err_msg, lat_ms, in_tok, out_tok = await worker._call_provider_async(
            request, worker.db, run_id
        )

        if not success:
            return _full_state(
                state,
                current_stage="finalize_failure",
                error_code="SYNTHESIS_FAILED",
                error_message=raw_text or "Synthesis model failed",
            )

        return _full_state(
            state,
            current_stage="synthesis_call",
            computed_final_answer=raw_text,
            computed_final_confidence=0.8,  # Default confidence for synthesis
        )

    except Exception as e:
        logger.error("Synthesis call failed", error=str(e))
        return _full_state(
            state,
            current_stage="finalize_failure",
            error_code="SYNTHESIS_ERROR",
            error_message=f"Synthesis call failed: {e}",
        )


async def node_synthesis_persist(
    state: OrchestrationState,
    config: Optional[RunnableConfig] = None,
) -> OrchestrationState:
    """
    Synthesis persist node — persists synthesis candidate to database.
    """
    current = state.get("current_stage", "")
    if current.startswith(("verification", "finalize")):
        return state

    worker = _get_worker(config)
    if not worker:
        logger.warning("No worker in config, skipping synthesis persist")
        return _full_state(state, current_stage="synthesis_persist")

    candidate_id = state.get("current_candidate_id")
    if not candidate_id:
        return _full_state(state, current_stage="synthesis_persist")

    existing_ids = list(state.get("candidate_ids", []))

    # Idempotency
    if candidate_id in existing_ids:
        return _full_state(state, current_stage="synthesis_persist")

    try:
        run_id = state.get("run_id", "")
        computed_answer = state.get("computed_final_answer", "")

        insert_candidate(
            worker.db,
            run_id=run_id,
            candidate_id=candidate_id,
            alias="synthesis",
            provider="",
            provider_model="",
            stage="synthesis",
            status="succeeded",
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        logger.info("Persisted synthesis candidate", run_id=run_id, candidate_id=candidate_id)

    except Exception as e:
        logger.error("Failed to persist synthesis candidate", error=str(e))

    updated_ids = existing_ids + [candidate_id]
    return _full_state(
        state,
        current_stage="synthesis_persist",
        candidate_ids=updated_ids,
    )


async def node_verification_call(
    state: OrchestrationState,
    config: Optional[RunnableConfig] = None,
) -> OrchestrationState:
    """
    Verification call node — calls verification model.
    """
    current = state.get("current_stage", "")
    if current.startswith("finalize"):
        return state

    worker = _get_worker(config)
    if not worker:
        logger.warning("No worker in config, skipping verification call")
        return _full_state(state, current_stage="verification_call")

    run_id = state.get("run_id")
    logger.info("Starting verification call", run_id=run_id)

    try:
        models = worker.catalog.get_models_for_mode("fusion")
        if not models:
            raise ValueError("No models available for verification")

        verification_model = models[0]
        computed_answer = state.get("computed_final_answer", "")

        # E1 fix: real ProviderGenerateRequest via canonical helper
        request = build_provider_request(
            verification_model,
            system_prompt=None,
            user_prompt=f"Verify this answer: {computed_answer}",
            max_output_tokens=state.get("max_tokens", 4096),
            temperature=state.get("temperature", 0.1),
        )
        # Use worker's async provider method
        success, raw_text, err_code, err_msg, lat_ms, in_tok, out_tok = await worker._call_provider_async(
            request, worker.db, run_id
        )

        if not success:
            return _full_state(
                state,
                current_stage="finalize_failure",
                error_code="VERIFICATION_FAILED",
                error_message=raw_text or "Verification model failed",
            )

        # For now, keep the computed answer (verification could adjust confidence)
        return _full_state(
            state,
            current_stage="verification_call",
            computed_final_confidence=0.85,  # Slightly higher after verification
        )

    except Exception as e:
        logger.error("Verification call failed", error=str(e))
        return _full_state(
            state,
            current_stage="finalize_failure",
            error_code="VERIFICATION_ERROR",
            error_message=f"Verification call failed: {e}",
        )


async def node_verification_persist(
    state: OrchestrationState,
    config: Optional[RunnableConfig] = None,
) -> OrchestrationState:
    """
    Verification persist node — persists verification candidate to database.
    """
    current = state.get("current_stage", "")
    if current.startswith("finalize"):
        return state

    worker = _get_worker(config)
    if not worker:
        logger.warning("No worker in config, skipping verification persist")
        return _full_state(state, current_stage="verification_persist")

    candidate_id = state.get("current_candidate_id")
    if not candidate_id:
        return _full_state(state, current_stage="verification_persist")

    existing_ids = list(state.get("candidate_ids", []))

    # Idempotency
    if candidate_id in existing_ids:
        return _full_state(state, current_stage="verification_persist")

    try:
        run_id = state.get("run_id", "")
        computed_answer = state.get("computed_final_answer", "")

        insert_candidate(
            worker.db,
            run_id=run_id,
            candidate_id=candidate_id,
            alias="verification",
            provider="",
            provider_model="",
            stage="verification",
            status="succeeded",
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        logger.info("Persisted verification candidate", run_id=run_id, candidate_id=candidate_id)

    except Exception as e:
        logger.error("Failed to persist verification candidate", error=str(e))

    updated_ids = existing_ids + [candidate_id]
    return _full_state(
        state,
        current_stage="verification_persist",
        candidate_ids=updated_ids,
    )


async def node_finalize_fusion_success(
    state: OrchestrationState,
    config: Optional[RunnableConfig] = None,
) -> OrchestrationState:
    """
    Finalize node (happy path) — copies computed_final_answer into final_answer.

    Idempotency guard: if already in a finalize stage, return unchanged.
    """
    current = state.get("current_stage", "")
    if current.startswith("finalize"):
        return state

    computed_final_answer = state.get("computed_final_answer")
    if computed_final_answer is None:
        return _full_state(
            state,
            current_stage="finalize_failure",
            error_code="NO_FINAL_ANSWER",
            error_message="No computed final answer to finalize",
        )

    computed_final_confidence = state.get("computed_final_confidence")
    if computed_final_confidence is None:
        computed_final_confidence = 0.0

    return _full_state(
        state,
        current_stage="finalize_success",
        final_answer=computed_final_answer,
        final_confidence=computed_final_confidence,
    )


async def node_finalize_fusion_failure(
    state: OrchestrationState,
    config: Optional[RunnableConfig] = None,
) -> OrchestrationState:
    """
    Finalize node (error path) — copies error_code/error_message into state.

    Idempotency: if already in a finalize stage, return unchanged.
    """
    current = state.get("current_stage", "")
    if current.startswith("finalize"):
        return state

    error_code = state.get("error_code") or "UNKNOWN"
    error_message = state.get("error_message") or "Fusion failed without error message"

    return _full_state(
        state,
        current_stage="finalize_failure",
        error_code=error_code,
        error_message=error_message,
    )
