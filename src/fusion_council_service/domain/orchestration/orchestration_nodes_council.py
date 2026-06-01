"""
LangGraph nodes for council mode orchestration.

Council mode: multiple peers give opinions, optionally debate, then synthesize.

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


def _compute_pairwise_agreement(candidate_results: list[dict]) -> float:
    """Compute agreement score between candidates (0-1 scale).

    Returns 1.0 if all candidates agree (identical normalized answers).
    Returns lower values as answers diverge.
    """
    if len(candidate_results) < 2:
        return 1.0

    # Simple agreement: count pairs with similar answers
    texts = [r.get("normalized_answer", r.get("raw_text", "")) for r in candidate_results]
    total_pairs = 0
    agreeing_pairs = 0

    for i in range(len(texts)):
        for j in range(i + 1, len(texts)):
            total_pairs += 1
            # Simple check: first 50 chars must match
            if texts[i][:50] == texts[j][:50]:
                agreeing_pairs += 1

    if total_pairs == 0:
        return 1.0

    return agreeing_pairs / total_pairs


async def node_prepare_council(
    state: OrchestrationState,
    config: Optional[RunnableConfig] = None,
) -> OrchestrationState:
    """
    Entry node for council mode — validates run exists and sets initial stage.

    Idempotency: if current_stage already past prepare, return unchanged.
    Error routing: if run_id is absent, sets finalize_failure.
    """
    run_id = state.get("run_id")
    if not run_id:
        return _full_state(
            state,
            current_stage="finalize_failure",
            error_code="RUN_ID_MISSING",
            error_message="run_id not found in state",
        )

    current = state.get("current_stage", "")
    if current.startswith(("first_opinion", "peer_review", "debate", "synthesis", "finalize")):
        return state

    logger.info("Preparing council mode", run_id=run_id)

    return _full_state(state, current_stage="prepare_council")


async def node_first_opinion_parallel(
    state: OrchestrationState,
    config: Optional[RunnableConfig] = None,
) -> OrchestrationState:
    """
    First opinion parallel call — gets opinions from multiple peers.

    Actual work:
    - Get peer models from catalog
    - Call each peer in parallel (max 3 concurrent)
    - Store results in state
    """
    current = state.get("current_stage", "")
    if current.startswith(("peer_review", "debate", "synthesis", "finalize")):
        return state

    worker = _get_worker(config)
    if not worker:
        logger.warning("No worker in config, skipping first opinion parallel")
        return _full_state(state, current_stage="first_opinion_parallel")

    run_id = state.get("run_id")
    logger.info("Starting first opinion parallel", run_id=run_id)

    try:
        models = worker.catalog.get_peers_for_mode("council")
    except Exception as e:
        logger.error("Failed to get peers for council mode", error=str(e))
        return _full_state(
            state,
            current_stage="finalize_failure",
            error_code="PEER_CATALOG_ERROR",
            error_message=f"Failed to get council peers: {e}",
        )

    if not models:
        return _full_state(
            state,
            current_stage="finalize_failure",
            error_code="NO_PEERS",
            error_message="No peers available for council mode",
        )

    # Call peers in parallel with semaphore (max 3 concurrent)
    semaphore = asyncio.Semaphore(3)

    async def call_peer(peer: dict) -> dict:
        async with semaphore:
            try:
                # E1 fix: build a real ProviderGenerateRequest via the canonical
                # helper so timeout_seconds is propagated from the catalog entry.
                # Previously this passed a plain dict with wrong field names
                # (e.g. "model" instead of "provider_model"), which would
                # AttributeError on real provider call — masked by tests because
                # Worker is mocked.
                request = build_provider_request(
                    peer,
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
                    "alias": peer.get("alias"),
                    "success": success,
                    "raw_text": raw_text,
                    "error": err_msg,
                    "input_tokens": in_tok,
                    "output_tokens": out_tok,
                }
            except Exception as e:
                logger.error("Peer call failed", peer=peer.get("alias"), error=str(e))
                return {
                    "alias": peer.get("alias"),
                    "success": False,
                    "error": str(e),
                }

    # Execute all peer calls in parallel
    tasks = [call_peer(peer) for peer in models]
    results = await asyncio.gather(*tasks)

    # Store results in state
    successful_results = [r for r in results if r.get("success")]
    logger.info(
        "First opinion parallel complete",
        run_id=run_id,
        total_calls=len(results),
        successful=len(successful_results),
    )

    return _full_state(
        state,
        current_stage="first_opinion_parallel",
        candidate_results=results,
    )


async def node_first_opinion_persist(
    state: OrchestrationState,
    config: Optional[RunnableConfig] = None,
) -> OrchestrationState:
    """
    Persist first opinion candidates to database.

    Idempotency: if candidate_id already in list, skip.
    """
    current = state.get("current_stage", "")
    if current.startswith(("peer_review", "debate", "synthesis", "finalize")):
        return state

    worker = _get_worker(config)
    if not worker:
        logger.warning("No worker in config, skipping first opinion persist")
        return _full_state(state, current_stage="first_opinion_persist")

    candidate_id = state.get("current_candidate_id")
    if not candidate_id:
        return _full_state(state, current_stage="first_opinion_persist")

    existing_ids = list(state.get("candidate_ids", []))

    # Idempotency
    if candidate_id in existing_ids:
        return _full_state(state, current_stage="first_opinion_persist")

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
            insert_candidate(
                worker.db,
                run_id=run_id,
                candidate_id=candidate_id,
                alias=result_data.get("alias", "unknown"),
                provider=result_data.get("provider", "unknown"),
                provider_model=result_data.get("model", "unknown"),
                stage="first_opinion",
                status="succeeded" if result_data.get("success") else "failed",
                created_at=datetime.now(timezone.utc).isoformat(),
            )
            logger.info("Persisted first opinion candidate", run_id=run_id, candidate_id=candidate_id)

    except Exception as e:
        logger.error("Failed to persist first opinion candidate", error=str(e))

    updated_ids = existing_ids + [candidate_id]
    return _full_state(
        state,
        current_stage="first_opinion_persist",
        candidate_ids=updated_ids,
    )


async def node_peer_review_call(
    state: OrchestrationState,
    config: Optional[RunnableConfig] = None,
) -> OrchestrationState:
    """
    Peer review call — asks another peer to review first opinions.

    Skip if degradation includes "skip_peer" or agreement is high (>0.55).
    """
    current = state.get("current_stage", "")
    if current.startswith(("debate", "synthesis", "finalize")):
        return state

    # Check if peer review should be skipped
    degradation = state.get("degradation", "")
    if "skip_peer" in degradation:
        logger.info("Skipping peer review per degradation flag")
        return _full_state(state, current_stage="peer_review_skip")

    # Compute pairwise agreement
    candidate_results = state.get("candidate_results", [])
    agreement = _compute_pairwise_agreement(candidate_results)

    # If agreement is high (>0.55), skip peer review
    if agreement >= 0.55:
        logger.info("High agreement, skipping peer review", agreement=agreement)
        return _full_state(state, current_stage="peer_review_skip")

    worker = _get_worker(config)
    if not worker:
        logger.warning("No worker in config, skipping peer review")
        return _full_state(state, current_stage="peer_review_skip")

    run_id = state.get("run_id")
    logger.info("Starting peer review call", run_id=run_id, agreement=agreement)

    try:
        models = worker.catalog.get_peers_for_mode("council")
        if not models:
            raise ValueError("No peers available for peer review")

        # Get a different peer for review
        review_peer = models[-1]  # Last peer is typically different
        first_opinions = [r.get("raw_text", "") for r in candidate_results if r.get("success")]

        # E1 fix: real ProviderGenerateRequest via canonical helper
        request = build_provider_request(
            review_peer,
            system_prompt=None,
            user_prompt="Review these first opinions:\n\n" + "\n\n".join(first_opinions),
            max_output_tokens=state.get("max_tokens", 4096),
            temperature=state.get("temperature", 0.1),
        )
        success, raw_text, err_code, err_msg, lat_ms, in_tok, out_tok = await worker._call_provider_async(
            request, worker.db, run_id
        )

        return _full_state(
            state,
            current_stage="peer_review_call",
            peer_review_text=raw_text,
        )

    except Exception as e:
        logger.error("Peer review call failed", error=str(e))
        return _full_state(state, current_stage="peer_review_skip")


async def node_peer_review_persist(
    state: OrchestrationState,
    config: Optional[RunnableConfig] = None,
) -> OrchestrationState:
    """Persist peer review candidate."""
    current = state.get("current_stage", "")
    if current.startswith(("debate", "synthesis", "finalize")):
        return state

    worker = _get_worker(config)
    if not worker:
        return _full_state(state, current_stage="peer_review_persist")

    # Just advance stage - no new candidate ID typically
    return _full_state(state, current_stage="peer_review_persist")


async def node_debate_call(
    state: OrchestrationState,
    config: Optional[RunnableConfig] = None,
) -> OrchestrationState:
    """
    Debate call — triggers debate if agreement is low (<0.55).

    Skip if agreement >= 0.55.
    """
    current = state.get("current_stage", "")
    if current.startswith(("synthesis", "finalize")):
        return state

    # Compute pairwise agreement
    candidate_results = state.get("candidate_results", [])
    agreement = _compute_pairwise_agreement(candidate_results)

    # If agreement is high, skip debate
    if agreement >= 0.55:
        logger.info("High agreement, skipping debate", agreement=agreement)
        return _full_state(state, current_stage="debate_skip")

    worker = _get_worker(config)
    if not worker:
        logger.warning("No worker in config, skipping debate")
        return _full_state(state, current_stage="debate_skip")

    run_id = state.get("run_id")
    logger.info("Starting debate call", run_id=run_id, agreement=agreement)

    try:
        models = worker.catalog.get_peers_for_mode("council")
        if not models:
            raise ValueError("No peers available for debate")

        # Get two different peers for debate
        peer1, peer2 = models[0], models[1]
        first_opinions = [r.get("raw_text", "") for r in candidate_results[:2]]

        # E1 fix: real ProviderGenerateRequest via canonical helper
        request = build_provider_request(
            peer1,
            system_prompt=None,
            user_prompt=f"Debate this topic with another perspective:\n\nQuestion: {state.get('prompt', '')}\n\nFirst Opinion A: {first_opinions[0] if len(first_opinions) > 0 else 'N/A'}\n\nFirst Opinion B: {first_opinions[1] if len(first_opinions) > 1 else 'N/A'}",
            max_output_tokens=state.get("max_tokens", 4096),
            temperature=state.get("temperature", 0.2),
        )
        success, raw_text, err_code, err_msg, lat_ms, in_tok, out_tok = await worker._call_provider_async(
            request, worker.db, run_id
        )

        return _full_state(
            state,
            current_stage="debate_call",
            debate_text=raw_text,
        )

    except Exception as e:
        logger.error("Debate call failed", error=str(e))
        return _full_state(state, current_stage="debate_skip")


async def node_debate_persist(
    state: OrchestrationState,
    config: Optional[RunnableConfig] = None,
) -> OrchestrationState:
    """Persist debate result."""
    current = state.get("current_stage", "")
    if current.startswith(("synthesis", "finalize")):
        return state

    return _full_state(state, current_stage="debate_persist")


async def node_synthesis_call(
    state: OrchestrationState,
    config: Optional[RunnableConfig] = None,
) -> OrchestrationState:
    """
    Synthesis call — builds combined prompt and calls synthesis model.
    """
    current = state.get("current_stage", "")
    if current.startswith(("verification", "finalize")):
        return state

    worker = _get_worker(config)
    if not worker:
        logger.warning("No worker in config, skipping synthesis call")
        return _full_state(state, current_stage="synthesis_call")

    run_id = state.get("run_id")
    logger.info("Starting council synthesis call", run_id=run_id)

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
        # Include peer review and debate if available
        peer_review = state.get("peer_review_text", "")
        debate = state.get("debate_text", "")

        prompt_parts = [
            f"Original question: {state.get('prompt', '')}",
            "\n\nFirst Opinions:",
        ]
        for i, ans in enumerate(candidate_answers, 1):
            prompt_parts.append(f"Opinion {i}: {ans}")

        if peer_review:
            prompt_parts.append(f"\n\nPeer Review: {peer_review}")
        if debate:
            prompt_parts.append(f"\n\nDebate: {debate}")

        synthesis_prompt = "\n".join(prompt_parts)
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
        models = worker.catalog.get_peers_for_mode("council")
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
            computed_final_confidence=0.8,
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
    """Persist synthesis candidate."""
    current = state.get("current_stage", "")
    if current.startswith(("verification", "finalize")):
        return state

    worker = _get_worker(config)
    if not worker:
        return _full_state(state, current_stage="synthesis_persist")

    candidate_id = state.get("current_candidate_id")
    if not candidate_id:
        return _full_state(state, current_stage="synthesis_persist")

    existing_ids = list(state.get("candidate_ids", []))

    if candidate_id in existing_ids:
        return _full_state(state, current_stage="synthesis_persist")

    try:
        run_id = state.get("run_id", "")

        insert_candidate(
            worker.db,
            run_id=run_id,
            candidate_id=candidate_id,
            alias="council_synthesis",
            provider="",
            provider_model="",
            stage="synthesis",
            status="succeeded",
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        logger.info("Persisted council synthesis candidate", run_id=run_id, candidate_id=candidate_id)

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
    """Verification call node."""
    current = state.get("current_stage", "")
    if current.startswith("finalize"):
        return state

    worker = _get_worker(config)
    if not worker:
        logger.warning("No worker in config, skipping verification call")
        return _full_state(state, current_stage="verification_call")

    run_id = state.get("run_id")
    logger.info("Starting council verification call", run_id=run_id)

    try:
        models = worker.catalog.get_peers_for_mode("council")
        if not models:
            raise ValueError("No models available for verification")
        verif_peer = models[0]
        computed_answer = state.get("computed_final_answer", "")

        # E1 fix: real ProviderGenerateRequest via canonical helper
        request = build_provider_request(
            verif_peer,
            system_prompt=None,
            user_prompt=f"Verify this answer:\n\n{computed_answer}",
            max_output_tokens=state.get("max_tokens", 4096),
            temperature=state.get("temperature", 0.1),
        )
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

        return _full_state(
            state,
            current_stage="verification_call",
            computed_final_confidence=0.85,
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
    """Persist verification candidate."""
    current = state.get("current_stage", "")
    if current.startswith("finalize"):
        return state

    worker = _get_worker(config)
    if not worker:
        return _full_state(state, current_stage="verification_persist")

    candidate_id = state.get("current_candidate_id")
    if not candidate_id:
        return _full_state(state, current_stage="verification_persist")

    existing_ids = list(state.get("candidate_ids", []))

    if candidate_id in existing_ids:
        return _full_state(state, current_stage="verification_persist")

    try:
        run_id = state.get("run_id", "")

        insert_candidate(
            worker.db,
            run_id=run_id,
            candidate_id=candidate_id,
            alias="council_verification",
            provider="",
            provider_model="",
            stage="verification",
            status="succeeded",
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        logger.info("Persisted council verification candidate", run_id=run_id, candidate_id=candidate_id)

    except Exception as e:
        logger.error("Failed to persist verification candidate", error=str(e))

    updated_ids = existing_ids + [candidate_id]
    return _full_state(
        state,
        current_stage="verification_persist",
        candidate_ids=updated_ids,
    )


async def node_finalize_council_success(
    state: OrchestrationState,
    config: Optional[RunnableConfig] = None,
) -> OrchestrationState:
    """
    Finalize node (happy path) — copies computed_final_answer.
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


async def node_finalize_council_failure(
    state: OrchestrationState,
    config: Optional[RunnableConfig] = None,
) -> OrchestrationState:
    """
    Finalize node (error path) — sets error state.
    """
    current = state.get("current_stage", "")
    if current.startswith("finalize"):
        return state

    error_code = state.get("error_code") or "UNKNOWN"
    error_message = state.get("error_message") or "Council failed without error message"

    return _full_state(
        state,
        current_stage="finalize_failure",
        error_code=error_code,
        error_message=error_message,
    )