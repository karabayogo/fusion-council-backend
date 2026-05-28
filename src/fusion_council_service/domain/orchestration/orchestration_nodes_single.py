"""
LangGraph nodes for single-mode orchestration.

Design: coarse-grained wrapper nodes (Option B).
Each node is a pure function: OrchestrationState -> OrchestrationState.
No direct DB or API calls — side effects are performed by the caller (worker_loop.py)
after the node returns, preserving idempotency and replay safety.

IMPORTANT — LangGraph channel semantics for total=False TypedDict:
  Every node MUST return ALL fields of OrchestrationState on every invocation.
  Missing fields default to None in the channel, which then propagates to all
  subsequent nodes in the same step. This is a fundamental LangGraph behavior,
  NOT a bug. Nodes that return partial dicts will silently zero out missing fields.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fusion_council_service.domain.orchestration.orchestration_state import (
        OrchestrationState,
    )


def node_prepare_run(state: OrchestrationState) -> OrchestrationState:
    """
    Entry node — validates run exists and sets initial stage.

    Idempotency: if already past prepare (in generation or finalize), return unchanged.
    Error routing: if run_id is absent/empty, sets finalize_failure stage.
    """
    run_id = state.get("run_id")
    if not run_id:
        return {
            **state,
            "run_id": state.get("run_id"),
            "mode": state.get("mode"),
            "engine": state.get("engine"),
            "engine_version": state.get("engine_version"),
            "thread_id": state.get("thread_id"),
            "checkpoint_namespace": state.get("checkpoint_namespace"),
            "resume_count": state.get("resume_count"),
            "current_stage": "finalize_failure",
            "candidate_ids": state.get("candidate_ids", []),
            "final_answer": state.get("final_answer"),
            "final_confidence": state.get("final_confidence"),
            "error_code": "RUN_ID_MISSING",
            "error_message": "run_id not found in state — cannot proceed",
            "updated_at": state.get("updated_at"),
            "raw_response": state.get("raw_response"),
            "candidate_summaries": state.get("candidate_summaries"),
            "computed_final_answer": state.get("computed_final_answer"),
            "computed_final_confidence": state.get("computed_final_confidence"),
        }

    # Idempotency guard — skip if already advanced past prepare_run
    current = state.get("current_stage", "")
    if current.startswith(("generation", "finalize")):
        return state

    return {
        **state,
        "current_stage": "prepare_run",
    }


def node_generation_call(state: OrchestrationState) -> OrchestrationState:
    """
    Generation call node — advances to generation_call stage.

    No side effects here. The caller (worker_loop.py) performs the actual
    model API call and populates current_candidate_id in state after this node returns.
    Keeping the node pure ensures replay safety.

    IMPORTANT: Must forward ALL fields (see module docstring).
    """
    current = state.get("current_stage", "")

    # Idempotency — if already past generation_call, skip
    if current.startswith(("generation_persist", "finalize")):
        return state

    return {
        **state,
        "run_id": state.get("run_id"),
        "mode": state.get("mode"),
        "engine": state.get("engine"),
        "engine_version": state.get("engine_version"),
        "thread_id": state.get("thread_id"),
        "checkpoint_namespace": state.get("checkpoint_namespace"),
        "resume_count": state.get("resume_count"),
        "current_stage": "generation_call",
        "candidate_ids": state.get("candidate_ids", []),
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


def node_generation_persist(state: OrchestrationState) -> OrchestrationState:
    """
    Persist node — appends current_candidate_id to candidate_ids and advances stage.

    Side effect (performed by caller after node returns):
      INSERT INTO run_candidates (...) VALUES (...) ON CONFLICT DO NOTHING

    Idempotency: if candidate_id already in candidate_ids list, skip adding it again.
    Idempotency: if already past generation_persist, skip.
    """
    candidate_id = state.get("current_candidate_id")
    existing_ids = state.get("candidate_ids", [])

    # Idempotency — don't duplicate if already persisted on prior run
    if candidate_id and candidate_id not in existing_ids:
        updated_ids = existing_ids + [candidate_id]
    else:
        updated_ids = list(existing_ids)

    # Idempotency — if already past generation_persist, skip
    current = state.get("current_stage", "")
    if current.startswith("finalize"):
        return state

    return {
        **state,
        "run_id": state.get("run_id"),
        "mode": state.get("mode"),
        "engine": state.get("engine"),
        "engine_version": state.get("engine_version"),
        "thread_id": state.get("thread_id"),
        "checkpoint_namespace": state.get("checkpoint_namespace"),
        "resume_count": state.get("resume_count"),
        "current_stage": "generation_persist",
        "candidate_ids": updated_ids,
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


def node_finalize_success(state: OrchestrationState) -> OrchestrationState:
    """
    Finalize node (happy path) — copies computed_final_answer into final_answer.

    Idempotency guard: if already in a finalize stage, return unchanged.
    The current_stage check is the authoritative replay guard — final_answer may
    already be set on a prior failed invoke that still reached this node.
    """
    # Idempotency — if already in a finalize state, skip
    current = state.get("current_stage", "")
    if current.startswith("finalize"):
        return state

    return {
        **state,
        "run_id": state.get("run_id"),
        "mode": state.get("mode"),
        "engine": state.get("engine"),
        "engine_version": state.get("engine_version"),
        "thread_id": state.get("thread_id"),
        "checkpoint_namespace": state.get("checkpoint_namespace"),
        "resume_count": state.get("resume_count"),
        "current_stage": "finalize_success",
        "candidate_ids": state.get("candidate_ids", []),
        "final_answer": state.get("computed_final_answer"),
        "final_confidence": state.get("computed_final_confidence", 0.0),
        "error_code": state.get("error_code"),
        "error_message": state.get("error_message"),
        "updated_at": state.get("updated_at"),
        "raw_response": state.get("raw_response"),
        "candidate_summaries": state.get("candidate_summaries"),
        "computed_final_answer": state.get("computed_final_answer"),
        "computed_final_confidence": state.get("computed_final_confidence"),
    }


def node_finalize_failure(state: OrchestrationState) -> OrchestrationState:
    """
    Finalize node (error path) — copies error_code/error_message into state.

    Idempotency: if already in a finalize stage, return unchanged.
    """
    # Idempotency — already finalized
    current = state.get("current_stage", "")
    if current.startswith("finalize"):
        return state

    return {
        **state,
        "run_id": state.get("run_id"),
        "mode": state.get("mode"),
        "engine": state.get("engine"),
        "engine_version": state.get("engine_version"),
        "thread_id": state.get("thread_id"),
        "checkpoint_namespace": state.get("checkpoint_namespace"),
        "resume_count": state.get("resume_count"),
        "current_stage": "finalize_failure",
        "candidate_ids": state.get("candidate_ids", []),
        "final_answer": state.get("final_answer"),
        "final_confidence": state.get("final_confidence"),
        "error_code": state.get("error_code", "UNKNOWN"),
        "error_message": state.get("error_message", "Finalization failed without error message"),
        "updated_at": state.get("updated_at"),
        "raw_response": state.get("raw_response"),
        "candidate_summaries": state.get("candidate_summaries"),
        "computed_final_answer": state.get("computed_final_answer"),
        "computed_final_confidence": state.get("computed_final_confidence"),
    }
