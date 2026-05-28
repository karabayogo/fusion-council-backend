"""
LangGraph nodes for council-mode orchestration.

Stage sequence:
  START -> node_prepare_council
          -> node_first_opinion_parallel -> node_first_opinion_persist
          -> node_peer_review_call -> node_peer_review_persist  (optional in graph; conditional in worker_loop)
          -> node_debate_call -> node_debate_persist            (optional in graph; conditional in worker_loop)
          -> node_synthesis_call -> node_synthesis_persist
          -> node_verification_call -> node_verification_persist
          -> node_finalize_council_success
          (or node_finalize_council_failure on error path)

Design: coarse-grained wrapper nodes (same pattern as fusion nodes).
Each node is a pure function: OrchestrationState -> OrchestrationState.
Side effects are performed by the caller (worker_loop.py) after node returns,
preserving idempotency and replay safety.

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


def _full_state(state: OrchestrationState, updates: dict) -> OrchestrationState:
    """Return a full OrchestrationState dict merged with updates.

    This helper avoids the verbose pattern of spelling out every field.
    All fields from the input state are forwarded; only the keys in updates
    are overwritten.
    """
    result: OrchestrationState = {**state}  # type: ignore[assignment]
    result.update(updates)  # type: ignore[assignment]
    return result


def node_prepare_council(state: OrchestrationState) -> OrchestrationState:
    """
    Entry node for council mode — validates run exists and sets initial stage.

    Idempotency: if current_stage already in first_opinion or later, return unchanged.
    Error routing: if run_id is absent/empty, sets finalize_failure stage.
    """
    run_id = state.get("run_id")
    if not run_id:
        return _full_state(state, {
            "current_stage": "finalize_failure",
            "error_code": "RUN_ID_MISSING",
            "error_message": "run_id not found in state — cannot proceed",
        })

    current = state.get("current_stage", "")
    if current.startswith(("first_opinion", "peer_review", "debate", "synthesis", "verification", "finalize")):
        return state

    return _full_state(state, {"current_stage": "prepare_council"})


def node_first_opinion_parallel(state: OrchestrationState) -> OrchestrationState:
    """
    First opinion parallel call node — advances to first_opinion_parallel stage.

    No side effects here. The caller (worker_loop.py) performs the actual
    parallel model API calls and populates current_candidate_id(s) in state
    after this node returns. Keeping the node pure ensures replay safety.

    Idempotency: if already past first_opinion_parallel (at peer_review or beyond),
    skip and return state unchanged.
    """
    current = state.get("current_stage", "")
    if current.startswith(("peer_review", "debate", "synthesis", "verification", "finalize")):
        return state

    return _full_state(state, {"current_stage": "first_opinion_parallel"})


def node_first_opinion_persist(state: OrchestrationState) -> OrchestrationState:
    """
    Persist node — appends current_candidate_id to candidate_ids and advances stage.

    Side effect (performed by caller after node returns):
      INSERT INTO run_candidates (...) VALUES (...) ON CONFLICT DO NOTHING

    Idempotency: if candidate_id already in candidate_ids list, skip adding it again.
    Idempotency: if already past first_opinion_persist, skip.
    """
    candidate_id = state.get("current_candidate_id")
    existing_ids = state.get("candidate_ids", [])

    if candidate_id and candidate_id not in existing_ids:
        updated_ids = list(existing_ids) + [candidate_id]
    else:
        updated_ids = list(existing_ids)

    current = state.get("current_stage", "")
    if current.startswith(("peer_review", "debate", "synthesis", "verification", "finalize")):
        return state

    return _full_state(state, {
        "current_stage": "first_opinion_persist",
        "candidate_ids": updated_ids,
    })


def node_peer_review_call(state: OrchestrationState) -> OrchestrationState:
    """
    Peer review call node — advances to peer_review_call stage.

    The caller (worker_loop.py) performs the peer review model calls and sets
    raw_response with the review output after this node returns.

    Idempotency: if already at debate or beyond, skip.
    """
    current = state.get("current_stage", "")
    if current.startswith(("debate", "synthesis", "verification", "finalize")):
        return state

    return _full_state(state, {"current_stage": "peer_review_call"})


def node_peer_review_persist(state: OrchestrationState) -> OrchestrationState:
    """
    Peer review persist node — records peer review candidate and advances stage.

    Side effect (performed by caller after node returns):
      INSERT INTO run_candidates (...) VALUES (...) ON CONFLICT DO NOTHING

    Idempotency: if already past peer_review_persist (at debate or beyond), skip.
    """
    current = state.get("current_stage", "")
    if current.startswith(("debate", "synthesis", "verification", "finalize")):
        return state

    candidate_id = state.get("current_candidate_id")
    existing_ids = list(state.get("candidate_ids", []))
    if candidate_id and candidate_id not in existing_ids:
        updated_ids = existing_ids + [candidate_id]
    else:
        updated_ids = existing_ids

    return _full_state(state, {
        "current_stage": "peer_review_persist",
        "candidate_ids": updated_ids,
    })


def node_debate_call(state: OrchestrationState) -> OrchestrationState:
    """
    Debate call node — advances to debate_call stage.

    The caller (worker_loop.py) performs the debate model call and sets
    raw_response with the debate output after this node returns.

    Idempotency: if already at synthesis or beyond, skip.
    """
    current = state.get("current_stage", "")
    if current.startswith(("synthesis", "verification", "finalize")):
        return state

    return _full_state(state, {"current_stage": "debate_call"})


def node_debate_persist(state: OrchestrationState) -> OrchestrationState:
    """
    Debate persist node — records debate candidate and advances stage.

    Side effect (performed by caller after node returns):
      INSERT INTO run_candidates (...) VALUES (...) ON CONFLICT DO NOTHING

    Idempotency: if already past debate_persist (at synthesis or beyond), skip.
    """
    current = state.get("current_stage", "")
    if current.startswith(("synthesis", "verification", "finalize")):
        return state

    candidate_id = state.get("current_candidate_id")
    existing_ids = list(state.get("candidate_ids", []))
    if candidate_id and candidate_id not in existing_ids:
        updated_ids = existing_ids + [candidate_id]
    else:
        updated_ids = existing_ids

    return _full_state(state, {
        "current_stage": "debate_persist",
        "candidate_ids": updated_ids,
    })


def node_synthesis_call(state: OrchestrationState) -> OrchestrationState:
    """
    Synthesis call node — advances to synthesis_call stage.

    The caller (worker_loop.py) performs the synthesis model call and sets
    raw_response with the synthesis output after this node returns.

    Idempotency: if already at verification or beyond, skip.
    """
    current = state.get("current_stage", "")
    if current.startswith(("verification", "finalize")):
        return state

    return _full_state(state, {"current_stage": "synthesis_call"})


def node_synthesis_persist(state: OrchestrationState) -> OrchestrationState:
    """
    Synthesis persist node — records synthesis candidate and advances stage.

    Side effect (performed by caller after node returns):
      INSERT INTO run_candidates (...) VALUES (...) ON CONFLICT DO NOTHING

    Idempotency: if already past synthesis_persist, skip.
    """
    current = state.get("current_stage", "")
    if current.startswith(("verification", "finalize")):
        return state

    candidate_id = state.get("current_candidate_id")
    existing_ids = list(state.get("candidate_ids", []))
    if candidate_id and candidate_id not in existing_ids:
        updated_ids = existing_ids + [candidate_id]
    else:
        updated_ids = existing_ids

    return _full_state(state, {
        "current_stage": "synthesis_persist",
        "candidate_ids": updated_ids,
    })


def node_verification_call(state: OrchestrationState) -> OrchestrationState:
    """
    Verification call node — advances to verification_call stage.

    Idempotency: if already at finalize, skip.
    """
    current = state.get("current_stage", "")
    if current.startswith("finalize"):
        return state

    return _full_state(state, {"current_stage": "verification_call"})


def node_verification_persist(state: OrchestrationState) -> OrchestrationState:
    """
    Verification persist node — records verification candidate and advances stage.

    Side effect (performed by caller after node returns):
      INSERT INTO run_candidates (...) VALUES (...) ON CONFLICT DO NOTHING

    Idempotency: if already past verification_persist (i.e., at finalize), skip.
    """
    current = state.get("current_stage", "")
    if current.startswith("finalize"):
        return state

    candidate_id = state.get("current_candidate_id")
    existing_ids = list(state.get("candidate_ids", []))
    if candidate_id and candidate_id not in existing_ids:
        updated_ids = existing_ids + [candidate_id]
    else:
        updated_ids = existing_ids

    return _full_state(state, {
        "current_stage": "verification_persist",
        "candidate_ids": updated_ids,
    })


def node_finalize_council_success(state: OrchestrationState) -> OrchestrationState:
    """
    Finalize node (happy path) — copies computed_final_answer into final_answer.

    Idempotency guard: if already in a finalize stage, return unchanged.
    """
    current = state.get("current_stage", "")
    if current.startswith("finalize"):
        return state

    return _full_state(state, {
        "current_stage": "finalize_success",
        "final_answer": state.get("computed_final_answer"),
        "final_confidence": state.get("computed_final_confidence") if state.get("computed_final_confidence") is not None else 0.0,
    })


def node_finalize_council_failure(state: OrchestrationState) -> OrchestrationState:
    """
    Finalize node (error path) — copies error_code/error_message into state.

    Idempotency: if already in a finalize stage, return unchanged.
    """
    current = state.get("current_stage", "")
    if current.startswith("finalize"):
        return state

    return _full_state(state, {
        "current_stage": "finalize_failure",
        "error_code": state.get("error_code") or "UNKNOWN",
        "error_message": state.get("error_message") or "Council failed without error message",
    })