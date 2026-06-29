"""Event emitter — helpers for emitting SSE events during run execution."""

from __future__ import annotations

import json
from typing import Any

from fusion_council_service.clock import utc_now_iso
from fusion_council_service.domain.candidate_repository import get_candidate
from fusion_council_service.domain.event_repository import append_event
from fusion_council_service.logging_utils import get_logger

logger = get_logger("fusion_council_service.event_emitter")

_THOUGHT_PREVIEW_CHARS = 280


def _safe_json_object(raw: str | None) -> dict[str, Any] | None:
    if not raw or not isinstance(raw, str):
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _thought_preview(text: str) -> str:
    normalized = " ".join(text.split())
    if len(normalized) <= _THOUGHT_PREVIEW_CHARS:
        return normalized
    return normalized[: _THOUGHT_PREVIEW_CHARS - 1].rstrip() + "…"


def _candidate_live_payload(db, candidate_id: str) -> dict[str, Any]:
    candidate = get_candidate(db, candidate_id)
    if not candidate:
        return {}

    payload: dict[str, Any] = {
        "status": candidate.get("status"),
        "provider": candidate.get("provider"),
        "provider_model": candidate.get("provider_model"),
        "latency_ms": candidate.get("latency_ms"),
        "input_tokens": candidate.get("input_tokens"),
        "output_tokens": candidate.get("output_tokens"),
    }

    raw_text = candidate.get("normalized_answer")
    if isinstance(raw_text, str) and raw_text.strip():
        payload["thought_content"] = raw_text
        payload["thought_preview"] = _thought_preview(raw_text)
        payload["thought_chars"] = len(raw_text)

    score_json = candidate.get("score_json")
    verification = _safe_json_object(score_json)
    if verification:
        payload["verification"] = verification

    return payload


def emit_event(db, run_id: str, event_type: str, payload: dict) -> dict:
    """Emit a run event to the database. Returns the event envelope."""
    result = append_event(db, run_id, event_type, payload, created_at=utc_now_iso())
    logger.info(f"Event emitted: {event_type}", run_id=run_id, event_type=event_type)
    return result


def emit_run_accepted(db, run_id: str, mode: str, deadline_seconds: int) -> dict:
    return emit_event(db, run_id, "run.accepted", {
        "run_id": run_id, "mode": mode, "deadline_seconds": deadline_seconds,
    })


def emit_run_started(db, run_id: str, mode: str) -> dict:
    return emit_event(db, run_id, "run.started", {"run_id": run_id, "mode": mode})


def emit_stage_started(db, run_id: str, stage: str, models: list[str] = None) -> dict:
    payload = {"run_id": run_id, "stage": stage}
    if models:
        payload["models"] = models
    return emit_event(db, run_id, "stage.started", payload)


def emit_stage_progress(db, run_id: str, stage: str, message: str, progress_percent: float) -> dict:
    return emit_event(db, run_id, "stage.progress", {
        "run_id": run_id, "stage": stage, "message": message,
        "progress_percent": progress_percent,
    })


def emit_heartbeat(db, run_id: str, current_stage: str) -> dict:
    return emit_event(db, run_id, "heartbeat", {
        "run_id": run_id, "current_stage": current_stage,
        "timestamp": utc_now_iso(),
    })


def emit_fallback_promoted(db, run_id: str, fallback_alias: str, replaced_alias: str) -> dict:
    return emit_event(db, run_id, "fallback.promoted", {
        "run_id": run_id, "fallback_alias": fallback_alias, "replaced_alias": replaced_alias,
    })


def emit_run_finalizing(db, run_id: str) -> dict:
    return emit_event(db, run_id, "run.finalizing", {"run_id": run_id})


def emit_run_completed(db, run_id: str, final_answer: str, confidence: float = None) -> dict:
    payload = {"run_id": run_id, "final_answer": final_answer}
    if confidence is not None:
        payload["confidence"] = confidence
    return emit_event(db, run_id, "run.completed", payload)


def emit_run_failed(db, run_id: str, error_code: str, error_message: str) -> dict:
    return emit_event(db, run_id, "run.failed", {
        "run_id": run_id, "error_code": error_code, "error_message": error_message,
    })


def emit_run_cancelled(db, run_id: str) -> dict:
    return emit_event(db, run_id, "run.cancelled", {"run_id": run_id})


def emit_candidate_completed(db, run_id: str, candidate_id: str, alias: str, stage: str) -> dict:
    payload = {
        "run_id": run_id,
        "candidate_id": candidate_id,
        "alias": alias,
        "stage": stage,
    }
    payload.update(_candidate_live_payload(db, candidate_id))
    return emit_event(db, run_id, "candidate.completed", payload)


def emit_candidate_failed(db, run_id: str, candidate_id: str, alias: str, stage: str, error: str) -> dict:
    return emit_event(db, run_id, "candidate.failed", {
        "run_id": run_id, "candidate_id": candidate_id, "alias": alias, "stage": stage, "error": error,
    })


def emit_run_succeeded_degraded(db, run_id: str, final_answer: str,
                                   degraded_reason: str, confidence: float = None) -> dict:
    """Emit a succeeded_degraded event when deadline pressure forces early finalization."""
    payload = {"run_id": run_id, "final_answer": final_answer, "degraded_reason": degraded_reason}
    if confidence is not None:
        payload["confidence"] = confidence
    return emit_event(db, run_id, "run.succeeded_degraded", payload)
