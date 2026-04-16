"""Event emitter — helpers for emitting SSE events during run execution."""

import json
import sqlite3

from fusion_council_service.clock import utc_now_iso
from fusion_council_service.domain.event_repository import append_event, get_next_seq
from fusion_council_service.logging_utils import get_logger

logger = get_logger("fusion_council_service.event_emitter")


def emit_event(db: sqlite3.Connection, run_id: str, event_type: str, payload: dict) -> dict:
    """Emit a run event to the database. Returns the event dict."""
    seq = get_next_seq(db, run_id)
    created_at = utc_now_iso()
    payload_json = json.dumps(payload, ensure_ascii=False)
    result = append_event(db, run_id, event_type, payload_json, seq, created_at)
    logger.info(f"Event emitted: {event_type}", run_id=run_id, event_type=event_type)
    return result


def emit_run_accepted(db: sqlite3.Connection, run_id: str, mode: str, deadline_seconds: int) -> dict:
    return emit_event(db, run_id, "run.accepted", {
        "run_id": run_id, "mode": mode, "deadline_seconds": deadline_seconds,
    })


def emit_run_started(db: sqlite3.Connection, run_id: str, mode: str) -> dict:
    return emit_event(db, run_id, "run.started", {"run_id": run_id, "mode": mode})


def emit_stage_started(db: sqlite3.Connection, run_id: str, stage: str, models: list[str] = None) -> dict:
    payload = {"run_id": run_id, "stage": stage}
    if models:
        payload["models"] = models
    return emit_event(db, run_id, "stage.started", payload)


def emit_stage_progress(db: sqlite3.Connection, run_id: str, stage: str, message: str, progress_percent: float) -> dict:
    return emit_event(db, run_id, "stage.progress", {
        "run_id": run_id, "stage": stage, "message": message,
        "progress_percent": progress_percent,
    })


def emit_heartbeat(db: sqlite3.Connection, run_id: str, current_stage: str) -> dict:
    return emit_event(db, run_id, "heartbeat", {
        "run_id": run_id, "current_stage": current_stage,
        "timestamp": utc_now_iso(),
    })


def emit_fallback_promoted(db: sqlite3.Connection, run_id: str, fallback_alias: str, replaced_alias: str) -> dict:
    return emit_event(db, run_id, "fallback.promoted", {
        "run_id": run_id, "fallback_alias": fallback_alias, "replaced_alias": replaced_alias,
    })


def emit_run_finalizing(db: sqlite3.Connection, run_id: str) -> dict:
    return emit_event(db, run_id, "run.finalizing", {"run_id": run_id})


def emit_run_completed(db: sqlite3.Connection, run_id: str, final_answer: str, confidence: float = None) -> dict:
    payload = {"run_id": run_id, "final_answer": final_answer}
    if confidence is not None:
        payload["confidence"] = confidence
    return emit_event(db, run_id, "run.completed", payload)


def emit_run_failed(db: sqlite3.Connection, run_id: str, error_code: str, error_message: str) -> dict:
    return emit_event(db, run_id, "run.failed", {
        "run_id": run_id, "error_code": error_code, "error_message": error_message,
    })


def emit_run_cancelled(db: sqlite3.Connection, run_id: str) -> dict:
    return emit_event(db, run_id, "run.cancelled", {"run_id": run_id})


def emit_candidate_completed(db: sqlite3.Connection, run_id: str, candidate_id: str, alias: str, stage: str) -> dict:
    return emit_event(db, run_id, "candidate.completed", {
        "run_id": run_id, "candidate_id": candidate_id, "alias": alias, "stage": stage,
    })


def emit_candidate_failed(db: sqlite3.Connection, run_id: str, candidate_id: str, alias: str, stage: str, error: str) -> dict:
    return emit_event(db, run_id, "candidate.failed", {
        "run_id": run_id, "candidate_id": candidate_id, "alias": alias, "stage": stage, "error": error,
    })


def emit_run_succeeded_degraded(db: sqlite3.Connection, run_id: str, final_answer: str,
                                   degraded_reason: str, confidence: float = None) -> dict:
    """Emit a succeeded_degraded event when deadline pressure forces early finalization."""
    payload = {"run_id": run_id, "final_answer": final_answer, "degraded_reason": degraded_reason}
    if confidence is not None:
        payload["confidence"] = confidence
    return emit_event(db, run_id, "run.succeeded_degraded", payload)