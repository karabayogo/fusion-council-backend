"""API routes for fusion-council-service."""

import asyncio
import json
from typing import Literal, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from fusion_council_service.auth import extract_bearer, resolve_role
from fusion_council_service.clock import utc_now_iso, utc_now_plus_seconds
from fusion_council_service.config import Settings
from fusion_council_service.db import new_session, initialize_schema
from fusion_council_service import metrics as app_metrics
from fusion_council_service.domain.budget import resolve_deadline, select_models_for_mode
from fusion_council_service.domain.candidate_repository import list_candidates_for_run
from fusion_council_service.domain.decision_log import resolve_decision_outcome, rotate_decision_log
from fusion_council_service.domain.event_emitter import emit_run_accepted
from fusion_council_service.domain.event_repository import (
    list_event_envelopes_for_run,
    list_events_for_run,
)
from fusion_council_service.domain.model_selection import (
    get_health_scores, get_health_latencies,
)
from fusion_council_service.domain.reflection import generate_reflection
from fusion_council_service.domain.run_repository import get_run, insert_run, list_runs, update_run_status
from fusion_council_service.domain.types import RespondRequest, RunRequest, RunResponse
from fusion_council_service.ids import new_run_id
from fusion_council_service.logging_utils import get_logger
from fusion_council_service.providers.registry import ProviderRegistry

logger = get_logger("fusion_council_service.api")

# Global db connection for API
_api_db = None
_settings: Optional[Settings] = None
_registry: Optional[ProviderRegistry] = None


def get_api_db():
    global _api_db
    if _api_db is None:
        if _settings is None:
            raise RuntimeError("Settings not initialized")
        _api_db = new_session()
        initialize_schema(_api_db)
    return _api_db


def init_api(settings: Settings, registry: Optional[ProviderRegistry] = None) -> None:
    global _settings, _api_db, _registry
    _settings = settings
    _registry = registry
    _api_db = new_session()
    initialize_schema(_api_db)
    logger.info("API DB initialized")


def get_settings() -> Settings:
    if _settings is None:
        raise RuntimeError("Settings not initialized")
    return _settings


def get_api_registry() -> ProviderRegistry:
    if _registry is None:
        raise RuntimeError("Provider registry not initialized")
    return _registry


def get_auth_dependency():
    """Factory for auth dependency using app settings.

    Supports both Authorization: Bearer header (standard clients) and
    ?auth=<token> query param (SSE clients via EventSource).
    """
    def dependency(
        authorization: Optional[str] = Header(None),
        auth_query: Optional[str] = Query(default=None, alias="auth"),
    ):
        settings = get_settings()
        # Prefer header; fall back to query param for SSE/EventSource clients
        token = extract_bearer(authorization)
        if token is None:
            token = auth_query
        if token is None:
            raise HTTPException(status_code=401, detail="Missing Authorization header")
        role = resolve_role(token, settings.service_api_keys, settings.service_admin_api_keys)
        if role is None:
            raise HTTPException(status_code=401, detail="Invalid API key")
        return token, role
    return dependency


_STAGE_ORDER = {
    "generation": 10,
    "first_opinion": 20,
    "peer_review": 30,
    "debate": 40,
    "synthesis": 50,
    "verification": 60,
}

_DEGRADED_SKIPPED_STAGES = {
    "council_skip_debate": ["debate"],
    "council_skip_peer_review": ["peer_review", "debate"],
    "council_deadline_imminent_return_best_opinion": ["peer_review", "debate", "synthesis", "verification"],
    "fusion_approaching_deadline_skip_verification": ["verification"],
    "fusion_deadline_imminent_return_best_candidate": ["verification"],
}


def _candidate_contract(candidate: dict, fallback_order: int) -> dict:
    """Map a persisted candidate row to the stable answers v1 contract."""
    row = dict(candidate)
    execution_order = row.get("execution_order") or fallback_order
    row["execution_order"] = execution_order
    normalized = row.get("normalized_answer")
    row["raw_text"]   = normalized
    row["raw_answer"] = normalized   # contract: raw_text and raw_answer are aliases
    return row


def _parse_event_payload(event: dict) -> dict:
    try:
        return json.loads(event.get("payload_json") or "{}")
    except json.JSONDecodeError:
        return {}


def _stage_summaries(run: dict, candidates: list[dict], events: list[dict], db: object = None) -> list[dict]:
    """Build orchestration-stage summaries without creating fake candidate rows."""
    by_stage: dict[str, dict] = {}

    # Skip terminal "complete" stage - it's set by update_run_status but isn't a real stage
    # This prevents duplicate "complete" entries in the stages list
    TERMINAL_STAGES = {"complete", "failed", "cancelled"}

    def append_model(summary: dict, alias: str | None) -> None:
        if not alias:
            return
        models = summary.setdefault("models", [])
        if alias not in models:
            models.append(alias)

    for event in events:
        payload = _parse_event_payload(event)
        stage = payload.get("stage")
        if not stage:
            continue
        summary = by_stage.setdefault(stage, {
            "stage": stage,
            "status": "started",
            "candidate_count": 0,
            "models": [],
            "started_at": event.get("created_at"),
        })
        if event.get("event_type") == "stage.started":
            summary["status"] = "started"
            summary["started_at"] = summary.get("started_at") or event.get("created_at")
            for alias in payload.get("models") or []:
                append_model(summary, alias)

    for candidate in candidates:
        stage = candidate.get("stage")
        if not stage:
            continue
        summary = by_stage.setdefault(stage, {
            "stage": stage,
            "status": "completed",
            "candidate_count": 0,
            "models": [],
            "started_at": candidate.get("created_at"),
        })
        summary["candidate_count"] = int(summary.get("candidate_count") or 0) + 1
        append_model(summary, candidate.get("alias"))
        if candidate.get("status") == "failed" and summary.get("status") != "completed":
            summary["status"] = "failed"
        else:
            summary["status"] = "completed"

    current_stage = run.get("current_stage")
    degraded_reason = run.get("degraded_reason")
    
    # Skip terminal stages - they're not real orchestration stages
    if current_stage and current_stage not in TERMINAL_STAGES:
        summary = by_stage.setdefault(current_stage, {
            "stage": current_stage,
            "candidate_count": 0,
            "models": [],
            "started_at": None,
        })
        if degraded_reason and summary.get("candidate_count", 0) == 0:
            summary["status"] = "skipped"
            summary["degraded_reason"] = degraded_reason
        else:
            summary.setdefault("status", "current")

    for skipped_stage in _DEGRADED_SKIPPED_STAGES.get(degraded_reason, []):
        summary = by_stage.setdefault(skipped_stage, {
            "stage": skipped_stage,
            "candidate_count": 0,
            "models": [],
            "started_at": None,
        })
        if summary.get("candidate_count", 0) == 0:
            summary["status"] = "skipped"
            summary["degraded_reason"] = degraded_reason

    # Enrich stages with selection metadata for explainability
    if db is not None:
        run_id = run.get("run_id", "")
        health_scores = get_health_scores(db)
        health_latencies = get_health_latencies(db)

        # Build excluded upstreams from same-run failed candidates
        seen_excluded: set = set()
        excluded_upstreams = []
        for candidate in candidates:
            if candidate.get("status") == "failed":
                pair = (candidate.get("provider", ""), candidate.get("provider_model", ""))
                if pair not in seen_excluded and pair[0] and pair[1]:
                    seen_excluded.add(pair)
                    excluded_upstreams.append({
                        "provider": pair[0],
                        "provider_model": pair[1],
                        "alias": candidate.get("alias", ""),
                        "stage": candidate.get("stage", ""),
                    })

        for stage in by_stage.values():
            stage_name = stage.get("stage", "")
            stage_candidates = [c for c in candidates if c.get("stage") == stage_name]
            candidates_health = []
            for c in stage_candidates:
                pair = (c.get("provider", ""), c.get("provider_model", ""))
                score = health_scores.get(pair, 1.0)
                latency = health_latencies.get(pair)
                candidates_health.append({
                    "alias": c.get("alias", ""),
                    "provider": c.get("provider", ""),
                    "provider_model": c.get("provider_model", ""),
                    "status": c.get("status", ""),
                    "health_score": round(score, 4),
                    "avg_latency_ms": latency,
                })
            stage["selection_metadata"] = {
                "candidates_health": candidates_health,
                "excluded_upstreams": excluded_upstreams,
            }

    return sorted(by_stage.values(), key=lambda s: (_STAGE_ORDER.get(s["stage"], 999), s["stage"]))


router = APIRouter()


_DEFAULT_OUTCOME_RAW_BY_RATING = {
    "helpful": 5.0,
    "partial": 3.0,
    "not_helpful": 1.0,
}


class OutcomeRequest(BaseModel):
    rating: Literal["helpful", "not_helpful", "partial"]
    outcome_raw: Optional[float] = Field(default=None, ge=1.0, le=5.0)


class OutcomeResponse(BaseModel):
    ok: bool
    run_id: str
    resolution: dict


@router.post("/v1/runs", response_model=RunResponse)
async def create_run(
    body: RunRequest,
    auth=Depends(get_auth_dependency()),
):
    """Create a new run (async, returns 202 immediately)."""
    token, role = auth
    settings = get_settings()
    db = get_api_db()

    if body.mode not in ("single", "fusion", "council"):
        raise HTTPException(status_code=400, detail=f"Invalid mode: {body.mode}")

    deadline_seconds, deadline_applied = resolve_deadline(body.mode, body.deadline_seconds)
    deadline_at = utc_now_plus_seconds(deadline_seconds)
    run_id = new_run_id()
    created_at = utc_now_iso()

    # Compute metadata
    models = select_models_for_mode(body.mode, _catalog_from_settings(), body.requested_models)
    metadata = {
        "requested_models": body.requested_models,
        "degrade_threshold": 0.80 if body.mode == "council" else 0.85,
    }
    if body.metadata:
        metadata.update(body.metadata)

    insert_run(
        db=db,
        run_id=run_id,
        mode=body.mode,
        prompt=body.prompt,
        system_prompt=body.system_prompt,
        temperature=body.temperature,
        max_output_tokens=body.max_output_tokens,
        deadline_seconds=deadline_seconds,
        deadline_at=deadline_at,
        owner_token_hash=_hash_token(token),
        metadata_json=json.dumps(metadata),
        requested_models_json=json.dumps(body.requested_models) if body.requested_models else None,
        created_at=created_at,
    )

    # Update deadline_applied
    update_run_status(db, run_id, "queued",
                      deadline_applied=deadline_applied,
                      current_stage="queued",
                      models_planned=len(models),
                      last_heartbeat_at=created_at)

    # Emit accepted event
    emit_run_accepted(db, run_id, body.mode, deadline_seconds)

    base_url = f"http://{settings.HOST}:{settings.PORT}"
    return RunResponse(
        run_id=run_id,
        status="queued",
        status_url=f"{base_url}/v1/runs/{run_id}",
        events_url=f"{base_url}/v1/runs/{run_id}/events",
        answers_url=f"{base_url}/v1/runs/{run_id}/answers",
        suggested_poll_interval_ms=settings.SSE_POLL_INTERVAL_MS,
    )


@router.get("/v1/runs/{run_id}/answers")
async def get_run_answers(
    run_id: str,
    auth=Depends(get_auth_dependency()),
):
    """Return raw candidate answers and intermediate artifacts for a run."""

    token, role = auth
    db = get_api_db()

    run = get_run(db, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    candidates = [
        _candidate_contract(candidate, idx)
        for idx, candidate in enumerate(list_candidates_for_run(db, run_id), start=1)
    ]
    events = list_events_for_run(db, run_id)
    # Record API-scrapable observability signals from persisted artifacts.
    app_metrics.observe_answers_payload_once(run_id, candidates)
    return {
        "schema_version": "v1",
        "run_id": run_id,
        "mode": run["mode"],
        "status": run["status"],
        "stages": _stage_summaries(run, candidates, events, db=db),
        "candidates": candidates,
        "count": len(candidates),
    }


@router.post("/v1/runs/{run_id}/cancel")
async def cancel_run(
    run_id: str,
    auth=Depends(get_auth_dependency()),
):
    """Cancel a queued or running run."""
    token, role = auth
    db = get_api_db()

    run = get_run(db, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    # Only admin can cancel
    if role != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    if run["status"] not in ("queued", "running"):
        raise HTTPException(status_code=409, detail=f"Cannot cancel run in status: {run['status']}")

    update_run_status(db, run_id, "cancelled")
    return {"ok": True, "run_id": run_id}


@router.get("/v1/runs/{run_id}")
async def get_run_status(
    run_id: str,
    auth=Depends(get_auth_dependency()),
):
    """Get current status of a run."""
    token, role = auth
    db = get_api_db()

    run = get_run(db, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    return {
        "run_id": run["run_id"],
        "status": run["status"],
        "mode": run["mode"],
        "current_stage": run.get("current_stage"),
        "current_stage_message": run.get("current_stage_message"),
        "progress_percent": run.get("progress_percent"),
        "last_heartbeat_at": run.get("last_heartbeat_at"),
        "deadline_at": run.get("deadline_at"),
        "deadline_applied": run.get("deadline_applied"),
        "models_planned": run.get("models_planned", 0),
        "models_completed": run.get("models_completed", 0),
        "models_failed": run.get("models_failed", 0),
        "final_answer": run.get("final_answer"),
        "final_confidence": run.get("final_confidence"),
        "error_code": run.get("error_code"),
        "error_message": run.get("error_message"),
        "created_at": run["created_at"],
        "started_at": run.get("started_at"),
        "finished_at": run.get("finished_at"),
    }


@router.get("/v1/runs/{run_id}/events/history")
async def list_run_event_history(
    run_id: str,
    auth=Depends(get_auth_dependency()),
):
    """Return the full persisted event history for a run in replay-friendly envelope form."""
    token, role = auth
    db = get_api_db()

    run = get_run(db, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    events = list_event_envelopes_for_run(db, run_id)
    response = JSONResponse(
        {
            "run_id": run_id,
            "events": events,
            "count": len(events),
            "last_seq": events[-1]["seq"] if events else 0,
        }
    )
    response.headers["Cache-Control"] = "no-store"
    return response


@router.get("/v1/runs/{run_id}/events")
async def stream_run_events(
    run_id: str,
    after_seq: int = Query(default=0, ge=0),
    auth=Depends(get_auth_dependency()),
):
    """SSE stream of run events."""
    token, role = auth
    db = get_api_db()

    run = get_run(db, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    terminal_statuses = {"succeeded", "succeeded_degraded", "failed", "cancelled"}
    terminal_events = {"run.completed", "run.failed", "run.cancelled", "run.succeeded_degraded"}

    async def event_stream():
        seen = after_seq
        poll_interval = _settings.SSE_POLL_INTERVAL_MS / 1000.0 if _settings else 0.5

        while True:
            events = list_event_envelopes_for_run(db, run_id, after_seq=seen)
            if events:
                terminal_seen = False
                for event in events:
                    seen = max(seen, int(event["seq"]))
                    # Emit default 'message' events only - browsers receive these via EventSource.onmessage
                    # Custom event names (event: X) are NOT received by onmessage
                    yield f"data: {json.dumps(event)}\n\n".encode()
                    if event["event_type"] in terminal_events:
                        terminal_seen = True

                if terminal_seen:
                    # Terminal envelope uses default message event too
                    yield "data: {\"done\": true, \"event_type\": \"terminal\"}\n\n".encode()
                    return
            else:
                latest_run = get_run(db, run_id)
                if latest_run and latest_run["status"] in terminal_statuses:
                    yield "data: {\"done\": true, \"event_type\": \"terminal\"}\n\n".encode()
                    return

            await asyncio.sleep(poll_interval)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/v1/runs")
async def list_runs_endpoint(
    limit: int = Query(default=50, ge=1, le=200),
    auth=Depends(get_auth_dependency()),
):
    """List recent runs."""
    token, role = auth
    db = get_api_db()
    runs = list_runs(db, limit=limit)
    return {"runs": runs, "count": len(runs)}


@router.patch("/v1/runs/{run_id}/outcome", response_model=OutcomeResponse)
async def submit_outcome(
    run_id: str,
    body: OutcomeRequest,
    auth=Depends(get_auth_dependency()),
):
    """Resolve a pending decision with user outcome feedback and reflection."""
    token, role = auth
    _ = (token, role)
    db = get_api_db()
    settings = get_settings()

    run = get_run(db, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    if run.get("status") not in {"succeeded", "succeeded_degraded"}:
        raise HTTPException(
            status_code=409,
            detail=f"Run {run_id} is not in a completable status for outcome submission",
        )

    outcome_raw = (
        float(body.outcome_raw)
        if body.outcome_raw is not None
        else _DEFAULT_OUTCOME_RAW_BY_RATING[body.rating]
    )

    try:
        registry = get_api_registry()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    def reflection_generator(*, prompt: str, final_answer: str, rating: str, outcome_raw: float) -> str:
        return generate_reflection(
            prompt=prompt,
            final_answer=final_answer,
            rating=rating,
            outcome_raw=outcome_raw,
            provider_registry=registry,
            backup_role_alias=settings.REFLECTION_ROLE_ALIAS,
        )

    try:
        resolution = resolve_decision_outcome(
            db=db,
            run_id=run_id,
            rating=body.rating,
            outcome_raw=outcome_raw,
            generate_reflection_fn=reflection_generator,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    rotate_decision_log(db, max_resolved_entries=settings.DECISION_LOG_MAX_ENTRIES)

    return OutcomeResponse(ok=True, run_id=run_id, resolution=resolution)


@router.post("/v1/respond")
async def respond_sync(
    body: RespondRequest,
    auth=Depends(get_auth_dependency()),
):
    """Synchronous respond helper — creates run and waits for completion.
    Returns 200 with final answer, or 202 if wait_timeout reached (check status_url)."""
    import uuid as uuid_mod
    token, role = auth
    settings = get_settings()
    db = get_api_db()

    if body.mode not in ("single", "fusion", "council"):
        raise HTTPException(status_code=400, detail=f"Invalid mode: {body.mode}")

    deadline_seconds, deadline_applied = resolve_deadline(body.mode, body.deadline_seconds)
    deadline_at = utc_now_plus_seconds(deadline_seconds)
    wait_timeout = body.wait_timeout_seconds or min(deadline_seconds, settings.SYNC_TIMEOUT_SECONDS)
    run_id = f"run_{uuid_mod.uuid4().hex[:24]}"
    created_at = utc_now_iso()

    models = select_models_for_mode(body.mode, _catalog_from_settings(), body.requested_models)
    metadata = {"requested_models": body.requested_models, "sync_respond": True}
    if body.metadata:
        metadata.update(body.metadata)

    insert_run(
        db=db,
        run_id=run_id,
        mode=body.mode,
        prompt=body.prompt,
        system_prompt=body.system_prompt,
        temperature=body.temperature,
        max_output_tokens=body.max_output_tokens,
        deadline_seconds=deadline_seconds,
        deadline_at=deadline_at,
        owner_token_hash=_hash_token(token),
        metadata_json=json.dumps(metadata),
        requested_models_json=json.dumps(body.requested_models) if body.requested_models else None,
        created_at=created_at,
    )
    update_run_status(db, run_id, "queued",
                      deadline_applied=deadline_applied,
                      current_stage="queued",
                      models_planned=len(models),
                      last_heartbeat_at=created_at)
    emit_run_accepted(db, run_id, body.mode, deadline_seconds)

    base_url = f"http://{settings.HOST}:{settings.PORT}"
    status_url = f"{base_url}/v1/runs/{run_id}"

    # Poll for completion with timeout
    poll_interval = settings.SSE_POLL_INTERVAL_MS / 1000.0
    elapsed = 0.0

    while elapsed < wait_timeout:
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval
        run = get_run(db, run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="Run disappeared")
        if run["status"] in ("succeeded", "failed", "cancelled"):
            if run["status"] == "succeeded":
                return {
                    "ok": True,
                    "run_id": run_id,
                    "status": "succeeded",
                    "final_answer": run.get("final_answer"),
                    "final_confidence": run.get("final_confidence"),
                    "mode": run["mode"],
                    "error_code": None,
                }
            elif run["status"] == "failed":
                return JSONResponse({
                    "ok": False,
                    "run_id": run_id,
                    "status": "failed",
                    "error_code": run.get("error_code"),
                    "error_message": run.get("error_message"),
                }, status_code=500)
            else:
                return JSONResponse({
                    "ok": False,
                    "run_id": run_id,
                    "status": "cancelled",
                }, status_code=409)

    # Timeout — return 202 with status URL for async polling
    return JSONResponse({
        "ok": False,
        "status": "timeout",
        "run_id": run_id,
        "status_url": status_url,
        "events_url": f"{base_url}/v1/runs/{run_id}/events",
        "message": f"Wait timeout ({wait_timeout}s) reached. Poll {status_url} for result.",
    }, status_code=202)


def _catalog_from_settings():
    """Get model catalog from settings — lazy import to avoid circular."""
    from fusion_council_service.model_catalog import ModelCatalog
    from fusion_council_service.model_catalog import load_yaml_catalog
    models = load_yaml_catalog(_settings.MODEL_CATALOG_PATH)
    return ModelCatalog(models)


@router.get("/v1/models")
async def list_models(auth=Depends(get_auth_dependency())):
    """Return the model alias catalog and enablement status."""
    catalog = _catalog_from_settings()
    models = catalog.all_models()
    return {
        "models": [
            {
                "alias": m["alias"],
                "provider": m["provider"],
                "provider_model": m["provider_model"],
                "family": m["family"],
                "tier": m["tier"],
                "enabled": m.get("enabled", True),
            }
            for m in models
        ],
        "count": len(models),
    }


def _hash_token(token: str) -> str:
    import hashlib
    return hashlib.sha256(token.encode()).hexdigest()
