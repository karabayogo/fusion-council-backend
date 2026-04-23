"""API routes for fusion-council-service."""

import asyncio
import json
import sqlite3
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from fastapi.responses import JSONResponse, StreamingResponse

from fusion_council_service.auth import extract_bearer, resolve_role
from fusion_council_service.clock import utc_now_iso, utc_now_plus_seconds
from fusion_council_service.config import Settings
from fusion_council_service.db import open_db_connection, initialize_schema
from fusion_council_service.domain.budget import resolve_deadline, select_models_for_mode
from fusion_council_service.domain.event_emitter import emit_run_accepted
from fusion_council_service.domain.event_repository import list_events_for_run
from fusion_council_service.domain.run_repository import get_run, insert_run, list_runs, update_run_status
from fusion_council_service.domain.types import RespondRequest, RunRequest, RunResponse
from fusion_council_service.ids import new_run_id
from fusion_council_service.logging_utils import get_logger

logger = get_logger("fusion_council_service.api")

# Global db connection for API (shared with worker in same process)
_api_db: Optional[sqlite3.Connection] = None
_settings: Optional[Settings] = None


def get_api_db() -> sqlite3.Connection:
    global _api_db
    if _api_db is None:
        if _settings is None:
            raise RuntimeError("Settings not initialized")
        _api_db = open_db_connection(_settings.DATABASE_PATH)
        initialize_schema(_api_db)
    return _api_db


def init_api(settings: Settings) -> None:
    global _settings, _api_db
    _settings = settings
    _api_db = open_db_connection(settings.DATABASE_PATH)
    initialize_schema(_api_db)
    logger.info("API DB initialized")


def get_settings() -> Settings:
    if _settings is None:
        raise RuntimeError("Settings not initialized")
    return _settings


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


router = APIRouter()


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
    from fusion_council_service.domain.candidate_repository import list_candidates_for_run

    token, role = auth
    db = get_api_db()

    run = get_run(db, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    candidates = list_candidates_for_run(db, run_id)
    return {
        "run_id": run_id,
        "mode": run["mode"],
        "status": run["status"],
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


@router.get("/v1/runs/{run_id}/events")
async def stream_run_events(
    run_id: str,
    auth=Depends(get_auth_dependency()),
):
    """SSE stream of run events."""
    token, role = auth
    db = get_api_db()

    run = get_run(db, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    async def event_stream():
        seen = 0
        poll_interval = _settings.SSE_POLL_INTERVAL_MS / 1000.0 if _settings else 0.5

        while True:
            events = list_events_for_run(db, run_id, after_seq=seen)
            for event in events:
                seen = max(seen, event["seq"] + 1)
                payload = json.loads(event["payload_json"]) if event["payload_json"] else {}
                yield f"event: {event['event_type']}\ndata: {json.dumps(payload)}\n\n".encode()

            # Check if run is terminal
            if seen > 0:
                # Get latest event
                latest_events = list_events_for_run(db, run_id, after_seq=seen - 2)
                for ev in latest_events:
                    if ev["event_type"] in ("run.completed", "run.failed", "run.cancelled"):
                        yield "event: END\ndata: {\"done\": true}\n\n".encode()
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
