"""Phase 1 regression tests for decision log schema and pending-write hooks."""

import hashlib
import json
from unittest.mock import MagicMock, patch

import pytest

from fusion_council_service.clock import utc_now_iso, utc_now_plus_seconds
from fusion_council_service.domain.run_repository import get_run, insert_run
from fusion_council_service.domain.worker_loop import Worker


def _insert_run(tmp_db, run_id: str, mode: str, prompt: str) -> None:
    insert_run(
        db=tmp_db,
        run_id=run_id,
        mode=mode,
        prompt=prompt,
        system_prompt=None,
        temperature=0.2,
        max_output_tokens=3000,
        deadline_seconds=300,
        deadline_at=utc_now_plus_seconds(300),
        owner_token_hash="testhash",
        metadata_json=json.dumps({}),
        requested_models_json=None,
        created_at=utc_now_iso(),
    )


def _make_worker(tmp_db, model_catalog):
    worker = Worker(
        db_path=":memory:",
        registry=MagicMock(),
        catalog=model_catalog,
        poll_interval_ms=50,
        heartbeat_interval_ms=5000,
        stale_run_threshold_seconds=30,
    )
    worker._db = tmp_db
    return worker


def _decision_row(tmp_db, run_id: str):
    return tmp_db.execute(
        "SELECT * FROM decision_log WHERE run_id = ?",
        (run_id,),
    ).fetchone()


def test_schema_initializes_decision_log_table(tmp_db):
    cols = tmp_db.execute("PRAGMA table_info(decision_log)").fetchall()
    col_names = {row[1] for row in cols}
    assert {
        "run_id",
        "prompt_hash",
        "prompt",
        "mode",
        "final_answer",
        "rating",
        "outcome_raw",
        "pending",
        "reflection",
        "created_at",
        "resolved_at",
    }.issubset(col_names)

    indexes = tmp_db.execute("PRAGMA index_list(decision_log)").fetchall()
    index_names = {row[1] for row in indexes}
    assert "idx_decision_log_pending" in index_names
    assert "idx_decision_log_prompt_hash" in index_names


@pytest.mark.asyncio
async def test_single_success_writes_pending_decision_log(tmp_db, model_catalog):
    run_id = "run_decision_single_success"
    prompt = "What is 1+1?"
    _insert_run(tmp_db, run_id, "single", prompt)

    worker = _make_worker(tmp_db, model_catalog)
    run = get_run(tmp_db, run_id)
    assert run is not None

    with patch.object(
        worker,
        "_call_provider_async",
        return_value=(True, "The answer is 2.", None, None, 12, 10, 20),
    ):
        await worker._run_single(tmp_db, run)

    row = _decision_row(tmp_db, run_id)
    assert row is not None
    assert row["mode"] == "single"
    assert row["pending"] == 1
    assert row["prompt"] == prompt
    assert row["final_answer"] == "The answer is 2."
    assert row["prompt_hash"] == hashlib.sha256(prompt.encode("utf-8")).hexdigest()[:16]


@pytest.mark.asyncio
async def test_single_fallback_success_writes_pending_decision_log(tmp_db, model_catalog):
    run_id = "run_decision_single_fallback"
    _insert_run(tmp_db, run_id, "single", "Need fallback answer")

    worker = _make_worker(tmp_db, model_catalog)
    run = get_run(tmp_db, run_id)
    assert run is not None

    async def fake_provider(*_args, **_kwargs):
        if not hasattr(fake_provider, "calls"):
            fake_provider.calls = 0
        fake_provider.calls += 1
        if fake_provider.calls == 1:
            return (False, None, "AUTH_FAILED", "primary failed", 10, None, None)
        return (True, "Fallback won", None, None, 8, 5, 7)

    with patch.object(worker, "_call_provider_async", side_effect=fake_provider):
        await worker._run_single(tmp_db, run)

    row = _decision_row(tmp_db, run_id)
    assert row is not None
    assert row["mode"] == "single"
    assert row["pending"] == 1
    assert row["final_answer"] == "Fallback won"


@pytest.mark.asyncio
async def test_fusion_success_writes_pending_decision_log(tmp_db, model_catalog):
    run_id = "run_decision_fusion_success"
    _insert_run(tmp_db, run_id, "fusion", "Compare approaches")

    worker = _make_worker(tmp_db, model_catalog)
    run = get_run(tmp_db, run_id)
    assert run is not None

    async def fake_provider(request, *_args, **_kwargs):
        if "verification agent" in request.user_prompt.lower():
            return (True, json.dumps({"verdict": "pass", "confidence": 0.86}), None, None, 11, 6, 8)
        if "synthesize" in request.user_prompt.lower():
            return (True, "Fusion synthesis answer", None, None, 11, 6, 8)
        return (True, f"Candidate from {request.alias}", None, None, 9, 4, 6)

    with patch.object(worker, "_call_provider_async", side_effect=fake_provider):
        await worker._run_fusion(tmp_db, run)

    row = _decision_row(tmp_db, run_id)
    assert row is not None
    assert row["mode"] == "fusion"
    assert row["pending"] == 1
    assert "Fusion synthesis answer" in row["final_answer"]


@pytest.mark.asyncio
async def test_council_success_writes_pending_decision_log(tmp_db, model_catalog):
    run_id = "run_decision_council_success"
    _insert_run(tmp_db, run_id, "council", "Plan retirement strategy")

    worker = _make_worker(tmp_db, model_catalog)
    run = get_run(tmp_db, run_id)
    assert run is not None

    async def fake_provider(request, *_args, **_kwargs):
        prompt = request.user_prompt.lower()
        if "verification agent" in prompt:
            return (True, json.dumps({"verdict": "pass", "confidence": 0.71}), None, None, 10, 5, 7)
        if "council chair" in prompt:
            return (True, "Council synthesis answer", None, None, 10, 5, 7)
        if "peer reviewer" in prompt:
            return (True, "Peer review ok", None, None, 10, 5, 7)
        if "conflicting perspectives" in prompt:
            return (True, "Debate summary", None, None, 10, 5, 7)
        return (True, f"First opinion from {request.alias}", None, None, 10, 5, 7)

    with patch.object(worker, "_call_provider_async", side_effect=fake_provider):
        await worker._run_council(tmp_db, run)

    row = _decision_row(tmp_db, run_id)
    assert row is not None
    assert row["mode"] == "council"
    assert row["pending"] == 1
    assert "Council synthesis answer" in row["final_answer"]
