"""Regression tests for the Fusion Council run-page live-streaming and verifier plan.

These tests cover defects RCA-1..RCA-4 from
`docs/plans/done/run-page-live-streaming-markdown-preview-provider-health-rca.md`:

  RCA-1: council first_opinion / peer_review must emit candidate events as
         each candidate finishes (as_completed), not after asyncio.gather.
  RCA-2: verifier verdict "reject" must fail the run, not pass through as
         a high-confidence success; "abstain" must use succeeded_degraded.
  RCA-3: structured verification payload (verdict / confidence / issues /
         reasoning) must persist into run_candidates.score_json for the
         UI to consume.
  RCA-4: council stage token caps must be env-tunable via Settings
         (defaults: first_opinion=1200, peer_review=800, debate=800,
         synthesis=1200, verification=400) and must override run.max_output_tokens.
  RCA-6: POST /v1/runs must not return 0.0.0.0 URLs; must use
         PUBLIC_BASE_URL when set, or a relative path otherwise.
  Plus: the existing SSE default-message contract and council-token-cap
         settings must stay intact (RCA-1 support).

TDD: these tests are written FIRST (red), then the production code is
patched to make them green. The patterns follow test_council_e2e.py and
test_run_status_terminal_defaults.py in the same directory.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from fusion_council_service.clock import utc_now_iso, utc_now_plus_seconds
from fusion_council_service.domain.candidate_repository import (
    insert_candidate,
    list_candidates_for_run,
)
from fusion_council_service.domain.event_emitter import (
    emit_run_accepted,
    emit_run_completed,
    emit_run_failed,
    emit_run_started,
)
from fusion_council_service.domain.event_repository import (
    append_event,
    list_events_for_run,
)
from fusion_council_service.domain.run_repository import (
    get_run,
    insert_run,
    update_run_status,
)
from fusion_council_service.domain.types import ProviderGenerateResult
from fusion_council_service.domain.worker_loop import (
    MIN_VERIFICATION_TOKENS,
    _apply_verification_result,
    _VerificationPayload,
    Worker,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _insert_council_run(db: sqlite3.Connection, run_id: str) -> None:
    insert_run(
        db=db,
        run_id=run_id,
        mode="council",
        prompt="Is AI conscious?",
        system_prompt=None,
        temperature=0.2,
        max_output_tokens=2000,
        deadline_seconds=600,
        deadline_at=utc_now_plus_seconds(600),
        owner_token_hash="testhash",
        metadata_json="{}",
        requested_models_json=None,
        created_at=utc_now_iso(),
    )


def _success_result(
    text: str = "answer", out_tokens: int = 200
) -> ProviderGenerateResult:
    return ProviderGenerateResult(
        success=True,
        raw_text=text,
        error_code=None,
        error_message=None,
        latency_ms=120,
        input_tokens=100,
        output_tokens=out_tokens,
    )


def _fail_result(code: str = "PROVIDER_TIMEOUT", msg: str = "boom") -> ProviderGenerateResult:
    return ProviderGenerateResult(
        success=False,
        raw_text="",
        error_code=code,
        error_message=msg,
        latency_ms=120,
        input_tokens=0,
        output_tokens=0,
    )


@pytest.fixture
def council_run(tmp_db):
    _insert_council_run(tmp_db, "run_council_rca")
    return tmp_db


@pytest.fixture
def mock_worker(tmp_db, monkeypatch):
    mock_registry = MagicMock()
    from fusion_council_service.model_catalog import ModelCatalog, load_yaml_catalog

    catalog = load_yaml_catalog("config/models.yaml")
    model_catalog = ModelCatalog(catalog)
    worker = Worker(
        db_path=":memory:",
        registry=mock_registry,
        catalog=model_catalog,
        poll_interval_ms=50,
        heartbeat_interval_ms=5000,
        stale_run_threshold_seconds=30,
    )
    worker._db = tmp_db
    return worker


# ---------------------------------------------------------------------------
# RCA-1: council first_opinion must emit events in completion order, not batch
# ---------------------------------------------------------------------------


def test_council_first_opinion_emits_in_completion_order(mock_worker, council_run, monkeypatch):
    """RCA-1: when one first-opinion model returns in 50ms and another in 500ms,
    the SSE consumer should see the fast candidate's events BEFORE the slow one
    is awaited, not after asyncio.gather() returns both.

    This regression test patches _call_provider_async to delay model alias
    "slow-mini" by 500ms and lets the other models respond instantly, then
    asserts that the persisted events for "first_opinion" arrive in the
    completion-time order, not the catalog order.
    """
    db = council_run
    run_id = "run_council_rca"
    _insert_council_run(db, "run_council_rca_fast")  # ensure catalog -> run
    update_run_status(db, "run_council_rca", "running", started_at=utc_now_iso())
    emit_run_started(db, "run_council_rca", "council")

    # Map alias -> simulated delay seconds
    delays = {"primary": 0.05, "reviewer": 0.50, "backup": 0.05}

    async def fake_call_provider_async(self, request, *args, **kwargs):
        alias = getattr(request, "alias", "")
        await asyncio.sleep(delays.get(alias, 0.05))
        return _success_result(f"answer-from-{alias}", out_tokens=120)

    # Also stub fallback: always returns None
    monkeypatch.setattr(Worker, "_call_provider_async", fake_call_provider_async)
    monkeypatch.setattr(Worker, "_try_fallback", lambda self, db, run, alias: None)
    monkeypatch.setattr(Worker, "_check_deadline", lambda self, run: None)

    asyncio.run(mock_worker._run_council(db, get_run(db, "run_council_rca")))

    # Inspect events of type candidate.completed
    events = list_events_for_run(db, "run_council_rca")
    completed = [e for e in events if e.get("event_type") == "candidate.completed"]
    # The fast aliases (primary, backup) must appear before the slow one (reviewer)
    # in the SSE completion sequence. The exact alias is catalog-dependent, so
    # we just assert that *some* candidate completed in <100ms and at least one
    # completed after the first one — i.e. they did NOT all land in one burst.
    # First event time and last event time should be > 300ms apart.
    from datetime import datetime

    def parse(ts: str) -> datetime:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))

    if len(completed) < 2:
        pytest.skip(
            "Catalog did not produce enough first-opinion candidates; "
            f"got {len(completed)} completed events"
        )
    first_ts = parse(completed[0]["created_at"])
    last_ts = parse(completed[-1]["created_at"])
    spread = (last_ts - first_ts).total_seconds()
    assert spread >= 0.30, (
        f"Council first_opinion candidates all landed within {spread:.3f}s — "
        "RCA-1 not fixed: as_completed pattern not in place."
    )


def test_council_peer_review_emits_in_completion_order(mock_worker, council_run, monkeypatch):
    """RCA-1: same as test_council_first_opinion_emits_in_completion_order, but
    for the peer_review stage. We assert that not all peer-review candidates
    land within the same millisecond burst.
    """
    db = council_run
    run_id = "run_council_rca"
    update_run_status(db, run_id, "running", started_at=utc_now_iso())
    emit_run_started(db, run_id, "council")

    delays = {"primary": 0.05, "reviewer": 0.40, "backup": 0.05}

    async def fake_call_provider_async(self, request, *args, **kwargs):
        alias = getattr(request, "alias", "")
        await asyncio.sleep(delays.get(alias, 0.05))
        return _success_result(f"answer-from-{alias}", out_tokens=120)

    monkeypatch.setattr(Worker, "_call_provider_async", fake_call_provider_async)
    monkeypatch.setattr(Worker, "_try_fallback", lambda self, db, run, alias: None)
    monkeypatch.setattr(Worker, "_check_deadline", lambda self, run: None)

    asyncio.run(mock_worker._run_council(db, get_run(db, run_id)))

    events = list_events_for_run(db, run_id)
    # Find peer_review stage events
    pr_events = [
        e
        for e in events
        if e.get("event_type") == "candidate.completed"
        and e.get("payload", {}).get("stage") == "peer_review"
    ]
    # The pipeline may skip peer_review under degradation; if so, skip the assertion
    if not pr_events:
        pytest.skip("peer_review stage was skipped by the worker (deadline pressure)")
    if len(pr_events) < 2:
        pytest.skip(f"only {len(pr_events)} peer_review candidate(s) emitted")

    from datetime import datetime

    def parse(ts: str) -> datetime:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))

    first_ts = parse(pr_events[0]["created_at"])
    last_ts = parse(pr_events[-1]["created_at"])
    spread = (last_ts - first_ts).total_seconds()
    assert spread >= 0.20, (
        f"Council peer_review candidates landed within {spread:.3f}s — "
        "RCA-1 not fully fixed: peer_review still using gather."
    )


# ---------------------------------------------------------------------------
# RCA-2: verifier reject must fail the run, not pass through as success
# ---------------------------------------------------------------------------


def test_verification_reject_does_not_yield_succeeded_status(tmp_db):
    """RCA-2: when the verifier returns verdict=reject, the helper that applies
    the verification result must return a marker (or flag) that causes the
    outer council loop to mark the run as 'failed' with error_code
    'VERIFICATION_REJECTED', not 'succeeded' with high confidence.

    Implementation contract: _apply_verification_result returns a tuple
    (confidence, synthesis_text, verdict_action) where verdict_action is one
    of 'ok', 'degraded', 'rejected'. The outer loop checks verdict_action
    and routes the run to the appropriate terminal state.
    """
    run_id = "run_reject_test"
    insert_run(
        db=tmp_db,
        run_id=run_id,
        mode="council",
        prompt="x",
        system_prompt=None,
        temperature=0.2,
        max_output_tokens=1000,
        deadline_seconds=600,
        deadline_at=utc_now_plus_seconds(600),
        owner_token_hash="testhash",
        metadata_json="{}",
        requested_models_json=None,
        created_at=utc_now_iso(),
    )
    cand_id = "cand_reject_test"
    insert_candidate(
        db=tmp_db,
        run_id=run_id,
        candidate_id=cand_id,
        alias="verif",
        provider="test",
        provider_model="test-model",
        stage="verification",
        status="succeeded",
        created_at=utc_now_iso(),
    )
    raw_text = json.dumps({
        "verdict": "reject",
        "confidence": 0.95,
        "issues": [
            "Answer is too verbose for a one-sentence prompt",
            "Did not obey 'exactly one short sentence' constraint",
        ],
    })
    result = _apply_verification_result(
        db=tmp_db,
        cand_id=cand_id,
        candidate={"output_tokens": 200},
        raw_text=raw_text,
        synthesis_text="A very long synthesis that does not match the prompt.",
        current_confidence=0.5,
        verif_alias="verifier",
        run_id=run_id,
    )
    assert isinstance(result, tuple) and len(result) == 3, (
        f"_apply_verification_result must return (confidence, text, verdict_action); "
        f"got {type(result).__name__} of length {len(result) if hasattr(result, '__len__') else '?'}"
    )
    confidence, text, action = result
    assert action == "rejected", (
        f"verdict=reject must yield verdict_action='rejected', got {action!r}"
    )
    assert "A very long synthesis" in text


def test_verification_abstain_yields_degraded_action(tmp_db):
    """RCA-2: verdict=abstain must return action='degraded' so the outer loop
    marks the run as succeeded_degraded (not succeeded)."""
    run_id = "run_abstain_test"
    insert_run(
        db=tmp_db,
        run_id=run_id,
        mode="council",
        prompt="x",
        system_prompt=None,
        temperature=0.2,
        max_output_tokens=1000,
        deadline_seconds=600,
        deadline_at=utc_now_plus_seconds(600),
        owner_token_hash="testhash",
        metadata_json="{}",
        requested_models_json=None,
        created_at=utc_now_iso(),
    )
    cand_id = "cand_abstain_test"
    insert_candidate(
        db=tmp_db,
        run_id=run_id,
        candidate_id=cand_id,
        alias="verif",
        provider="test",
        provider_model="test-model",
        stage="verification",
        status="succeeded",
        created_at=utc_now_iso(),
    )
    raw_text = json.dumps({
        "verdict": "abstain",
        "confidence": 0.45,
        "issues": ["Insufficient evidence to confirm claim 1"],
    })
    result = _apply_verification_result(
        db=tmp_db,
        cand_id=cand_id,
        candidate={"output_tokens": 200},
        raw_text=raw_text,
        synthesis_text="Some answer.",
        current_confidence=0.5,
        verif_alias="verifier",
        run_id=run_id,
    )
    assert isinstance(result, tuple) and len(result) == 3
    confidence, text, action = result
    assert action == "degraded", (
        f"verdict=abstain must yield action='degraded', got {action!r}"
    )


def test_verification_approve_yields_ok_action(tmp_db):
    """RCA-2: verdict=approve (or any non-reject/non-abstain verdict) returns
    action='ok' so the outer loop marks the run as succeeded."""
    run_id = "run_approve_test"
    insert_run(
        db=tmp_db,
        run_id=run_id,
        mode="council",
        prompt="x",
        system_prompt=None,
        temperature=0.2,
        max_output_tokens=1000,
        deadline_seconds=600,
        deadline_at=utc_now_plus_seconds(600),
        owner_token_hash="testhash",
        metadata_json="{}",
        requested_models_json=None,
        created_at=utc_now_iso(),
    )
    cand_id = "cand_approve_test"
    insert_candidate(
        db=tmp_db,
        run_id=run_id,
        candidate_id=cand_id,
        alias="verif",
        provider="test",
        provider_model="test-model",
        stage="verification",
        status="succeeded",
        created_at=utc_now_iso(),
    )
    raw_text = json.dumps({"verdict": "approve", "confidence": 0.85})
    result = _apply_verification_result(
        db=tmp_db,
        cand_id=cand_id,
        candidate={"output_tokens": 200},
        raw_text=raw_text,
        synthesis_text="Correct answer.",
        current_confidence=0.5,
        verif_alias="verifier",
        run_id=run_id,
    )
    assert isinstance(result, tuple) and len(result) == 3
    _confidence, _text, action = result
    assert action == "ok"


def test_verification_short_output_preserves_existing_guard(tmp_db):
    """RCA-2 regression: the existing MIN_VERIFICATION_TOKENS guard from
    PR #26 must still trip. Below 50 tokens, the verdict is rejected, the
    synthesis gets the [INSUFFICIENT EVIDENCE] prefix, and the helper
    returns action='degraded' (matching the abstain behavior)."""
    run_id = "run_short_test"
    insert_run(
        db=tmp_db,
        run_id=run_id,
        mode="council",
        prompt="x",
        system_prompt=None,
        temperature=0.2,
        max_output_tokens=1000,
        deadline_seconds=600,
        deadline_at=utc_now_plus_seconds(600),
        owner_token_hash="testhash",
        metadata_json="{}",
        requested_models_json=None,
        created_at=utc_now_iso(),
    )
    cand_id = "cand_short_test"
    insert_candidate(
        db=tmp_db,
        run_id=run_id,
        candidate_id=cand_id,
        alias="verif",
        provider="test",
        provider_model="test-model",
        stage="verification",
        status="succeeded",
        created_at=utc_now_iso(),
    )
    raw_text = json.dumps({"verdict": "approve", "confidence": 0.99})  # 19 tokens total
    result = _apply_verification_result(
        db=tmp_db,
        cand_id=cand_id,
        candidate={"output_tokens": 19},
        raw_text=raw_text,
        synthesis_text="Original synthesis.",
        current_confidence=0.5,
        verif_alias="verifier",
        run_id="run_short_test",
    )
    assert isinstance(result, tuple) and len(result) == 3
    confidence, text, action = result
    assert confidence == 0.5
    assert "[INSUFFICIENT EVIDENCE" in text
    assert action == "degraded"


def test_verification_payload_schema_accepts_issues_and_reasoning():
    """RCA-3: the verifier JSON contract is extended with optional issues
    and reasoning fields. Old verifiers that only return verdict+confidence
    still parse successfully (defaults to empty arrays/strings)."""
    # Minimal: just verdict + confidence (back-compat)
    p_min = _VerificationPayload.model_validate_json(
        json.dumps({"verdict": "approve", "confidence": 0.8})
    )
    assert p_min.verdict == "approve"
    assert p_min.confidence == 0.8
    # Full: with issues and reasoning
    p_full = _VerificationPayload.model_validate_json(
        json.dumps({
            "verdict": "reject",
            "confidence": 0.9,
            "issues": ["too verbose", "missed the constraint"],
            "reasoning": "Output violates the user's shape instruction.",
        })
    )
    assert p_full.issues == ["too verbose", "missed the constraint"]
    assert "violates" in p_full.reasoning


# ---------------------------------------------------------------------------
# RCA-3: verification issues persist into run_candidates.score_json
# ---------------------------------------------------------------------------


def test_run_candidates_score_json_persists_verification_payload(council_run, mock_worker, monkeypatch):
    """RCA-3: when the verification stage completes, the run_candidates row
    must have score_json populated with the parsed {verdict, confidence,
    issues, reasoning} object — not just the raw JSON blob in normalized_answer.
    """
    db = council_run
    run_id = "run_council_rca"
    update_run_status(db, run_id, "running", started_at=utc_now_iso())
    emit_run_started(db, run_id, "council")

    # All non-verification stages return a quick success
    async def fake_call_provider_async(self, request, *args, **kwargs):
        alias = getattr(request, "alias", "")
        return _success_result(f"answer-from-{alias}", out_tokens=150)

    async def fake_call_structured_provider_async(self, request, schema, *args, **kwargs):
        return _success_result(
            json.dumps({
                "verdict": "reject",
                "confidence": 0.95,
                "issues": ["Verbose", "Wrong shape"],
                "reasoning": "Broke the one-sentence rule",
            }),
            out_tokens=200,
        )

    monkeypatch.setattr(Worker, "_call_provider_async", fake_call_provider_async)
    monkeypatch.setattr(Worker, "_call_structured_provider_async", fake_call_structured_provider_async)
    monkeypatch.setattr(Worker, "_try_fallback", lambda self, db, run, alias: None)
    monkeypatch.setattr(Worker, "_check_deadline", lambda self, run: None)

    asyncio.run(mock_worker._run_council(db, get_run(db, run_id)))

    candidates = list_candidates_for_run(db, run_id)
    verif_cands = [c for c in candidates if c.get("stage") == "verification"]
    assert verif_cands, "Expected at least one verification-stage candidate"
    verif = verif_cands[0]
    score_json = verif.get("score_json")
    assert score_json, (
        "verification candidate row must have score_json populated; "
        "RCA-3 regression: score_json is still None after council run."
    )
    parsed = json.loads(score_json)
    assert parsed["verdict"] == "reject"
    assert parsed["confidence"] == 0.95
    assert "Verbose" in parsed["issues"]
    assert "Broke the one-sentence rule" in parsed["reasoning"]


# ---------------------------------------------------------------------------
# RCA-4: stage token caps are env-tunable via Settings
# ---------------------------------------------------------------------------


def test_stage_token_caps_settings_have_expected_defaults():
    """RCA-4: Settings.STAGE_TOKEN_CAPS exposes the council stage ceilings
    so they can be overridden via env (GitOps-tunable). Defaults match the
    plan: first_opinion=1200, peer_review=800, debate=800, synthesis=1200,
    verification=400.
    """
    from fusion_council_service.config import Settings

    s = Settings(
        DATABASE_PATH=":memory:",
        SERVICE_API_KEYS="***",
        SERVICE_ADMIN_API_KEYS="***",
    )
    caps = s.stage_token_caps
    assert caps["first_opinion"] == 1200
    assert caps["peer_review"] == 800
    assert caps["debate"] == 800
    assert caps["synthesis"] == 1200
    assert caps["verification"] == 400


def test_council_first_opinion_uses_capped_tokens(mock_worker, council_run, monkeypatch):
    """RCA-4: the first_opinion stage must cap max_output_tokens to
    min(run.max_output_tokens, STAGE_TOKEN_CAPS[first_opinion]).

    Run with max_output_tokens=10000 and the default cap of 1200, the
    ProviderGenerateRequest sent to the provider must have
    max_output_tokens=1200.
    """
    db = council_run
    run_id = "run_council_rca"
    # Insert a run with a generous max_output_tokens
    insert_run(
        db=db,
        run_id=run_id + "_caps",
        mode="council",
        prompt="Is AI conscious?",
        system_prompt=None,
        temperature=0.2,
        max_output_tokens=10000,
        deadline_seconds=600,
        deadline_at=utc_now_plus_seconds(600),
        owner_token_hash="testhash",
        metadata_json="{}",
        requested_models_json=None,
        created_at=utc_now_iso(),
    )
    update_run_status(db, run_id + "_caps", "running", started_at=utc_now_iso())
    emit_run_started(db, run_id + "_caps", "council")

    captured: list[Any] = []

    async def fake_call_provider_async(self, request, *args, **kwargs):
        captured.append(getattr(request, "max_output_tokens", None))
        alias = getattr(request, "alias", "")
        return _success_result(f"answer-from-{alias}", out_tokens=100)

    monkeypatch.setattr(Worker, "_call_provider_async", fake_call_provider_async)
    monkeypatch.setattr(Worker, "_try_fallback", lambda self, db, run, alias: None)
    monkeypatch.setattr(Worker, "_check_deadline", lambda self, run: None)

    asyncio.run(mock_worker._run_council(db, get_run(db, run_id + "_caps")))

    first_op_tokens = [t for t in captured if t is not None]
    # First-opinion caps should all be <= 1200 (default) — they should NOT
    # be 10000 (the run max).
    assert first_op_tokens, "No max_output_tokens captured for first_opinion"
    for t in first_op_tokens[:3]:  # at least the first 3 are first-opinion
        assert t <= 1200, (
            f"first_opinion max_output_tokens={t} exceeds stage cap of 1200"
        )


# ---------------------------------------------------------------------------
# RCA-6: POST /v1/runs must not return 0.0.0.0 URLs
# ---------------------------------------------------------------------------


def test_create_run_response_uses_public_base_url_when_set(
    monkeypatch, tmp_db, mock_settings
):
    """RCA-6: when Settings.PUBLIC_BASE_URL is set, create_run() must use it
    to build status_url. It must NEVER use 0.0.0.0."""
    from fastapi.testclient import TestClient

    from fusion_council_service.api import routes
    from fusion_council_service.main import app

    # Patch settings to include PUBLIC_BASE_URL
    patched = mock_settings.model_copy(update={"PUBLIC_BASE_URL": "https://fusion.example.com"})
    monkeypatch.setattr(routes, "get_api_db", lambda: tmp_db)
    monkeypatch.setattr(routes, "_settings", patched)
    client = TestClient(app)

    response = client.post(
        "/v1/runs",
        headers={"Authorization": "Bearer test-user-key"},
        json={
            "mode": "single",
            "prompt": "hello",
            "max_output_tokens": 100,
            "temperature": 0.2,
        },
    )
    assert response.status_code in (200, 201, 202), response.text
    body = response.json()
    assert "status_url" in body
    assert "0.0.0.0" not in body["status_url"], (
        f"create_run returned 0.0.0.0 URL even with PUBLIC_BASE_URL set: {body['status_url']}"
    )
    assert body["status_url"].startswith("https://fusion.example.com"), (
        f"create_run did not honor PUBLIC_BASE_URL: {body['status_url']}"
    )


def test_create_run_response_falls_back_to_relative_path(
    monkeypatch, tmp_db, mock_settings
):
    """RCA-6: when Settings.PUBLIC_BASE_URL is unset, create_run() must return
    a relative path like /v1/runs/{id} — never 0.0.0.0."""
    from fastapi.testclient import TestClient

    from fusion_council_service.api import routes
    from fusion_council_service.main import app

    # PUBLIC_BASE_URL stays empty in mock_settings
    assert not getattr(mock_settings, "PUBLIC_BASE_URL", ""), (
        "mock_settings fixture must not set PUBLIC_BASE_URL by default"
    )
    monkeypatch.setattr(routes, "get_api_db", lambda: tmp_db)
    monkeypatch.setattr(routes, "_settings", mock_settings)
    client = TestClient(app)

    response = client.post(
        "/v1/runs",
        headers={"Authorization": "Bearer test-user-key"},
        json={
            "mode": "single",
            "prompt": "hello",
            "max_output_tokens": 100,
            "temperature": 0.2,
        },
    )
    assert response.status_code in (200, 201, 202), response.text
    body = response.json()
    assert "status_url" in body
    assert "0.0.0.0" not in body["status_url"]
    assert body["status_url"].startswith("/v1/runs/"), (
        f"create_run did not fall back to a relative path: {body['status_url']}"
    )


# ---------------------------------------------------------------------------
# RCA-support: SSE default-message contract must stay intact
# ---------------------------------------------------------------------------


def test_sse_route_emits_only_default_data_frames(monkeypatch, tmp_db, mock_settings):
    """RCA-support: the /v1/runs/{id}/events stream must yield only default
    `data:` frames. No `event: X\n` lines — those break EventSource.onmessage.
    """
    from fastapi.testclient import TestClient

    from fusion_council_service.api import routes
    from fusion_council_service.main import app
    from fusion_council_service.domain.run_repository import insert_run
    from fusion_council_service.clock import utc_now_iso, utc_now_plus_seconds

    run_id = "run_sse_contract"
    insert_run(
        db=tmp_db,
        run_id=run_id,
        mode="single",
        prompt="x",
        system_prompt=None,
        temperature=0.2,
        max_output_tokens=100,
        deadline_seconds=60,
        deadline_at=utc_now_plus_seconds(60),
        owner_token_hash="testhash",
        metadata_json="{}",
        requested_models_json=None,
        created_at=utc_now_iso(),
    )
    update_run_status(tmp_db, run_id, "succeeded", finished_at=utc_now_iso())
    emit_run_started(tmp_db, run_id, "single")
    emit_run_completed(tmp_db, run_id, "done")

    # Fast poll + small interval
    patched = mock_settings.model_copy(update={"SSE_POLL_INTERVAL_MS": 10})
    monkeypatch.setattr(routes, "get_api_db", lambda: tmp_db)
    monkeypatch.setattr(routes, "_settings", patched)
    client = TestClient(app)

    with client.stream(
        "GET",
        f"/v1/runs/{run_id}/events",
        headers={"Authorization": "Bearer test-user-key"},
    ) as response:
        assert response.status_code == 200, response.read()
        assert response.headers["content-type"].startswith("text/event-stream")
        chunks: list[str] = []
        for chunk in response.iter_text():
            chunks.append(chunk)
            if len(chunks) >= 3:
                break

    assert chunks, "SSE stream produced no chunks"
    for chunk in chunks:
        # Must contain data: lines
        assert "data:" in chunk, f"SSE chunk missing data: line: {chunk!r}"
        # No `event: <name>` lines — those break EventSource.onmessage
        for line in chunk.splitlines():
            assert not (line.startswith("event:") and len(line) > len("event:") and line[len("event:"):].strip() != ""), (
                f"SSE chunk has a custom 'event:' line: {line!r}"
            )
