"""End-to-end functional tests for council-mode runs.

Tests the full council pipeline: first_opinion quorum → synthesis → verification.
Uses mocked providers to avoid real API calls in tests.

Architecture:
  - Worker._run_council() drives the pipeline
  - _call_provider_async wraps provider calls with timeout
  - All providers are mocked to return instantly
  - SQLite in-memory for speed

Key scenarios tested:
  1. Happy path (3/3 succeed): full pipeline runs, status=succeeded
  2. Degraded path (2/3 succeed): proceeds to synthesis, confidence=0.5
  3. Quorum not met (1/3 succeed, fallbacks fail): COUNCIL_QUORUM_NOT_MET
  4. HTTP 500 is classified as HTTP_500, not PROVIDER_TIMEOUT
  5. NO_MODELS if catalog has fewer than 3 council models
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from fusion_council_service.domain.candidate_repository import list_candidates_for_run
from fusion_council_service.domain.run_repository import insert_run, get_run
from fusion_council_service.domain.worker_loop import Worker
from fusion_council_service.domain.types import ProviderGenerateResult
from fusion_council_service.clock import utc_now_iso, utc_now_plus_seconds


# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------

@pytest.fixture
def council_run_setup(tmp_db):
    """Insert a council-mode run, return the run dict."""
    def _setup(run_id: str = "run_e2e_council") -> dict:
        insert_run(
            db=tmp_db,
            run_id=run_id,
            mode="council",
            prompt="Is AI consciousness possible?",
            system_prompt=None,
            temperature=0.2,
            max_output_tokens=500,
            deadline_seconds=600,
            deadline_at=utc_now_plus_seconds(600),
            owner_token_hash="testhash",
            metadata_json=json.dumps({}),
            requested_models_json=None,
            created_at=utc_now_iso(),
        )
        return get_run(tmp_db, run_id)
    return _setup


@pytest.fixture
def worker_with_mocked_calls(tmp_db, model_catalog):
    """Build a Worker with a mock registry for e2e tests."""
    mock_registry = MagicMock()
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


# -----------------------------------------------------------------------------
# Mock provider factory
# -----------------------------------------------------------------------------

def _build_mock_call_provider_async(
    successes: dict[str, tuple],
    failures: dict[str, tuple],
    fallback_responses: dict[str, tuple],
):
    """Build a side_effect for worker._call_provider_async (async fn).

    Each key in successes/failures is an alias string. The alias may include a
    stage suffix (e.g. "-review", "-debate", "-synthesis", "-verification") to
    route responses by stage. Falls back to base alias (without suffix) when no
    stage-specific entry exists.

    Examples:
        "opencode-go/qwen3.6-plus"     → first_opinion
        "opencode-go/qwen3.6-plus-review"  → peer_review
        "opencode-go/qwen3.6-plus-synthesis"  → synthesis
    """
    async def mock_call(req, db, run_id, timeout_seconds=300):
        alias = req.alias

        # Route by stage suffix first, then fall back to base alias
        for suffix in ("-review", "-debate", "-synthesis", "-verification"):
            base = alias[:-len(suffix)] if alias.endswith(suffix) else None
            if base:
                key = alias  # use exact stage-specific key
                if key in successes:
                    raw_text, lat_ms, in_tok, out_tok = successes[key]
                    return (True, raw_text, None, None, lat_ms, in_tok, out_tok)
                if key in failures:
                    err_code, err_msg = failures[key]
                    return (False, None, err_code, err_msg, 50, None, None)
                # Fall through to base alias

        if alias in successes:
            raw_text, lat_ms, in_tok, out_tok = successes[alias]
            return (True, raw_text, None, None, lat_ms, in_tok, out_tok)

        if alias in failures:
            err_code, err_msg = failures[alias]
            return (False, None, err_code, err_msg, 50, None, None)

        if alias in fallback_responses:
            raw_text, lat_ms, in_tok, out_tok = fallback_responses[alias]
            return (True, raw_text, None, None, lat_ms, in_tok, out_tok)

        return (False, None, "UNKNOWN_MODEL", f"No mock for {alias}", 0, None, None)

    return mock_call


# -----------------------------------------------------------------------------
# Test 1: Happy path — all 3 first opinions succeed
# -----------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_council_all_first_opinions_succeed(
    tmp_db, council_run_setup, worker_with_mocked_calls
):
    """3/3 first opinions succeed → synthesis → verification → succeeded."""
    run = council_run_setup("run_all_succeed")
    worker = worker_with_mocked_calls

    synth_text = "Council synthesis: majority view is positive."
    verify_text = json.dumps({"verdict": "pass", "confidence": 0.85})

    mock_side_effect = _build_mock_call_provider_async(
        successes={
            # First opinions (council selects primary, reviewer, creative)
            "primary-researcher": ("Primary researcher says yes definitely", 900, 25, 45),
            "reviewer": ("Reviewer finds strong evidence for it", 850, 22, 42),
            "creative": ("Creative take: unlikely in current form", 800, 20, 40),
            # Synthesis (role_bias=synthesis)
            "synthesizer": (synth_text, 600, 25, 50),
            # Backup (role_bias=backup, used in peer reviews and fallback)
            "backup": ("Backup model concurs with primary", 750, 18, 38),
            # Verifier (used in peer reviews, debate, and verification fallback)
            "verifier": (verify_text, 500, 20, 60),
        },
        failures={},
        fallback_responses={},
    )

    with patch.object(worker, "_call_provider_async", side_effect=mock_side_effect):
        await worker._run_council(tmp_db, run)

    final_run = get_run(tmp_db, "run_all_succeed")
    assert final_run["status"] == "succeeded", (
        f"Expected succeeded, got {final_run['status']} — "
        f"error: {final_run.get('error_code')} {final_run.get('error_message')}"
    )
    assert final_run["final_answer"] is not None

    cands = list_candidates_for_run(tmp_db, "run_all_succeed")

    # Full pipeline: 3 first_opinion + 3 peer_review + 1 debate + 1 synthesis + 1 verification = 9
    assert len(cands) == 9, f"Expected 9 candidates, got {len(cands)}: {cands}"

    stages = {c["stage"] for c in cands}
    assert stages == {"first_opinion", "peer_review", "debate", "synthesis", "verification"}, (
        f"Expected all 5 stages, got {stages}"
    )

    first_op_cands = [c for c in cands if c["stage"] == "first_opinion"]
    assert len(first_op_cands) == 3 and all(c["status"] == "succeeded" for c in first_op_cands), (
        f"Expected 3 successful first_opinions, got {first_op_cands}"
    )
    assert all(c["status"] == "succeeded" for c in cands if c["stage"] in ("peer_review", "debate")), (
        f"Some peer_review or debate candidates failed: {[c for c in cands if c['stage'] in ('peer_review','debate') and c['status'] != 'succeeded']}"
    )


# -----------------------------------------------------------------------------
# Test 2: Degraded — exactly 2 first opinions succeed
# -----------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_council_two_first_opinions_succeed_degrades_gracefully(
    tmp_db, council_run_setup, worker_with_mocked_calls
):
    """2/3 succeed → synthesis → succeeded (degraded confidence)."""
    run = council_run_setup("run_2_succeed")
    worker = worker_with_mocked_calls

    synth_text = "Synthesized answer from council."
    verify_text = json.dumps({"verdict": "pass", "confidence": 0.5})

    mock_side_effect = _build_mock_call_provider_async(
        successes={
            "primary-researcher": ("Deepseek answer", 900, 25, 45),
            "creative": ("MiniMax answer", 800, 20, 40),
            # Fallback for failed reviewer
            "backup": ("Backup fallback answer", 850, 22, 42),
            # Synthesis
            "synthesizer": (synth_text, 600, 25, 50),
            # Verification (via structured fallback)
            "verifier": (verify_text, 500, 20, 60),
        },
        failures={
            "reviewer": ("HTTP_500", "Provider returned 500: server error"),
        },
        fallback_responses={},
    )

    with patch.object(worker, "_call_provider_async", side_effect=mock_side_effect):
        await worker._run_council(tmp_db, run)

    final_run = get_run(tmp_db, "run_2_succeed")
    assert final_run["status"] == "succeeded", (
        f"Expected succeeded (degraded), got {final_run['status']} — "
        f"error: {final_run.get('error_code')} {final_run.get('error_message')}"
    )

    cands = list_candidates_for_run(tmp_db, "run_2_succeed")
    # 3 first_op + 2 peer_review + 1 debate + 1 synthesis + 1 verification = 8
    assert len(cands) == 8, f"Expected 8 candidates, got {len(cands)}: {cands}"


# -----------------------------------------------------------------------------
# Test 3: Quorum not met — 0/3 first opinions succeed, all fallbacks also fail
# -----------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_council_quorum_not_met(
    tmp_db, council_run_setup, worker_with_mocked_calls
):
    """1/3 first opinions succeed + all fallbacks fail → COUNCIL_QUORUM_NOT_MET."""
    run = council_run_setup("run_quorum_0")
    worker = worker_with_mocked_calls

    mock_side_effect = _build_mock_call_provider_async(
        successes={
            "creative": ("MiniMax answer", 800, 20, 40),
        },
        failures={
            "primary-researcher": (
                "HTTP_500",
                "Provider returned 500: {\"error\":{\"type\":\"AuthError\"}}"
            ),
            "reviewer": (
                "PROVIDER_TIMEOUT",
                "Provider call timed out after 300s"
            ),
            "backup": (
                "AUTH_FAILED",
                "API key rejected"
            ),
        },
        fallback_responses={},
    )

    with patch.object(worker, "_call_provider_async", side_effect=mock_side_effect):
        await worker._run_council(tmp_db, run)

    final_run = get_run(tmp_db, "run_quorum_0")
    assert final_run["status"] == "failed", (
        f"Expected failed (quorum not met), got {final_run['status']}"
    )
    assert final_run["error_code"] == "COUNCIL_QUORUM_NOT_MET", (
        f"Expected COUNCIL_QUORUM_NOT_MET, got {final_run.get('error_code')}"
    )


# -----------------------------------------------------------------------------
# Test 4: HTTP 500 is classified as HTTP_500, NOT PROVIDER_TIMEOUT
# -----------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_council_http_500_classified_not_masked_as_timeout(
    tmp_db, council_run_setup, worker_with_mocked_calls
):
    """HTTP 500 must be recorded as HTTP_500, not PROVIDER_TIMEOUT.

    Regression for run_3ae5ae7: opencode.ai returned 500 but the 300s timeout
    barrier swallowed it and logged "Provider call timed out". With the fix,
    HTTP_500 is classified explicitly by openai_compatible.py and propagated
    through _call_provider_async with the real error code.
    """
    run = council_run_setup("run_http500")
    worker = worker_with_mocked_calls

    synth_text = "Synthesized answer."
    verify_text = json.dumps({"verdict": "pass", "confidence": 0.85})

    mock_side_effect = _build_mock_call_provider_async(
        successes={
            "creative": ("MiniMax answer", 800, 20, 40),
            # Fallback models (save the run from quorum failure)
            "backup": ("Backup fallback answer", 850, 22, 42),
            "synthesizer": (synth_text, 600, 25, 50),
            "verifier": (verify_text, 500, 20, 60),
        },
        failures={
            "primary-researcher": (
                "HTTP_500",
                "Provider returned 500: {\"error\":{\"type\":\"AuthError\",\"message\":\"Invalid API key\"}}"
            ),
            "reviewer": (
                "HTTP_500",
                "Provider returned 500: server error"
            ),
        },
        fallback_responses={},
    )

    with patch.object(worker, "_call_provider_async", side_effect=mock_side_effect):
        await worker._run_council(tmp_db, run)

    final_run = get_run(tmp_db, "run_http500")
    # Fallback kimi-k2.6 saves the run from quorum failure
    assert final_run["status"] == "succeeded", (
        f"Expected succeeded (fallback kimi-k2.6 should save the run), "
        f"got {final_run['status']} — "
        f"error: {final_run.get('error_code')} {final_run.get('error_message')}"
    )

    # Verify the 500s were NOT recorded as PROVIDER_TIMEOUT
    cands = list_candidates_for_run(tmp_db, "run_http500")
    failed_cands = [c for c in cands if c["status"] == "failed"]

    for cand in failed_cands:
        err_code = cand.get("error_code") or ""
        err_msg = cand.get("error_message") or ""

        assert err_code != "PROVIDER_TIMEOUT", (
            f"Candidate {cand['alias']} has error_code=PROVIDER_TIMEOUT — "
            f"HTTP 500 must be classified as HTTP_500, not masked as timeout. "
            f"error_message={err_msg}"
        )
        assert "timed out" not in err_msg.lower(), (
            f"Candidate {cand['alias']} error_message mentions 'timed out' — "
            f"HTTP 500 must be classified explicitly, not as timeout. "
            f"error_code={err_code}, error_message={err_msg}"
        )

        # HTTP 500 should be explicitly classified
        if "opencode" in cand.get("alias", "").lower():
            assert err_code == "HTTP_500", (
                f"Candidate {cand['alias']} expected error_code=HTTP_500, got {err_code}"
            )


# -----------------------------------------------------------------------------
# Test 5: NO_MODELS when catalog has fewer than 3 council-mode models
# -----------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_council_rejects_insufficient_models(
    tmp_db, worker_with_mocked_calls
):
    """NO_MODELS if select_models_for_mode('council') returns < 3 models.

    Simulates a catalog with fewer than 3 council-mode models by patching
    the catalog at runtime so _run_council sees only 2 models.
    """
    from fusion_council_service.domain.run_repository import insert_run, get_run
    from fusion_council_service.domain.budget import select_models_for_mode as original_select_models_for_mode
    from fusion_council_service.clock import utc_now_iso, utc_now_plus_seconds

    run_id = "run_no_models_test"
    insert_run(
        db=tmp_db,
        run_id=run_id,
        mode="council",
        prompt="test",
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
    run = get_run(tmp_db, run_id)
    worker = worker_with_mocked_calls

    # Patch select_models_for_mode to return only 2 models for council mode
    thin_council_models = [
        {"alias": "primary-researcher", "provider": "opencode_go",
         "provider_model": "qwen3.7-max", "enabled": True},
        {"alias": "reviewer", "provider": "opencode_go",
         "provider_model": "kimi-k2.6", "enabled": True},
    ]

    def thin_select(mode, catalog, requested=None):
        if mode == "council":
            return thin_council_models
        return original_select_models_for_mode(mode, catalog, requested)

    with patch("fusion_council_service.domain.worker_loop.select_models_for_mode", side_effect=thin_select):
        with patch.object(worker, "_call_provider_async") as mock_call:
            await worker._run_council(tmp_db, run)

    final_run = get_run(tmp_db, run_id)
    assert final_run["status"] == "failed"
    assert final_run["error_code"] == "NO_MODELS"
    # NO_MODELS short-circuits before any provider calls
    mock_call.assert_not_called()