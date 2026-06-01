"""Tests for council-mode runs."""

import json
from unittest.mock import MagicMock, patch

import pytest

from fusion_council_service.domain.candidate_repository import insert_candidate, list_candidates_for_run
from fusion_council_service.domain.run_repository import insert_run, get_run
from fusion_council_service.domain.scoring import (
    build_council_synthesis_prompt,
    build_peer_review_prompt,
    build_debate_prompt,
    build_verification_prompt,
    compute_pairwise_agreement,
)
from fusion_council_service.clock import utc_now_iso, utc_now_plus_seconds


@pytest.fixture
def council_run_setup(tmp_db):
    def _setup():
        run_id = "run_council_test"
        insert_run(
            db=tmp_db,
            run_id=run_id,
            mode="council",
            prompt="Is AI consciousness possible?",
            system_prompt=None,
            temperature=0.2,
            max_output_tokens=3000,
            deadline_seconds=120,
            deadline_at=utc_now_plus_seconds(120),
            owner_token_hash="testhash",
            metadata_json=json.dumps({}),
            requested_models_json=None,
            created_at=utc_now_iso(),
        )
        return run_id
    return _setup


def test_insert_council_run(tmp_db, council_run_setup):
    run_id = council_run_setup()
    run = get_run(tmp_db, run_id)
    assert run["mode"] == "council"
    assert run["status"] == "queued"


def test_council_quorum_not_met_with_only_1_success(tmp_db, council_run_setup):
    run_id = council_run_setup()
    for i, status in enumerate(["succeeded", "failed", "failed"]):
        cid = f"cand_{i}"
        insert_candidate(tmp_db, run_id, cid, f"model-{i}", "test_provider",
                         f"model-{i}", "first_opinion", status, utc_now_iso())

    cands = list_candidates_for_run(tmp_db, run_id)
    succeeded = [c for c in cands if c["status"] == "succeeded"]
    assert len(succeeded) == 1
    assert len(cands) == 3


def test_build_council_synthesis_prompt():
    opinions = [
        {"alias": "model-a", "normalized_answer": "Yes, consciousness emerges."},
        {"alias": "model-b", "normalized_answer": "No, not in current AI."},
    ]
    reviews = [
        {"alias": "reviewer-1", "normalized_answer": "Good analysis."},
    ]
    prompt = build_council_synthesis_prompt("Is AI consciousness possible?", opinions, reviews)
    assert "Original question" in prompt
    assert "model-a" in prompt
    assert "model-b" in prompt
    assert "Council Chair" in prompt


def test_build_peer_review_prompt():
    prompt = build_peer_review_prompt(
        "Is AI consciousness possible?",
        "Yes, consciousness emerges.",
        "model-a",
    )
    assert "Original question" in prompt
    assert "Yes, consciousness emerges" in prompt
    assert "model-a" in prompt
    assert "peer reviewer" in prompt


def test_build_debate_prompt():
    opinions = [
        {"alias": "model-a", "normalized_answer": "Yes"},
        {"alias": "model-b", "normalized_answer": "No"},
    ]
    prompt = build_debate_prompt("Is AI consciousness possible?", opinions)
    assert "Original question" in prompt
    assert "conflicting" in prompt
    assert "Yes" in prompt
    assert "No" in prompt


def test_build_verification_prompt():
    prompt = build_verification_prompt("Is AI consciousness possible?", "Yes, it is.")
    assert "Original question" in prompt
    assert "Yes, it is" in prompt
    assert "verification agent" in prompt
    assert "verdict" in prompt


def test_debate_triggers_on_low_agreement():
    cands = [
        {"normalized_answer": "Yes definitely"},
        {"normalized_answer": "No absolutely not"},
    ]
    agreement = compute_pairwise_agreement(cands)
    assert agreement < 0.55  # Should trigger debate


@pytest.mark.asyncio
async def test_council_later_stages_avoid_failed_upstream_provider_model(tmp_db, model_catalog):
    """A failed first-opinion upstream identity must not be reused downstream.

    Regression for run_1452caf6037a446f9602ae01: the primary opencode/deepseek
    first opinion failed, then peer_review and debate retried the same upstream
    provider/model pair. Later council stages should prefer healthy role-specific
    models and treat upstream provider/model identity, not just alias, as failed.
    """
    from fusion_council_service.domain.worker_loop import Worker
    from fusion_council_service.domain.types import ProviderGenerateResult

    run_id = "run_failed_primary_not_reused_downstream"
    insert_run(
        db=tmp_db,
        run_id=run_id,
        mode="council",
        prompt="Design a robust retirement travel plan.",
        system_prompt=None,
        temperature=0.2,
        max_output_tokens=3000,
        deadline_seconds=1800,
        deadline_at=utc_now_plus_seconds(1800),
        owner_token_hash="testhash",
        metadata_json=json.dumps({}),
        requested_models_json=None,
        created_at=utc_now_iso(),
    )

    worker = Worker(
        db_path=":memory:",
        registry=MagicMock(),
        catalog=model_catalog,
        poll_interval_ms=50,
        heartbeat_interval_ms=5000,
        stale_run_threshold_seconds=30,
    )
    worker._db = tmp_db
    run = get_run(tmp_db, run_id)
    assert run is not None

    async def fake_provider(request, db, active_run_id, timeout_seconds=300):
        if request.alias == "primary-researcher":
            return ProviderGenerateResult(
                success=False,
                raw_text=None,
                error_code="PROVIDER_ERROR",
                error_message="simulated upstream 429",
                latency_ms=10,
                input_tokens=None,
                output_tokens=None,
            )
        if request.alias == "verifier":
            text = json.dumps({"verdict": "pass", "confidence": 0.8})
        else:
            text = f"healthy response from {request.alias}"
        return ProviderGenerateResult(
            success=True,
            raw_text=text,
            error_code=None,
            error_message=None,
            latency_ms=10,
            input_tokens=5,
            output_tokens=7,
        )

    with patch.object(worker, "_call_provider_async", side_effect=fake_provider):
        await worker._run_council(tmp_db, run)

    candidates = list_candidates_for_run(tmp_db, run_id)
    first_failed = [
        c for c in candidates
        if c["stage"] == "first_opinion" and c["alias"] == "primary-researcher"
    ]
    assert first_failed and first_failed[0]["status"] == "failed"

    downstream = [c for c in candidates if c["stage"] != "first_opinion"]
    assert downstream, "expected peer/synthesis/verification candidates"
    assert all(c["alias"] != "primary-researcher" for c in downstream)
    assert all(
        (c["provider"], c["provider_model"]) != ("opencode_go", "qwen3.7-max")
        for c in downstream
    )

    run_after = get_run(tmp_db, run_id)
    assert run_after["status"] == "succeeded"
    assert run_after["current_stage"] == "completed"
