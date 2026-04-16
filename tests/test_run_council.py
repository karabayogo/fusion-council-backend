"""Tests for council-mode runs."""

import json
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
        {"alias": "model-a", "raw_answer": "Yes, consciousness emerges."},
        {"alias": "model-b", "raw_answer": "No, not in current AI."},
    ]
    reviews = [
        {"alias": "reviewer-1", "raw_answer": "Good analysis."},
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
        {"alias": "model-a", "raw_answer": "Yes"},
        {"alias": "model-b", "raw_answer": "No"},
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
        {"raw_answer": "Yes definitely", "normalized_answer": ""},
        {"raw_answer": "No absolutely not", "normalized_answer": ""},
    ]
    agreement = compute_pairwise_agreement(cands)
    assert agreement < 0.55  # Should trigger debate
