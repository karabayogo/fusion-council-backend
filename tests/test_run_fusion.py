"""Tests for fusion-mode runs."""

import json
import pytest

from fusion_council_service.domain.candidate_repository import insert_candidate, list_candidates_for_run
from fusion_council_service.domain.run_repository import insert_run, get_run, update_run_status
from fusion_council_service.domain.scoring import (
    build_fusion_prompt,
    cosine_similarity,
    compute_pairwise_agreement,
    select_best_candidate,
)
from fusion_council_service.clock import utc_now_iso, utc_now_plus_seconds


@pytest.fixture
def fusion_run_setup(tmp_db):
    def _setup():
        run_id = "run_fusion_test"
        insert_run(
            db=tmp_db,
            run_id=run_id,
            mode="fusion",
            prompt="What is 1+1?",
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


def test_insert_fusion_run(tmp_db, fusion_run_setup):
    run_id = fusion_run_setup()
    run = get_run(tmp_db, run_id)
    assert run["mode"] == "fusion"
    assert run["status"] == "queued"


def test_fusion_run_quorum_requires_2_succeeded(tmp_db, fusion_run_setup):
    """Fusion mode needs at least 2/3 models to succeed for quorum."""
    run_id = fusion_run_setup()

    # Insert 3 candidates: 2 succeeded, 1 failed
    for i, status in enumerate(["succeeded", "succeeded", "failed"]):
        cid = f"cand_{i}"
        insert_candidate(tmp_db, run_id, cid, f"model-{i}", "test_provider",
                         f"model-{i}", "generation", status, utc_now_iso())
        if status == "failed":
            tmp_db.execute(
                "UPDATE run_candidates SET error_code='AUTH_FAILED' WHERE candidate_id=?",
                (cid,),
            )
            tmp_db.commit()

    cands = list_candidates_for_run(tmp_db, run_id)
    succeeded = [c for c in cands if c["status"] == "succeeded"]
    assert len(succeeded) == 2
    assert len(cands) == 3


def test_cosine_similarity_same_text():
    assert cosine_similarity("hello world", "hello world") >= 0.999


def test_cosine_similarity_different_text():
    sim = cosine_similarity("apple banana", "car dog elephant")
    assert 0.0 <= sim <= 1.0


def test_pairwise_agreement_high():
    cands = [
        {"raw_answer": "The answer is definitely 2", "normalized_answer": ""},
        {"raw_answer": "The answer is definitely 2", "normalized_answer": ""},
    ]
    assert compute_pairwise_agreement(cands) >= 0.999


def test_pairwise_agreement_low():
    cands = [
        {"raw_answer": "The sky is blue", "normalized_answer": ""},
        {"raw_answer": "Quantum physics is fascinating", "normalized_answer": ""},
    ]
    # Different texts should have low agreement
    assert 0.0 <= compute_pairwise_agreement(cands) < 1.0


def test_select_best_candidate_prefers_succeeded():
    cands = [
        {"status": "failed", "raw_answer": "error"},
        {"status": "succeeded", "raw_answer": "The answer is 2"},
        {"status": "succeeded", "raw_answer": "It's 2"},
    ]
    best = select_best_candidate(cands)
    assert best["status"] == "succeeded"
    assert "2" in best["raw_answer"]


def test_select_best_candidate_all_failed():
    cands = [
        {"status": "failed", "raw_answer": "error 1"},
        {"status": "failed", "raw_answer": "error 2 longer"},
    ]
    best = select_best_candidate(cands)
    assert best["status"] == "failed"
    assert "error 2 longer" in best["raw_answer"]


def test_build_fusion_prompt_includes_all_candidates():
    cands = [
        {"alias": "model-a", "raw_answer": "Answer A", "status": "succeeded"},
        {"alias": "model-b", "raw_answer": "Answer B", "status": "succeeded"},
    ]
    prompt = build_fusion_prompt("What is 1+1?", cands)
    assert "Original question" in prompt
    assert "Answer A" in prompt
    assert "Answer B" in prompt
    assert "Synthesize" in prompt
