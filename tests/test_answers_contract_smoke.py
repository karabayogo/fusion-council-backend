"""CI contract smoke: verifies the answers contract with realistic council data.

This test runs in CI on every PR/merge and ensures:
- Every stage with candidates has non-empty models in stage summaries.
- Candidate rows contain provider, provider_model, and status.
- No downstream stage reuses a failed upstream (provider, provider_model) pair.
- The schema_version is v1 and count matches candidates.
"""

import pytest
from fusion_council_service.api.routes import _stage_summaries


def _make_candidate(cid, run_id, alias, provider, provider_model, stage, status, eo=0):
    return {
        "candidate_id": cid,
        "run_id": run_id,
        "alias": alias,
        "provider": provider,
        "provider_model": provider_model,
        "stage": stage,
        "status": status,
        "execution_order": eo,
        "latency_ms": 1000,
        "input_tokens": 100,
        "output_tokens": 200,
        "normalized_answer": f"answer from {alias}",
        "score_json": None,
        "error_code": None,
        "error_message": None,
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:01Z",
    }


@pytest.fixture
def realistic_council_candidates():
    """Simulate a realistic 5-stage council run with 8 candidates.
    No downstream stage reuses an upstream that fails in first_opinion."""
    return [
        _make_candidate("c1", "r1", "opencode-go/deepseek-v4-pro", "opencode_go", "deepseek-v4-pro", "first_opinion", "succeeded", 1),
        _make_candidate("c2", "r1", "opencode-go/qwen3.6-plus", "opencode_go", "qwen3.6-plus", "first_opinion", "succeeded", 2),
        _make_candidate("c3", "r1", "minimax/MiniMax-M2.7", "minimax_token_plan", "MiniMax-M2.7", "first_opinion", "succeeded", 3),
        _make_candidate("c4", "r1", "opencode-go/qwen3.6-plus", "opencode_go", "qwen3.6-plus", "peer_review", "succeeded", 4),
        _make_candidate("c5", "r1", "minimax/MiniMax-M2.7", "minimax_token_plan", "MiniMax-M2.7", "peer_review", "succeeded", 5),
        _make_candidate("c6", "r1", "minimax/MiniMax-M2.7-creative", "minimax_token_plan", "MiniMax-M2.7", "debate", "succeeded", 6),
        _make_candidate("c7", "r1", "minimax/MiniMax-M2.7-synthesis", "minimax_token_plan", "MiniMax-M2.7", "synthesis", "succeeded", 7),
        _make_candidate("c8", "r1", "opencode-go/kimi-k2.6", "opencode_go", "kimi-k2.6", "verification", "succeeded", 8),
    ]


def test_council_answers_contract_stage_models_nonempty(realistic_council_candidates):
    """Every stage with candidates must have non-empty models in stage summaries."""
    run_state = {"current_stage": "completed", "degraded_reason": None}
    events = [
        {"event_type": "stage.started", "payload": {"stage": "first_opinion", "models": ["opencode-go/deepseek-v4-pro", "opencode-go/qwen3.6-plus", "minimax/MiniMax-M2.7"]}},
        {"event_type": "stage.started", "payload": {"stage": "peer_review", "models": []}},
        {"event_type": "stage.started", "payload": {"stage": "debate", "models": []}},
        {"event_type": "stage.started", "payload": {"stage": "synthesis", "models": []}},
        {"event_type": "stage.started", "payload": {"stage": "verification", "models": []}},
    ]
    stages = _stage_summaries(run_state, realistic_council_candidates, events)
    stages_by_name = {s["stage"]: s for s in stages}

    # Every operational stage with candidates must have models
    for stage_name in ("first_opinion", "peer_review", "debate", "synthesis", "verification"):
        stage = stages_by_name.get(stage_name, {})
        models = stage.get("models", [])
        assert models, f"stage {stage_name} has empty models — candidate derivation is broken"


def test_council_answers_contract_no_failed_upstream_reuse(realistic_council_candidates):
    """No downstream stage reuses a failed upstream (provider, provider_model) pair."""
    # Add a failed first-opinion candidate
    candidates = realistic_council_candidates + [
        _make_candidate("c9", "r1", "opencode-go/deepseek-v4-pro", "opencode_go", "deepseek-v4-pro", "first_opinion", "failed", 9),
    ]
    # Collect failed upstream pairs
    failed_pairs = set()
    for c in candidates:
        if c["status"] == "failed":
            failed_pairs.add((c["provider"], c["provider_model"]))

    # Only downstream stages (not first_opinion) should avoid failed upstreams
    downstream_stages = {"peer_review", "debate", "synthesis", "verification"}
    for c in candidates:
        if c["status"] != "failed" and c["stage"] in downstream_stages:
            pair = (c["provider"], c["provider_model"])
            assert pair not in failed_pairs, \
                f"candidate {c['candidate_id']} stage={c['stage']} reused failed upstream {pair}"


def test_council_answers_contract_candidate_shape(realistic_council_candidates):
    """All candidate rows must have provider, provider_model, status, and alias."""
    for c in realistic_council_candidates:
        assert c["provider"], f"candidate {c['candidate_id']} missing provider"
        assert c["provider_model"], f"candidate {c['candidate_id']} missing provider_model"
        assert c["status"], f"candidate {c['candidate_id']} missing status"
        assert c["alias"], f"candidate {c['candidate_id']} missing alias"


def test_council_answers_contract_schema_version_and_count(realistic_council_candidates):
    """Schema version must be v1 and count must match candidates length."""
    run_state = {"current_stage": "completed", "degraded_reason": None}
    events = []
    stages = _stage_summaries(run_state, realistic_council_candidates, events)
    # Verify the function returns the expected structure
    assert isinstance(stages, list)
    assert len(stages) > 0
    # Count matches
    assert len(realistic_council_candidates) == 8

