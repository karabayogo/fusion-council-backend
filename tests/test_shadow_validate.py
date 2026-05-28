"""Tests for shadow parity gate logic."""

from fusion_council_service.scripts.shadow_validate import (
    _compute_metrics_from_rows,
    evaluate_gate,
)


def test_compute_metrics_from_rows_counts_parity_and_errors():
    rows = [
        {"engine": "langgraph", "final_status": "succeeded", "final_answer_present": True, "stage_order_match": True, "error_codes": []},
        {"engine": "langgraph", "final_status": "failed", "final_answer_present": True, "stage_order_match": False, "error_codes": ["E1"]},
        {"engine": "legacy", "final_status": "succeeded", "final_answer_present": False, "stage_order_match": True, "error_codes": []},
    ]
    metrics = _compute_metrics_from_rows(rows, lookback_hours=24)
    assert metrics["total_runs"] == 3
    assert metrics["stage_parity_rate"] == 2 / 3
    assert metrics["answer_presence_rate"] == 2 / 3
    assert metrics["error_rate"] == 1 / 3
    assert metrics["terminal_corruption_count"] == 1
    assert metrics["runs_by_engine"] == {"langgraph": 2, "legacy": 1}


def test_evaluate_gate_passes_when_thresholds_met():
    metrics = {
        "total_runs": 100,
        "stage_parity_rate": 0.99,
        "terminal_corruption_count": 0,
    }
    decision = evaluate_gate(metrics)
    assert decision.overall == "PASS"
    assert decision.failures == []


def test_evaluate_gate_fails_for_low_parity_and_corruption():
    metrics = {
        "total_runs": 10,
        "stage_parity_rate": 0.80,
        "terminal_corruption_count": 2,
    }
    decision = evaluate_gate(metrics)
    assert decision.overall == "FAIL"
    assert len(decision.failures) == 3

