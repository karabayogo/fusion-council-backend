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


# ---------------------------------------------------------------------------
# E3 fix — shadow parity gate must fail on NO_DATA, not silently PASS
# ---------------------------------------------------------------------------

def test_compute_metrics_from_rows_no_rows_returns_no_data_marker():
    """E3 fix: when 0 rows are written, metrics must carry error='NO_DATA'
    so evaluate_gate() can distinguish 'no signal' from 'parity verified'.
    Run c908a00b1c834b8eb9ebe2b4 (2026-06-01) had 0 shadow_diff rows and the
    gate silently PASSed, masking that parity was never observed.
    """
    metrics = _compute_metrics_from_rows([], lookback_hours=168)
    assert metrics["total_runs"] == 0
    assert metrics["error"] == "NO_DATA"


def test_evaluate_gate_fails_on_no_data():
    """E3 fix: the gate must FAIL when 0 rows in lookback. The old code
    silently PASSed, which let the langgraph engine run without writing
    run_shadow_diff for an entire run cycle with no signal to anyone.
    """
    metrics = _compute_metrics_from_rows([], lookback_hours=168)
    decision = evaluate_gate(metrics)
    assert decision.overall == "FAIL", (
        f"E3 bug: gate should FAIL on NO_DATA, got {decision.overall}. "
        f"Failures: {decision.failures}"
    )
    assert any("NO_DATA" in f for f in decision.failures), (
        f"failure list should mention NO_DATA: {decision.failures}"
    )

