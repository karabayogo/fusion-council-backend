from fusion_council_service import metrics
from fusion_council_service.clock import utc_now_iso
from fusion_council_service.domain.candidate_repository import (
    count_candidates_for_run,
    insert_candidate,
    update_candidate_result,
)
from fusion_council_service.domain.run_repository import insert_run, record_terminal_run_metrics


def _insert_run(tmp_db, run_id="run_metrics", mode="council"):
    now = utc_now_iso()
    return insert_run(
        tmp_db,
        run_id=run_id,
        mode=mode,
        prompt="prompt",
        system_prompt=None,
        temperature=0.2,
        max_output_tokens=1000,
        deadline_seconds=60,
        deadline_at=now,
        owner_token_hash="owner",
        metadata_json="{}",
        requested_models_json="[]",
        created_at=now,
    )


def test_count_candidates_for_run_counts_all_statuses(tmp_db):
    _insert_run(tmp_db)
    assert count_candidates_for_run(tmp_db, "run_metrics") == 0

    insert_candidate(tmp_db, "run_metrics", "cand_a", "alias-a", "provider", "model", "first_opinion", "succeeded", utc_now_iso())
    insert_candidate(tmp_db, "run_metrics", "cand_b", "alias-b", "provider", "model", "peer_review", "failed", utc_now_iso())

    assert count_candidates_for_run(tmp_db, "run_metrics") == 2


def test_update_candidate_result_records_status_and_stage_latency_metrics(tmp_db):
    metrics.reset_metrics()
    _insert_run(tmp_db)
    insert_candidate(tmp_db, "run_metrics", "cand_metric", "alias", "provider", "model", "first_opinion", "running", utc_now_iso())

    update_candidate_result(tmp_db, "cand_metric", "succeeded", raw_answer="ok", latency_ms=1250)

    rendered = metrics.render_prometheus()
    assert 'council_candidate_status_total{status="succeeded"} 1' in rendered
    assert 'council_stage_duration_seconds_count{stage="first_opinion"} 1' in rendered
    assert 'council_stage_duration_seconds_sum{stage="first_opinion"} 1.25' in rendered


def test_terminal_council_run_records_candidate_count_distribution(tmp_db):
    metrics.reset_metrics()
    _insert_run(tmp_db)
    insert_candidate(tmp_db, "run_metrics", "cand_one", "alias", "provider", "model", "first_opinion", "succeeded", utc_now_iso())

    record_terminal_run_metrics(tmp_db, "run_metrics")

    rendered = metrics.render_prometheus()
    assert "council_answers_candidate_count_count 1" in rendered
    assert "council_answers_candidate_count_sum 1.0" in rendered
    assert "council_runs_terminal_without_candidates_total 0" in rendered


def test_terminal_run_without_candidates_records_regression_counter(tmp_db):
    metrics.reset_metrics()
    _insert_run(tmp_db, run_id="run_empty", mode="council")

    record_terminal_run_metrics(tmp_db, "run_empty")

    rendered = metrics.render_prometheus()
    assert "council_answers_candidate_count_count 1" in rendered
    assert "council_answers_candidate_count_sum 0.0" in rendered
    assert "council_runs_terminal_without_candidates_total 1" in rendered


def test_terminal_run_metrics_are_idempotent(tmp_db):
    metrics.reset_metrics()
    _insert_run(tmp_db, run_id="run_empty", mode="council")

    record_terminal_run_metrics(tmp_db, "run_empty")
    record_terminal_run_metrics(tmp_db, "run_empty")

    rendered = metrics.render_prometheus()
    assert "council_answers_candidate_count_count 1" in rendered
    assert "council_runs_terminal_without_candidates_total 1" in rendered
