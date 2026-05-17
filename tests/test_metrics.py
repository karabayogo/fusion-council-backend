from fusion_council_service import metrics


def test_metrics_endpoint_returns_prometheus_text(monkeypatch):
    from fastapi.testclient import TestClient
    import fusion_council_service.main as main

    class Settings:
        APP_ENV = "test"

    monkeypatch.setattr(main, "_settings", Settings())
    monkeypatch.setattr(main, "_catalog", ["a", "b"])
    metrics.reset_metrics()
    metrics.increment_terminal_without_candidates()

    response = TestClient(main.app).get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert "council_runs_terminal_without_candidates_total 1" in response.text
    assert 'fusion_council_app_info{app_env="test",catalog_models="2"} 1' in response.text


def test_metrics_render_prometheus_text_contains_required_series():
    metrics.reset_metrics()
    metrics.observe_council_answers_candidate_count(9)
    metrics.increment_terminal_without_candidates()
    metrics.observe_stage_duration("first_opinion", 1.25)
    metrics.increment_candidate_status("succeeded")
    metrics.increment_candidate_status("failed")

    rendered = metrics.render_prometheus(app_env="test", catalog_models=6)

    assert 'fusion_council_app_info{app_env="test",catalog_models="6"} 1' in rendered
    assert "council_answers_candidate_count_bucket" in rendered
    assert "council_answers_candidate_count_count 1" in rendered
    assert "council_answers_candidate_count_sum 9.0" in rendered
    assert "council_runs_terminal_without_candidates_total 1" in rendered
    assert 'council_stage_duration_seconds_count{stage="first_opinion"} 1' in rendered
    assert 'council_stage_duration_seconds_sum{stage="first_opinion"} 1.25' in rendered
    assert 'council_candidate_status_total{status="succeeded"} 1' in rendered
    assert 'council_candidate_status_total{status="failed"} 1' in rendered


def test_metrics_reset_isolation():
    metrics.reset_metrics()
    metrics.increment_candidate_status("failed")
    assert 'council_candidate_status_total{status="failed"} 1' in metrics.render_prometheus()

    metrics.reset_metrics()
    rendered = metrics.render_prometheus()

    assert 'council_candidate_status_total{status="failed"}' not in rendered
    assert "council_runs_terminal_without_candidates_total 0" in rendered
