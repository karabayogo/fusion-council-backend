from fastapi.testclient import TestClient

from fusion_council_service.main import app
from fusion_council_service.api import routes
from fusion_council_service.domain.event_emitter import emit_run_completed, emit_run_started, emit_stage_started
from fusion_council_service.domain.run_repository import insert_run
from fusion_council_service.clock import utc_now_iso, utc_now_plus_seconds


def test_run_events_history_returns_envelopes(tmp_db, monkeypatch, auth_headers_admin, mock_settings):
    run_id = "run_history_route_test"
    insert_run(
        db=tmp_db,
        run_id=run_id,
        mode="single",
        prompt="test",
        system_prompt=None,
        temperature=0.2,
        max_output_tokens=1000,
        deadline_seconds=60,
        deadline_at=utc_now_plus_seconds(60),
        owner_token_hash="testhash",
        metadata_json="{}",
        requested_models_json=None,
        created_at=utc_now_iso(),
    )
    emit_run_started(tmp_db, run_id, "single")
    emit_stage_started(tmp_db, run_id, "generation", ["model-a"])
    emit_run_completed(tmp_db, run_id, "final answer", confidence=0.9)

    monkeypatch.setattr(routes, "get_api_db", lambda: tmp_db)
    monkeypatch.setattr(routes, "_settings", mock_settings)
    client = TestClient(app)

    response = client.get(f"/v1/runs/{run_id}/events/history", headers=auth_headers_admin)

    assert response.status_code == 200
    payload = response.json()
    assert payload["run_id"] == run_id
    assert payload["count"] == 3
    assert payload["last_seq"] == 3
    assert [event["seq"] for event in payload["events"]] == [1, 2, 3]
    assert payload["events"][1]["event_type"] == "stage.started"
    assert payload["events"][1]["created_at"]
    assert payload["events"][1]["payload"]["stage"] == "generation"
    assert payload["events"][2]["payload"]["final_answer"] == "final answer"
