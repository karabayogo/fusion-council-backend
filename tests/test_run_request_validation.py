from fastapi.testclient import TestClient

from fusion_council_service.api import routes as api_routes
from fusion_council_service.main import app


def test_create_run_rejects_max_output_tokens_above_cap(auth_headers_admin, tmp_db, mock_settings, monkeypatch):
    monkeypatch.setattr(api_routes, "get_api_db", lambda: tmp_db)
    monkeypatch.setattr(api_routes, "_settings", mock_settings)

    client = TestClient(app)

    response = client.post(
        "/v1/runs",
        headers=auth_headers_admin,
        json={
            "mode": "single",
            "prompt": "Hello",
            "temperature": 0.2,
            "max_output_tokens": 30001,
            "deadline_seconds": 60,
        },
    )

    assert response.status_code == 422
    detail = response.json()["detail"]
    assert any("max_output_tokens" in str(item) for item in detail)
