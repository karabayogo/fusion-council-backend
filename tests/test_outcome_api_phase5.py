"""Phase 5 regression tests for PATCH /v1/runs/{run_id}/outcome."""

import sqlite3
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from fusion_council_service.clock import utc_now_iso, utc_now_plus_seconds
from fusion_council_service.config import Settings
from fusion_council_service.domain.decision_log import log_pending_decision
from fusion_council_service.domain.run_repository import insert_run, update_run_status
from fusion_council_service.main import app


def _open_test_db() -> sqlite3.Connection:
    from fusion_council_service.db import initialize_schema

    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    initialize_schema(conn)
    return conn


@pytest.fixture
def db():
    conn = _open_test_db()
    yield conn
    conn.close()


@pytest.fixture
def settings():
    return Settings(
        DATABASE_PATH=":memory:",
        SERVICE_API_KEYS="test-key",
        SERVICE_ADMIN_API_KEYS="admin-key",
        MINIMAX_TOKEN_PLAN_API_KEY="test",
        OLLAMA_API_KEY="test",
        MINIMAX_ANTHROPIC_BASE_URL="https://api.minimax.io/anthropic",
        OLLAMA_BASE_URL="https://ollama.com",
    )


def _seed_completed_run_with_pending_decision(db, run_id: str, mode: str = "fusion"):
    insert_run(
        db=db,
        run_id=run_id,
        mode=mode,
        prompt="How should I structure retirement drawdown?",
        system_prompt=None,
        temperature=0.2,
        max_output_tokens=1200,
        deadline_seconds=300,
        deadline_at=utc_now_plus_seconds(300),
        owner_token_hash="abc123",
        metadata_json="{}",
        requested_models_json=None,
        created_at=utc_now_iso(),
    )
    update_run_status(db, run_id, "succeeded", final_answer="Use a guardrail withdrawal strategy.")
    log_pending_decision(
        db,
        run_id=run_id,
        prompt="How should I structure retirement drawdown?",
        mode=mode,
        final_answer="Use a guardrail withdrawal strategy.",
    )


def test_submit_outcome_resolves_pending_decision_and_rotates(db, settings):
    from fusion_council_service.api.routes import get_auth_dependency, init_api
    import fusion_council_service.api.routes as routes_mod

    init_api(settings)
    routes_mod._api_db = db
    routes_mod._registry = object()

    _seed_completed_run_with_pending_decision(db, "run_phase5_ok")

    with patch("fusion_council_service.api.routes.generate_reflection", return_value="Reflection lesson"), \
         patch("fusion_council_service.api.routes.rotate_decision_log") as rotate_mock:
        app.dependency_overrides[get_auth_dependency] = lambda: ("test-key", "user")
        client = TestClient(app)
        response = client.patch(
            "/v1/runs/run_phase5_ok/outcome",
            headers={"Authorization": "Bearer test-key"},
            json={"rating": "helpful", "outcome_raw": 4.0},
        )
        app.dependency_overrides.clear()

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["ok"] is True
    assert payload["run_id"] == "run_phase5_ok"
    assert payload["resolution"]["rating"] == "helpful"
    assert payload["resolution"]["outcome_raw"] == 4.0
    assert payload["resolution"]["reflection"] == "Reflection lesson"
    rotate_mock.assert_called_once()

    row = db.execute(
        "SELECT pending, rating, outcome_raw, reflection, resolved_at FROM decision_log WHERE run_id = ?",
        ("run_phase5_ok",),
    ).fetchone()
    assert row is not None
    assert row["pending"] == 0
    assert row["rating"] == "helpful"
    assert float(row["outcome_raw"]) == 4.0
    assert row["reflection"] == "Reflection lesson"
    assert row["resolved_at"]


def test_submit_outcome_defaults_outcome_raw_when_omitted(db, settings):
    from fusion_council_service.api.routes import get_auth_dependency, init_api
    import fusion_council_service.api.routes as routes_mod

    init_api(settings)
    routes_mod._api_db = db
    routes_mod._registry = object()
    _seed_completed_run_with_pending_decision(db, "run_phase5_default_raw", mode="council")

    with patch("fusion_council_service.api.routes.generate_reflection", return_value="Default-score reflection"):
        app.dependency_overrides[get_auth_dependency] = lambda: ("test-key", "user")
        client = TestClient(app)
        response = client.patch(
            "/v1/runs/run_phase5_default_raw/outcome",
            headers={"Authorization": "Bearer test-key"},
            json={"rating": "partial"},
        )
        app.dependency_overrides.clear()

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["resolution"]["rating"] == "partial"
    assert payload["resolution"]["outcome_raw"] == 3.0


def test_submit_outcome_rejects_unknown_rating(db, settings):
    from fusion_council_service.api.routes import get_auth_dependency, init_api
    import fusion_council_service.api.routes as routes_mod

    init_api(settings)
    routes_mod._api_db = db
    routes_mod._registry = object()
    _seed_completed_run_with_pending_decision(db, "run_phase5_bad_rating")

    app.dependency_overrides[get_auth_dependency] = lambda: ("test-key", "user")
    client = TestClient(app)
    response = client.patch(
        "/v1/runs/run_phase5_bad_rating/outcome",
        headers={"Authorization": "Bearer test-key"},
        json={"rating": "excellent"},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 422


def test_submit_outcome_returns_404_when_run_missing(db, settings):
    from fusion_council_service.api.routes import get_auth_dependency, init_api
    import fusion_council_service.api.routes as routes_mod

    init_api(settings)
    routes_mod._api_db = db
    routes_mod._registry = object()

    app.dependency_overrides[get_auth_dependency] = lambda: ("test-key", "user")
    client = TestClient(app)
    response = client.patch(
        "/v1/runs/run_does_not_exist/outcome",
        headers={"Authorization": "Bearer test-key"},
        json={"rating": "helpful", "outcome_raw": 5},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 404


def test_submit_outcome_returns_409_when_no_pending_decision(db, settings):
    from fusion_council_service.api.routes import get_auth_dependency, init_api
    import fusion_council_service.api.routes as routes_mod

    init_api(settings)
    routes_mod._api_db = db
    routes_mod._registry = object()

    insert_run(
        db=db,
        run_id="run_phase5_no_pending",
        mode="single",
        prompt="Question with no pending decision row",
        system_prompt=None,
        temperature=0.2,
        max_output_tokens=1200,
        deadline_seconds=300,
        deadline_at=utc_now_plus_seconds(300),
        owner_token_hash="abc123",
        metadata_json="{}",
        requested_models_json=None,
        created_at=utc_now_iso(),
    )
    update_run_status(db, "run_phase5_no_pending", "succeeded", final_answer="answer exists")

    app.dependency_overrides[get_auth_dependency] = lambda: ("test-key", "user")
    client = TestClient(app)
    response = client.patch(
        "/v1/runs/run_phase5_no_pending/outcome",
        headers={"Authorization": "Bearer test-key"},
        json={"rating": "helpful", "outcome_raw": 4},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 409
