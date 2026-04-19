"""Tests for the GET /v1/runs/{run_id}/answers endpoint and succeeded_degraded status."""

import sqlite3
import pytest

from fusion_council_service.config import Settings
from fusion_council_service.domain.budget import should_degrade, resolve_deadline
from fusion_council_service.domain.candidate_repository import insert_candidate, update_candidate_result
from fusion_council_service.domain.event_emitter import emit_run_succeeded_degraded
from fusion_council_service.domain.run_repository import insert_run, update_run_status
from fusion_council_service.ids import new_run_id, new_candidate_id
from fusion_council_service.clock import utc_now_iso


def _open_test_db() -> sqlite3.Connection:
    """Open an in-memory SQLite DB with check_same_thread=False for test client compat."""
    from fusion_council_service.db import initialize_schema
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=memory")
    conn.execute("PRAGMA foreign_keys=ON")
    initialize_schema(conn)
    return conn


@pytest.fixture
def db():
    """In-memory DB for testing."""
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


class TestAnswersEndpoint:
    """Tests for GET /v1/runs/{run_id}/answers."""

    def test_answers_returns_candidates(self, db, settings):
        """Answers endpoint should return all candidates for a run."""
        from fusion_council_service.api.routes import init_api, get_auth_dependency

        init_api(settings)
        import fusion_council_service.api.routes as routes_mod
        routes_mod._api_db = db

        # Insert a run
        run_id = new_run_id()
        insert_run(
            db=db, run_id=run_id, mode="single",
            prompt="test", system_prompt=None,
            temperature=0.2, max_output_tokens=100,
            deadline_seconds=60, deadline_at="2026-01-01T00:01:00Z",
            owner_token_hash="abc123", metadata_json="{}",
            requested_models_json=None, created_at=utc_now_iso(),
        )
        update_run_status(db, run_id, "queued", created_at=utc_now_iso())

        # Insert candidates
        cand1 = new_candidate_id()
        insert_candidate(db, run_id, cand1, "minimax-portal/MiniMax-M2.7", "minimax_token_plan",
                         "MiniMax-M2.7", "generation", "succeeded", utc_now_iso())
        update_candidate_result(db, cand1, "succeeded", raw_answer="Answer from MiniMax")

        cand2 = new_candidate_id()
        insert_candidate(db, run_id, cand2, "ollama/glm-5.1:cloud", "ollama_cloud",
                         "glm-5.1", "generation", "failed", utc_now_iso())
        update_candidate_result(db, cand2, "failed", error_code="TIMEOUT", error_message="Request timed out")

        # Test via FastAPI test client
        from fastapi.testclient import TestClient
        from fusion_council_service.main import app

        # Override auth dependency
        app.dependency_overrides[get_auth_dependency] = lambda: ("test-key", "user")

        client = TestClient(app)
        response = client.get(
            f"/v1/runs/{run_id}/answers",
            headers={"Authorization": "Bearer test-key"},
        )

        # Clean up override
        app.dependency_overrides.clear()

        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        data = response.json()
        assert data["run_id"] == run_id
        assert data["count"] == 2
        assert data["candidates"][0]["alias"] == "minimax-portal/MiniMax-M2.7"

    def test_answers_404_for_missing_run(self, db, settings):
        """Answers endpoint should return 404 for non-existent run."""
        from fusion_council_service.api.routes import init_api, get_auth_dependency

        init_api(settings)
        import fusion_council_service.api.routes as routes_mod
        routes_mod._api_db = db

        from fastapi.testclient import TestClient
        from fusion_council_service.main import app

        app.dependency_overrides[get_auth_dependency] = lambda: ("test-key", "user")

        client = TestClient(app)
        response = client.get(
            "/v1/runs/run_nonexistent/answers",
            headers={"Authorization": "Bearer test-key"},
        )
        app.dependency_overrides.clear()

        assert response.status_code == 404


class TestSucceededDegraded:
    """Tests for succeeded_degraded status and deadline degradation."""

    def test_should_degrade_fusion_skip_verification(self):
        """Fusion mode should degrade to skip verification at >85% deadline."""
        reason = should_degrade("fusion", 103, 120)  # 103/120 ≈ 85.8%
        assert reason == "fusion_approaching_deadline_skip_verification"

    def test_should_degrade_fusion_return_best(self):
        """Fusion mode should return best candidate at >95% deadline."""
        reason = should_degrade("fusion", 115, 120)  # 115/120 ≈ 95.8%
        assert reason == "fusion_deadline_imminent_return_best_candidate"

    def test_should_degrade_council_skip_debate(self):
        """Council mode should skip debate at >80% deadline."""
        reason = should_degrade("council", 97, 120)  # 97/120 ≈ 80.8%
        assert reason == "council_skip_debate"

    def test_should_degrade_council_skip_peer_review(self):
        """Council mode should skip peer review at >90% deadline."""
        reason = should_degrade("council", 109, 120)  # 109/120 ≈ 90.8%
        assert reason == "council_skip_peer_review"

    def test_should_degrade_council_deadline_imminent(self):
        """Council mode should return best opinion at >95% deadline."""
        reason = should_degrade("council", 115, 120)  # 115/120 ≈ 95.8%
        assert reason == "council_deadline_imminent_return_best_opinion"

    def test_should_degrade_no_degradation(self):
        """No degradation when well within deadline."""
        reason = should_degrade("fusion", 30, 120)  # 25%
        assert reason is None

    def test_emit_run_succeeded_degraded(self, db):
        """Emit succeeded_degraded event."""
        run_id = new_run_id()
        insert_run(
            db=db, run_id=run_id, mode="fusion",
            prompt="test", system_prompt=None,
            temperature=0.2, max_output_tokens=100,
            deadline_seconds=120, deadline_at="2026-01-01T00:02:00Z",
            owner_token_hash="abc123", metadata_json="{}",
            requested_models_json=None, created_at=utc_now_iso(),
        )

        result = emit_run_succeeded_degraded(
            db, run_id, "Best available answer",
            "fusion_approaching_deadline_skip_verification",
            confidence=0.6,
        )
        assert result["event_type"] == "run.succeeded_degraded"

    def test_resolve_deadline_defaults(self):
        """Deadline defaults per mode."""
        d, applied = resolve_deadline("single", None)
        assert d == 60
        assert applied == 1

        d, applied = resolve_deadline("fusion", None)
        assert d == 120
        assert applied == 1

        d, applied = resolve_deadline("council", None)
        assert d == 120
        assert applied == 1

    def test_resolve_deadline_ceiling(self):
        """Deadline should be capped at mode ceiling."""
        d, applied = resolve_deadline("single", 500)
        assert d == 300
        assert applied == 2

        d, applied = resolve_deadline("fusion", 500)
        assert d == 300
        assert applied == 2