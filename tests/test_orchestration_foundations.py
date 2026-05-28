"""Regression tests for LangGraph orchestration foundations."""

from fusion_council_service.db import initialize_schema


def test_settings_expose_orchestrator_flags():
    from fusion_council_service.config import Settings

    settings = Settings(
        DATABASE_PATH=":memory:",
        SERVICE_API_KEYS="test",
        SERVICE_ADMIN_API_KEYS="admin",
        MINIMAX_TOKEN_PLAN_API_KEY="test-minimax-key",
        OLLAMA_API_KEY="test-ollama-key",
    )

    assert settings.ORCHESTRATOR_ENGINE == "legacy"
    assert settings.ORCHESTRATOR_LANGGRAPH_MODES == ""
    assert settings.LANGGRAPH_CHECKPOINT_ENABLED is False
    assert settings.LANGGRAPH_THREAD_NAMESPACE == "fusion-council"
    assert settings.LANGGRAPH_ENGINE_VERSION == "v1"


def test_schema_contains_run_orchestration_state_table(tmp_db):
    initialize_schema(tmp_db)
    row = tmp_db.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table' AND name='run_orchestration_state'
        """
    ).fetchone()
    assert row is not None

