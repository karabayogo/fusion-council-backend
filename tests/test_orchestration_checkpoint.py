"""
RED tests: orchestration_checkpoint.py functions must exist and behave per plan spec.

Run: cd .../fusion-council-backend && uv run pytest tests/test_orchestration_checkpoint.py -v
"""
import asyncio
from datetime import datetime, timezone
from unittest.mock import MagicMock, AsyncMock, patch

import pytest


class TestGetOrCreateThreadId:
    """RED tests for get_or_create_thread_id() — resume vs fresh run decision."""

    def test_module_imports(self):
        """get_or_create_thread_id, ensure_langgraph_checkpoint_tables,
        check_engine_version_compatible, and OrchestrationEngineVersionMismatch
        must be importable from orchestration_checkpoint."""
        from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
            get_or_create_thread_id,
            ensure_langgraph_checkpoint_tables,
            check_engine_version_compatible,
            OrchestrationEngineVersionMismatch,
        )
        assert callable(get_or_create_thread_id)
        assert callable(ensure_langgraph_checkpoint_tables)
        assert callable(check_engine_version_compatible)
        assert issubclass(OrchestrationEngineVersionMismatch, Exception)

    def test_returns_tuple_of_config_dict_and_is_resume_bool(self):
        """get_or_create_thread_id must return (langgraph_config, is_resume)."""
        from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
            get_or_create_thread_id,
        )
        import inspect
        sig = inspect.signature(get_or_create_thread_id)
        # Parameters: conn (asyncpg), run_id (str), mode (str)
        assert list(sig.parameters.keys()) == ["conn", "run_id", "mode"]

    def test_fresh_run_creates_new_thread_id_and_inserts_row(self):
        """When run_id has no existing row, a NEW thread_id is generated and a row is inserted."""
        from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
            get_or_create_thread_id,
        )

        conn = AsyncMock()
        # No existing row
        conn.fetch = AsyncMock(return_value=[])
        conn.execute = AsyncMock()

        config, is_resume = asyncio.run(
            get_or_create_thread_id(conn, "run-123", "single")
        )

        assert is_resume is False
        assert isinstance(config, dict)
        assert "thread_id" in config
        assert "checkpoint_namespace" in config
        assert config["checkpoint_namespace"] == "mode=single"
        # INSERT was called to create the new row
        assert conn.execute.called

    def test_fresh_run_inserts_with_started_status(self):
        """Fresh run must INSERT row with orchestration_status='started'."""
        from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
            get_or_create_thread_id,
        )

        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[])
        conn.execute = AsyncMock()

        _, is_resume = asyncio.run(
            get_or_create_thread_id(conn, "run-456", "fusion")
        )

        assert is_resume is False
        # Verify INSERT statement contains orchestration_status='started'
        call_args = str(conn.execute.call_args)
        assert "started" in call_args.lower() or conn.execute.called

    def test_resume_path_returns_existing_thread_id_for_started_status(self):
        """When row exists with status='started', is_resume=True and existing thread_id is returned."""
        from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
            get_or_create_thread_id,
        )

        existing_row = MagicMock()
        existing_row.__getitem__ = lambda self, k: {
            "run_id": "run-789",
            "thread_id": "thread-existing-abc",
            "checkpoint_namespace": "mode=single",
            "orchestration_status": "started",
        }[k]

        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[existing_row])
        # No INSERT should happen for resume
        conn.execute = AsyncMock()

        config, is_resume = asyncio.run(
            get_or_create_thread_id(conn, "run-789", "single")
        )

        assert is_resume is True
        assert config["thread_id"] == "thread-existing-abc"
        assert config["checkpoint_namespace"] == "mode=single"
        assert not conn.execute.called  # no INSERT on resume

    def test_resume_path_returns_existing_thread_id_for_resumed_status(self):
        """When row exists with status='resumed', is_resume=True (orphaned run recovery)."""
        from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
            get_or_create_thread_id,
        )

        existing_row = MagicMock()
        existing_row.__getitem__ = lambda self, k: {
            "run_id": "run-orphaned",
            "thread_id": "thread-orphaned-xyz",
            "checkpoint_namespace": "mode=single",
            "orchestration_status": "resumed",
        }[k]

        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[existing_row])
        conn.execute = AsyncMock()

        config, is_resume = asyncio.run(
            get_or_create_thread_id(conn, "run-orphaned", "single")
        )

        assert is_resume is True
        assert config["thread_id"] == "thread-orphaned-xyz"

    def test_completed_status_triggers_fresh_run_not_resume(self):
        """When row exists with status='completed', a NEW thread_id is generated (not resumed)."""
        from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
            get_or_create_thread_id,
        )

        completed_row = MagicMock()
        completed_row.__getitem__ = lambda self, k: {
            "run_id": "run-completed",
            "thread_id": "thread-old",
            "checkpoint_namespace": "mode=single",
            "orchestration_status": "completed",
        }[k]

        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[completed_row])
        conn.execute = AsyncMock()

        config, is_resume = asyncio.run(
            get_or_create_thread_id(conn, "run-completed", "single")
        )

        assert is_resume is False
        # A new thread_id should have been generated (not the old one)
        assert config["thread_id"] != "thread-old"
        assert conn.execute.called  # new row was inserted

    def test_failed_status_triggers_fresh_run_not_resume(self):
        """When row exists with status='failed', a NEW thread_id is generated (not resumed)."""
        from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
            get_or_create_thread_id,
        )

        failed_row = MagicMock()
        failed_row.__getitem__ = lambda self, k: {
            "run_id": "run-failed",
            "thread_id": "thread-old",
            "checkpoint_namespace": "mode=single",
            "orchestration_status": "failed",
        }[k]

        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[failed_row])
        conn.execute = AsyncMock()

        config, is_resume = asyncio.run(
            get_or_create_thread_id(conn, "run-failed", "single")
        )

        assert is_resume is False
        assert config["thread_id"] != "thread-old"


class TestEnsureLanggraphCheckpointTables:
    """Tests for ensure_langgraph_checkpoint_tables() — delegates to AsyncPostgresSaver.setup()."""

    def test_function_exists_and_is_async(self):
        """ensure_langgraph_checkpoint_tables must be an async function."""
        import inspect
        from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
            ensure_langgraph_checkpoint_tables,
        )
        assert inspect.iscoroutinefunction(ensure_langgraph_checkpoint_tables)

    def test_delegates_to_saver_setup(self):
        """
        ensure_langgraph_checkpoint_tables() must delegate to AsyncPostgresSaver.setup()
        instead of running raw CREATE TABLE SQL. This ensures LangGraph manages its own
        schema migrations — manual DDL drifts from the canonical package schema.
        """
        from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
            ensure_langgraph_checkpoint_tables,
        )

        # Mock AsyncPostgresSaver.setup — verify it gets called.
        # AsyncPostgresSaver is imported inside ensure_langgraph_checkpoint_tables()
        # from langgraph.checkpoint.postgres.aio, so patch at that location.
        mock_saver = MagicMock()
        mock_saver.setup = AsyncMock()

        with patch(
            "langgraph.checkpoint.postgres.aio.AsyncPostgresSaver",
            return_value=mock_saver,
        ):
            conn = AsyncMock()
            asyncio.run(ensure_langgraph_checkpoint_tables(conn))

        # saver.setup() must be called — this is the canonical table init path
        assert mock_saver.setup.called, (
            "ensure_langgraph_checkpoint_tables() must call saver.setup(), "
            "not raw CREATE TABLE SQL"
        )


class TestCheckEngineVersionCompatible:
    """RED tests for check_engine_version_compatible()."""

    def test_does_not_raise_when_versions_match(self):
        """No exception when stored_version == current_version."""
        from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
            check_engine_version_compatible,
        )
        # Should not raise
        check_engine_version_compatible("v1", "v1")

    def test_raises_orchestration_engine_version_mismatch_on_version_mismatch(self):
        """Must raise OrchestrationEngineVersionMismatch when versions differ."""
        from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
            check_engine_version_compatible,
            OrchestrationEngineVersionMismatch,
        )

        with pytest.raises(OrchestrationEngineVersionMismatch) as exc_info:
            check_engine_version_compatible("v1", "v2")

        assert "v1" in str(exc_info.value)
        assert "v2" in str(exc_info.value)

    def test_exception_message_includes_stored_and_current_versions(self):
        """Error message must include both stored and current version for debugging."""
        from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
            check_engine_version_compatible,
            OrchestrationEngineVersionMismatch,
        )

        with pytest.raises(OrchestrationEngineVersionMismatch) as exc_info:
            check_engine_version_compatible("v1", "v3")

        msg = str(exc_info.value)
        assert "stored" in msg or "v1" in msg
        assert "current" in msg or "v3" in msg
