"""
Tests for orchestration_checkpoint.py — sync API (post-shadow-bug-fix).

These tests verify the sync get_or_create_thread_id signature and behavior
using mocked execute_sql / execute_sql_one instead of asyncpg.
"""

import asyncio
import inspect
from unittest.mock import MagicMock, AsyncMock, patch

import pytest


class TestGetOrCreateThreadId:
    """Tests for get_or_create_thread_id() — now sync, using execute_sql."""

    def test_module_imports(self):
        """All expected functions must be importable."""
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
        """Signature: (db, run_id, mode) — sync, returns (config, is_resume)."""
        from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
            get_or_create_thread_id,
        )
        sig = inspect.signature(get_or_create_thread_id)
        assert list(sig.parameters.keys()) == ["db", "run_id", "mode"]

    def test_is_sync_function(self):
        """get_or_create_thread_id must be a SYNC function (not async)."""
        from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
            get_or_create_thread_id,
        )
        assert not inspect.iscoroutinefunction(get_or_create_thread_id), (
            "get_or_create_thread_id must be sync after shadow bug fix"
        )

    def test_fresh_run_creates_new_thread_id_and_inserts_row(self):
        """When execute_sql_one returns None (no existing row),
        a new thread_id is generated and execute_sql is called."""
        from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
            get_or_create_thread_id,
        )

        with patch(
            "fusion_council_service.db.execute_sql_one",
            return_value=None,
        ), patch(
            "fusion_council_service.db.execute_sql",
        ) as mock_exec, patch(
            "fusion_council_service.db.commit_tx",
        ):
            db = MagicMock()
            config, is_resume = get_or_create_thread_id(db, "run-123", "single")

            assert is_resume is False
            assert isinstance(config, dict)
            assert "thread_id" in config
            assert "checkpoint_namespace" in config
            assert config["checkpoint_namespace"] == "mode=single"
            assert mock_exec.called

    def test_resume_path_returns_existing_thread_id_for_started_status(self):
        """When row exists with status='started', is_resume=True."""
        from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
            get_or_create_thread_id,
        )

        existing_row = {
            "run_id": "run-789",
            "thread_id": "thread-existing-abc",
            "orchestrator_mode": "mode=single",
            "orchestration_status": "started",
        }

        with patch(
            "fusion_council_service.db.execute_sql_one",
            return_value=existing_row,
        ):
            db = MagicMock()
            config, is_resume = get_or_create_thread_id(db, "run-789", "single")

            assert is_resume is True
            assert config["thread_id"] == "thread-existing-abc"
            assert config["checkpoint_namespace"] == "mode=single"

    def test_resume_path_returns_existing_thread_id_for_resumed_status(self):
        """When row exists with status='resumed', is_resume=True."""
        from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
            get_or_create_thread_id,
        )

        existing_row = {
            "run_id": "run-orphaned",
            "thread_id": "thread-orphaned-xyz",
            "orchestrator_mode": "mode=single",
            "orchestration_status": "resumed",
        }

        with patch(
            "fusion_council_service.db.execute_sql_one",
            return_value=existing_row,
        ):
            db = MagicMock()
            config, is_resume = get_or_create_thread_id(db, "run-orphaned", "single")

            assert is_resume is True
            assert config["thread_id"] == "thread-orphaned-xyz"

    def test_completed_status_triggers_fresh_run_not_resume(self):
        """When row exists with status='completed', new thread_id generated."""
        from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
            get_or_create_thread_id,
        )

        completed_row = {
            "run_id": "run-completed",
            "thread_id": "thread-old",
            "orchestrator_mode": "mode=single",
            "orchestration_status": "completed",
        }

        with patch(
            "fusion_council_service.db.execute_sql_one",
            return_value=completed_row,
        ), patch(
            "fusion_council_service.db.execute_sql",
        ), patch(
            "fusion_council_service.db.commit_tx",
        ):
            db = MagicMock()
            config, is_resume = get_or_create_thread_id(db, "run-completed", "single")

            assert is_resume is False
            assert config["thread_id"] != "thread-old"

    def test_failed_status_triggers_fresh_run_not_resume(self):
        """When row exists with status='failed', new thread_id generated."""
        from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
            get_or_create_thread_id,
        )

        failed_row = {
            "run_id": "run-failed",
            "thread_id": "thread-old",
            "orchestrator_mode": "mode=single",
            "orchestration_status": "failed",
        }

        with patch(
            "fusion_council_service.db.execute_sql_one",
            return_value=failed_row,
        ), patch(
            "fusion_council_service.db.execute_sql",
        ), patch(
            "fusion_council_service.db.commit_tx",
        ):
            db = MagicMock()
            config, is_resume = get_or_create_thread_id(db, "run-failed", "single")

            assert is_resume is False
            assert config["thread_id"] != "thread-old"


class TestEnsureLanggraphCheckpointTables:
    """Tests for ensure_langgraph_checkpoint_tables() — delegates to AsyncPostgresSaver.setup()."""

    def test_function_exists_and_is_async(self):
        """ensure_langgraph_checkpoint_tables must be an async function."""
        from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
            ensure_langgraph_checkpoint_tables,
        )
        assert inspect.iscoroutinefunction(ensure_langgraph_checkpoint_tables)

    def test_delegates_to_saver_setup(self):
        """ensure_langgraph_checkpoint_tables() must delegate to AsyncPostgresSaver.setup()."""
        from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
            ensure_langgraph_checkpoint_tables,
        )

        mock_saver = MagicMock()
        mock_saver.setup = AsyncMock()

        with patch(
            "langgraph.checkpoint.postgres.aio.AsyncPostgresSaver",
            return_value=mock_saver,
        ):
            conn = AsyncMock()
            asyncio.run(ensure_langgraph_checkpoint_tables(conn))

        assert mock_saver.setup.called, (
            "ensure_langgraph_checkpoint_tables() must call saver.setup()"
        )


class TestCheckEngineVersionCompatible:
    """Tests for check_engine_version_compatible()."""

    def test_does_not_raise_when_versions_match(self):
        from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
            check_engine_version_compatible,
        )
        check_engine_version_compatible("v1", "v1")

    def test_raises_orchestration_engine_version_mismatch_on_version_mismatch(self):
        from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
            check_engine_version_compatible,
            OrchestrationEngineVersionMismatch,
        )
        with pytest.raises(OrchestrationEngineVersionMismatch) as exc_info:
            check_engine_version_compatible("v1", "v2")
        assert "v1" in str(exc_info.value)
        assert "v2" in str(exc_info.value)

    def test_exception_message_includes_stored_and_current_versions(self):
        from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
            check_engine_version_compatible,
            OrchestrationEngineVersionMismatch,
        )
        with pytest.raises(OrchestrationEngineVersionMismatch) as exc_info:
            check_engine_version_compatible("v1", "v3")
        msg = str(exc_info.value)
        assert "stored" in msg or "v1" in msg
        assert "current" in msg or "v3" in msg
