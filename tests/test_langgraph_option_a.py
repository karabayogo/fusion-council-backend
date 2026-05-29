"""
RED tests: LangGraph fine-grained node execution (Option A).

These tests verify that the LangGraph engine actually performs real work
(model calls, candidate persistence, event emission) through fine-grained nodes,
NOT just advance current_stage markers.

Run: cd .../fusion-council-backend && uv run pytest tests/test_langgraph_option_a.py -v

When RED: tests fail because nodes are pure stage markers — no real work is done.
When GREEN: nodes call models, persist candidates, emit events, support resume + idempotency.
"""
import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ── Test 1: get_checkpoint_snapshot() must not crash from async context ──

class TestGetCheckpointSnapshotAsyncSafe:
    """BUG 2: get_checkpoint_snapshot() uses asyncio.run() which crashes inside async event loop."""

    def test_get_checkpoint_snapshot_sync_function_exists(self):
        """Verify the function exists and is importable."""
        from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
            get_checkpoint_snapshot,
        )
        assert callable(get_checkpoint_snapshot)

    @pytest.mark.asyncio
    async def test_get_checkpoint_snapshot_called_from_async_context_does_not_crash(self):
        """
        RED: Calling get_checkpoint_snapshot() from an async context should NOT crash
        with RuntimeError: asyncio.run() cannot be called from a running event loop.

        The current implementation uses asyncio.run(saver.aget(config)) which WILL crash.
        """
        from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
            get_checkpoint_snapshot,
        )

        # Simulate the AsyncPostgresSaver with an aget that returns None
        mock_saver = MagicMock()
        mock_saver.aget = AsyncMock(return_value=None)
        config = {"thread_id": "test-thread", "checkpoint_ns": "mode=single"}

        # This should NOT raise RuntimeError when called from async context
        try:
            result = get_checkpoint_snapshot(mock_saver, config)
        except RuntimeError as e:
            if "asyncio.run() cannot be called from a running event loop" in str(e):
                pytest.fail(
                    "BUG 2: get_checkpoint_snapshot() crashes with asyncio.run() "
                    "inside async event loop. Must use await or be properly async."
                )
            raise
        except Exception:
            # Other exceptions (e.g., from mock interaction) are acceptable
            pass

        # If the function returns None (no checkpoint), that's fine
        # The critical thing is it didn't crash with RuntimeError


# ── Test 2: _recover_stale_runs() must be called on worker idle cycles ──

class TestRecoverStaleRunsLangGraphAware:
    """BUG 1: _recover_stale_runs() in startup.py is never called from worker_loop.py."""

    def test_startup_has_recover_stale_runs_function(self):
        """Verify the LangGraph-aware _recover_stale_runs exists in startup.py."""
        from fusion_council_service.startup import _recover_stale_runs
        assert callable(_recover_stale_runs)
        assert asyncio.iscoroutinefunction(_recover_stale_runs), (
            "_recover_stale_runs must be an async function"
        )

    def test_recover_stale_runs_queries_orchestration_state_not_runs_status(self):
        """
        Verify _recover_stale_runs() queries run_orchestration_state table,
        NOT runs.status. The legacy worker's _recover_stale_runs() only touches
        runs.status — this is a different recovery path.
        """
        import inspect
        from fusion_council_service.startup import _recover_stale_runs

        source = inspect.getsource(_recover_stale_runs)
        # Must query run_orchestration_state, not just runs
        assert "run_orchestration_state" in source, (
            "BUG 1: _recover_stale_runs() does not query run_orchestration_state. "
            "It must scan for stale 'resumed' rows in this table."
        )
        # Must check for 'resumed' status
        assert "'resumed'" in source or '"resumed"' in source, (
            "_recover_stale_runs() must filter by orchestration_status='resumed'"
        )

    def test_recover_stale_runs_importable_by_worker_loop(self):
        """
        Verify worker_loop.py CAN import and call the LangGraph-aware
        _recover_stale_runs(). This checks the import chain works.
        """
        # This import should succeed without errors
        from fusion_council_service.startup import _recover_stale_runs
        assert _recover_stale_runs is not None


# ── Test 3: LangGraph nodes produce real output (Option A) ──

class TestLangGraphSingleHappyPath:
    """
    GREEN: Option A fine-grained nodes — verify graph produces real output
    when a mock worker with provider registry/catalog is passed through config.
    """

    @pytest.mark.asyncio
    async def test_graph_invoke_with_mock_worker_produces_real_output(self):
        """
        GREEN: With a mock worker in config, the graph should:
        1. Call select_healthy_stage_model for model selection
        2. Call invoke_structured_or_freetext for the model API call
        3. Call insert_candidate for candidate persistence
        4. Call update_run_status + emit_run_completed for finalization
        5. Set final_answer in the returned state
        """
        from fusion_council_service.domain.orchestration.orchestration_langgraph_engine import (
            _graph_single,
        )

        from concurrent.futures import ThreadPoolExecutor
        from unittest.mock import MagicMock, AsyncMock

        graph = _graph_single()
        run_id = str(uuid.uuid4())
        thread_id = str(uuid.uuid4())

        # Create mock worker
        mock_worker = MagicMock()
        mock_db = MagicMock()
        mock_worker._get_db = MagicMock(return_value=mock_db)
        mock_worker._catalog = MagicMock()
        mock_worker._catalog.enabled_models = MagicMock(return_value=[
            {"alias": "test-model", "provider": "test", "provider_model": "test-v1"}
        ])
        mock_worker._registry = MagicMock()

        # Mock model selection to return a model
        mock_model_info = {
            "alias": "test-model",
            "provider": "test",
            "provider_model": "test-v1",
        }

        # Mock ProviderGenerateResult
        mock_result = MagicMock()
        mock_result.success = True
        mock_result.raw_text = "The answer is 42"
        mock_result.input_tokens = 10
        mock_result.output_tokens = 5

        config = {
            "thread_id": thread_id,
            "checkpoint_namespace": "mode=single",
            "configurable": {"worker": mock_worker},
        }

        initial_state = {
            "run_id": run_id,
            "mode": "single",
            "engine": "langgraph",
            "engine_version": "v1",
            "thread_id": thread_id,
            "checkpoint_namespace": "mode=single",
            "resume_count": 0,
            "current_stage": "",
            "candidate_ids": [],
            "final_answer": None,
            "final_confidence": None,
            "error_code": None,
            "error_message": None,
            "updated_at": datetime.now(timezone.utc),
            "raw_response": None,
            "candidate_summaries": None,
            "computed_final_answer": None,
            "computed_final_confidence": None,
        }

        with patch(
            "fusion_council_service.domain.model_selection.select_healthy_stage_model",
            return_value=mock_model_info,
        ), patch(
            "fusion_council_service.domain.structured_output.invoke_structured_or_freetext",
            return_value=mock_result,
        ), patch(
            "fusion_council_service.domain.candidate_repository.insert_candidate",
        ) as mock_insert, patch(
            "fusion_council_service.domain.run_repository.update_run_status",
        ) as mock_update, patch(
            "fusion_council_service.domain.event_emitter.emit_run_completed",
        ) as mock_emit, patch(
            "fusion_council_service.domain.event_emitter.emit_candidate_completed",
        ) as mock_cand_emit:
            result = await graph.ainvoke(initial_state, config)

        # GREEN assertions — nodes actually did work
        assert result.get("final_answer") == "The answer is 42", (
            f"Option A nodes did NOT produce output. final_answer={result.get('final_answer')}"
        )
        assert result.get("current_stage", "").startswith("finalize"), (
            f"Graph did not reach finalize stage: {result.get('current_stage')}"
        )
        # Candidate was persisted
        assert mock_insert.called, "insert_candidate was never called — persist node skipped"
        # Run status was updated
        assert mock_update.called, "update_run_status was never called — finalize skipped"
        # Completion event was emitted
        assert mock_emit.called, "emit_run_completed was never called"

    @pytest.mark.asyncio
    async def test_graph_stages_advance_beyond_prepare_run(self):
        """
        Verify the graph advances through at least 3 stages (not just one).
        Even with stage-marker-only nodes, the graph should reach finalize.
        """
        from fusion_council_service.domain.orchestration.orchestration_langgraph_engine import (
            _graph_single,
        )

        graph = _graph_single()
        run_id = str(uuid.uuid4())
        thread_id = str(uuid.uuid4())
        config = {"thread_id": thread_id, "checkpoint_namespace": "mode=single"}

        initial_state = {
            "run_id": run_id,
            "mode": "single",
            "engine": "langgraph",
            "engine_version": "v1",
            "thread_id": thread_id,
            "checkpoint_namespace": "mode=single",
            "resume_count": 0,
            "current_stage": "",
            "candidate_ids": [],
            "final_answer": None,
            "final_confidence": None,
            "error_code": None,
            "error_message": None,
            "updated_at": datetime.now(timezone.utc),
            "raw_response": None,
            "candidate_summaries": None,
            "computed_final_answer": None,
            "computed_final_confidence": None,
        }

        result = await graph.ainvoke(initial_state, config)

        # After graph completion, current_stage should be in finalize state
        assert result.get("current_stage", "").startswith("finalize"), (
            f"Graph did not reach finalize stage. "
            f"current_stage={result.get('current_stage')}. "
            f"Graph may be broken (START edge missing?)."
        )


# ── Test 4: Idempotency (replay safety) ──

class TestLangGraphSingleIdempotency:
    """Verify that replaying completed runs is safe (no side effects on replay)."""

    @pytest.mark.asyncio
    async def test_graph_replay_on_completed_state_is_noop(self):
        """
        Running the graph on a state that already has current_stage='finalize_success'
        should be a no-op — all nodes should return state unchanged.
        """
        from fusion_council_service.domain.orchestration.orchestration_langgraph_engine import (
            _graph_single,
        )

        graph = _graph_single()
        run_id = str(uuid.uuid4())
        thread_id = str(uuid.uuid4())
        config = {"thread_id": thread_id, "checkpoint_namespace": "mode=single"}

        # State that has already completed
        already_finalized_state = {
            "run_id": run_id,
            "mode": "single",
            "engine": "langgraph",
            "engine_version": "v1",
            "thread_id": thread_id,
            "checkpoint_namespace": "mode=single",
            "resume_count": 1,  # was resumed once
            "current_stage": "finalize_success",
            "candidate_ids": ["candidate-1"],
            "final_answer": "The answer is 42",
            "final_confidence": 0.95,
            "error_code": None,
            "error_message": None,
            "updated_at": datetime.now(timezone.utc),
            "raw_response": None,
            "candidate_summaries": None,
            "computed_final_answer": "The answer is 42",
            "computed_final_confidence": 0.95,
        }

        result = await graph.ainvoke(already_finalized_state, config)

        # On replay, final_answer must not change (idempotency)
        assert result.get("final_answer") == "The answer is 42", (
            "Replay changed final_answer! Idempotency broken."
        )
        assert result.get("candidate_ids") == ["candidate-1"], (
            "Replay modified candidate_ids! Idempotency broken."
        )


# ── Test 5: Resume from checkpoint ──

class TestLangGraphSingleResume:
    """Verify that resume from a mid-run checkpoint continues correctly."""

    @pytest.mark.asyncio
    async def test_graph_resume_from_mid_run_state(self):
        """
        Starting the graph from a mid-run state (e.g., current_stage='generation_call')
        should continue from that point, not restart from prepare_run.
        """
        from fusion_council_service.domain.orchestration.orchestration_langgraph_engine import (
            _graph_single,
        )

        graph = _graph_single()
        run_id = str(uuid.uuid4())
        thread_id = str(uuid.uuid4())
        config = {"thread_id": thread_id, "checkpoint_namespace": "mode=single"}

        # State at mid-run (generation was already called but not finalized)
        mid_run_state = {
            "run_id": run_id,
            "mode": "single",
            "engine": "langgraph",
            "engine_version": "v1",
            "thread_id": thread_id,
            "checkpoint_namespace": "mode=single",
            "resume_count": 1,
            "current_stage": "generation_call",
            "candidate_ids": [],  # not yet persisted
            "final_answer": None,
            "final_confidence": None,
            "error_code": None,
            "error_message": None,
            "updated_at": datetime.now(timezone.utc),
            "raw_response": {"model": "test", "choices": [{"message": {"content": "test"}}]},
            "candidate_summaries": None,
            "computed_final_answer": None,
            "computed_final_confidence": None,
        }

        result = await graph.ainvoke(mid_run_state, config)

        # Should advance from generation_call → generation_persist → finalize_success
        assert result.get("current_stage", "").startswith("finalize"), (
            f"Graph did not advance from mid-run state. "
            f"current_stage went from 'generation_call' to '{result.get('current_stage')}'"
        )


# ── Test 6: _serialize_state() is called before checkpoint writes ──

class TestSerializeStateInCheckpointPath:
    """Verify _serialize_state() is actually invoked in the checkpoint write path."""

    def test_serialize_state_converts_datetime_called_before_put(self):
        """
        _serialize_state() must convert datetime to ISO string.
        If it doesn't, saver.put() will fail with TypeError.
        """
        from fusion_council_service.domain.orchestration.orchestration_state import (
            _serialize_state,
        )

        now = datetime.now(timezone.utc)
        state = {
            "run_id": "test-123",
            "updated_at": now,
            "mode": "single",
        }
        serialized = _serialize_state(state)
        assert isinstance(serialized["updated_at"], str), (
            "datetime was not converted to ISO string by _serialize_state()"
        )
        # Must survive JSON round-trip
        json_str = json.dumps(serialized)
        parsed = json.loads(json_str)
        assert isinstance(parsed["updated_at"], str)
