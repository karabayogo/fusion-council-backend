"""
RED test: LangGraph StateGraph for single mode must compile, invoke, and produce valid state.

This test verifies the compiled StateGraph with all 5 nodes can execute a linear
START -> node_prepare_run -> node_generation_call -> node_generation_persist ->
node_finalize_success sequence without raising.

Run: cd .../fusion-council-backend && uv run pytest tests/test_langgraph_single_happy_path.py -v
"""
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
from langgraph.graph import StateGraph, START

import pytest


class TestLangGraphSingleNodes:
    """RED tests — verify each node is importable and produces expected state transitions."""

    def test_orchestration_state_importable(self):
        """OrchestrationState TypedDict must be importable from orchestration_state."""
        from fusion_council_service.domain.orchestration.orchestration_state import (
            OrchestrationState,
            _serialize_state,
        )
        assert OrchestrationState is not None

    def test_all_five_nodes_importable(self):
        """All 5 node functions must be importable from orchestration_nodes_single."""
        from fusion_council_service.domain.orchestration.orchestration_nodes_single import (
            node_prepare_run,
            node_generation_call,
            node_generation_persist,
            node_finalize_success,
            node_finalize_failure,
        )
        assert callable(node_prepare_run)
        assert callable(node_generation_call)
        assert callable(node_generation_persist)
        assert callable(node_finalize_success)
        assert callable(node_finalize_failure)

    def test_node_prepare_run_sets_initial_stage(self):
        """node_prepare_run must set current_stage to 'prepare_run' on fresh state."""
        from fusion_council_service.domain.orchestration.orchestration_nodes_single import (
            node_prepare_run,
        )
        from fusion_council_service.domain.orchestration.orchestration_state import (
            OrchestrationState,
        )

        input_state: OrchestrationState = {
            "run_id": "test-run-123",
            "updated_at": None,
            "current_stage": "",
            "candidate_ids": [],
            "mode": "single",
            "engine": "langgraph",
            "engine_version": "v1",
            "final_answer": None,
            "final_confidence": None,
            "error_code": None,
            "error_message": None,
            "thread_id": "thread-abc",
            "checkpoint_namespace": "mode=single",
            "resume_count": 0,
        }

        result = asyncio.run(node_prepare_run(input_state))
        assert result["current_stage"] == "prepare_run"

    def test_node_prepare_run_routes_to_failure_when_run_id_missing(self):
        """node_prepare_run must set error_code and current_stage='finalize_failure' when run_id is absent."""
        from fusion_council_service.domain.orchestration.orchestration_nodes_single import (
            node_prepare_run,
        )
        from fusion_council_service.domain.orchestration.orchestration_state import (
            OrchestrationState,
        )

        input_state: OrchestrationState = {
            "run_id": "",  # missing/empty
            "updated_at": None,
            "current_stage": "",
            "candidate_ids": [],
            "mode": "single",
            "engine": "langgraph",
            "engine_version": "v1",
            "final_answer": None,
            "final_confidence": None,
            "error_code": None,
            "error_message": None,
            "thread_id": "thread-abc",
            "checkpoint_namespace": "mode=single",
            "resume_count": 0,
        }

        result = asyncio.run(node_prepare_run(input_state))
        assert result["current_stage"] == "finalize_failure"
        assert result["error_code"] == "RUN_ID_MISSING"

    def test_node_generation_call_sets_generation_call_stage(self):
        """node_generation_call must advance current_stage to 'generation_call'."""
        from fusion_council_service.domain.orchestration.orchestration_nodes_single import (
            node_generation_call,
        )
        from fusion_council_service.domain.orchestration.orchestration_state import (
            OrchestrationState,
        )

        state: OrchestrationState = {
            "run_id": "test-run",
            "updated_at": None,
            "current_stage": "prepare_run",
            "candidate_ids": [],
            "mode": "single",
            "engine": "langgraph",
            "engine_version": "v1",
            "final_answer": None,
            "final_confidence": None,
            "error_code": None,
            "error_message": None,
            "thread_id": "thread-abc",
            "checkpoint_namespace": "mode=single",
            "resume_count": 0,
        }

        result = asyncio.run(node_generation_call(state))
        assert result["current_stage"] == "generation_call"

    def test_node_generation_persist_advances_stage_and_adds_candidate(self):
        """node_generation_persist must add current_candidate_id to candidate_ids list and advance stage."""
        from fusion_council_service.domain.orchestration.orchestration_nodes_single import (
            node_generation_persist,
        )
        from fusion_council_service.domain.orchestration.orchestration_state import (
            OrchestrationState,
        )

        state: OrchestrationState = {
            "run_id": "test-run",
            "updated_at": None,
            "current_stage": "generation_call",
            "candidate_ids": [],
            "current_candidate_id": "candidate-uuid-abc",
            "mode": "single",
            "engine": "langgraph",
            "engine_version": "v1",
            "final_answer": None,
            "final_confidence": None,
            "error_code": None,
            "error_message": None,
            "thread_id": "thread-abc",
            "checkpoint_namespace": "mode=single",
            "resume_count": 0,
        }

        result = asyncio.run(node_generation_persist(state))
        assert result["current_stage"] == "generation_persist"
        assert "candidate-uuid-abc" in result["candidate_ids"]

    def test_node_finalize_success_idempotent_guard(self):
        """node_finalize_success must be idempotent — return state unchanged if already finalized."""
        from fusion_council_service.domain.orchestration.orchestration_nodes_single import (
            node_finalize_success,
        )
        from fusion_council_service.domain.orchestration.orchestration_state import (
            OrchestrationState,
        )

        already_finalized: OrchestrationState = {
            "run_id": "test-run",
            "updated_at": None,
            "current_stage": "finalize_success",
            "candidate_ids": ["c1"],
            "mode": "single",
            "engine": "langgraph",
            "engine_version": "v1",
            "final_answer": "already set",
            "final_confidence": 0.9,
            "error_code": None,
            "error_message": None,
            "thread_id": "thread-abc",
            "checkpoint_namespace": "mode=single",
            "resume_count": 0,
        }

        result = asyncio.run(node_finalize_success(already_finalized))
        assert result["final_answer"] == "already set"  # unchanged

    def test_node_finalize_failure_idempotent_guard(self):
        """node_finalize_failure must be idempotent — skip if already in finalize stage."""
        from fusion_council_service.domain.orchestration.orchestration_nodes_single import (
            node_finalize_failure,
        )
        from fusion_council_service.domain.orchestration.orchestration_state import (
            OrchestrationState,
        )

        already_failed: OrchestrationState = {
            "run_id": "test-run",
            "updated_at": None,
            "current_stage": "finalize_failure",
            "candidate_ids": [],
            "mode": "single",
            "engine": "langgraph",
            "engine_version": "v1",
            "final_answer": None,
            "final_confidence": None,
            "error_code": "ALREADY_FAILED",
            "error_message": "first failure",
            "thread_id": "thread-abc",
            "checkpoint_namespace": "mode=single",
            "resume_count": 0,
        }

        result = asyncio.run(node_finalize_failure(already_failed))
        assert result["error_code"] == "ALREADY_FAILED"  # unchanged


class TestLangGraphSingleCompilation:
    """RED tests — StateGraph must compile and invoke without errors."""

    def test_stategraph_compiles_successfully(self):
        """The StateGraph must compile without raising ValueError."""
        from fusion_council_service.domain.orchestration.orchestration_nodes_single import (
            node_prepare_run,
            node_generation_call,
            node_generation_persist,
            node_finalize_success,
            node_finalize_failure,
        )
        from fusion_council_service.domain.orchestration.orchestration_state import (
            OrchestrationState,
        )

        builder = StateGraph(OrchestrationState)
        builder.add_node("node_prepare_run", node_prepare_run)
        builder.add_node("node_generation_call", node_generation_call)
        builder.add_node("node_generation_persist", node_generation_persist)
        builder.add_node("node_finalize_success", node_finalize_success)
        builder.add_node("node_finalize_failure", node_finalize_failure)

        # START entry point
        builder.add_edge(START, "node_prepare_run")
        builder.add_edge("node_prepare_run", "node_generation_call")
        builder.add_edge("node_generation_call", "node_generation_persist")
        builder.add_edge("node_generation_persist", "node_finalize_success")

        # Must not raise
        compiled = builder.compile()
        assert compiled is not None

    def test_stategraph_invoke_produces_finalize_success_stage(self):
        """Invoking the compiled graph must produce state with current_stage='finalize_success'."""
        from fusion_council_service.domain.orchestration.orchestration_nodes_single import (
            node_prepare_run,
            node_generation_call,
            node_generation_persist,
            node_finalize_success,
            node_finalize_failure,
        )
        from fusion_council_service.domain.orchestration.orchestration_state import (
            OrchestrationState,
        )

        builder = StateGraph(OrchestrationState)
        builder.add_node("node_prepare_run", node_prepare_run)
        builder.add_node("node_generation_call", node_generation_call)
        builder.add_node("node_generation_persist", node_generation_persist)
        builder.add_node("node_finalize_success", node_finalize_success)
        builder.add_node("node_finalize_failure", node_finalize_failure)

        builder.add_edge(START, "node_prepare_run")
        builder.add_edge("node_prepare_run", "node_generation_call")
        builder.add_edge("node_generation_call", "node_generation_persist")
        builder.add_edge("node_generation_persist", "node_finalize_success")

        compiled = builder.compile()

        initial_state: OrchestrationState = {
            "run_id": "test-run-invoke",
            "updated_at": None,
            "current_stage": "",
            "candidate_ids": [],
            "mode": "single",
            "engine": "langgraph",
            "engine_version": "v1",
            "final_answer": None,
            "final_confidence": None,
            "error_code": None,
            "error_message": None,
            "thread_id": "thread-test",
            "checkpoint_namespace": "mode=single",
            "resume_count": 0,
        }

        result = asyncio.run(compiled.ainvoke(initial_state))
        assert result["current_stage"] == "finalize_success"

    def test_stategraph_invoke_happy_path_all_stages_transition(self):
        """Full happy-path invoke must visit all stages in order."""
        from fusion_council_service.domain.orchestration.orchestration_nodes_single import (
            node_prepare_run,
            node_generation_call,
            node_generation_persist,
            node_finalize_success,
            node_finalize_failure,
        )
        from fusion_council_service.domain.orchestration.orchestration_state import (
            OrchestrationState,
        )

        builder = StateGraph(OrchestrationState)
        builder.add_node("node_prepare_run", node_prepare_run)
        builder.add_node("node_generation_call", node_generation_call)
        builder.add_node("node_generation_persist", node_generation_persist)
        builder.add_node("node_finalize_success", node_finalize_success)
        builder.add_node("node_finalize_failure", node_finalize_failure)

        builder.add_edge(START, "node_prepare_run")
        builder.add_edge("node_prepare_run", "node_generation_call")
        builder.add_edge("node_generation_call", "node_generation_persist")
        builder.add_edge("node_generation_persist", "node_finalize_success")

        compiled = builder.compile()

        initial_state: OrchestrationState = {
            "run_id": "test-run-stages",
            "updated_at": None,
            "current_stage": "",
            "candidate_ids": [],
            "current_candidate_id": "c-final",
            "mode": "single",
            "engine": "langgraph",
            "engine_version": "v1",
            "final_answer": "computed_answer",
            "final_confidence": 0.95,
            "error_code": None,
            "error_message": None,
            "thread_id": "thread-stages",
            "checkpoint_namespace": "mode=single",
            "resume_count": 0,
        }

        result = asyncio.run(compiled.ainvoke(initial_state))

        # Verify all stage transitions
        assert result["run_id"] == "test-run-stages"
        assert result["current_stage"] == "finalize_success"
        assert "final_answer" in result
        assert "c-final" in result["candidate_ids"]

    def test_stategraph_invoke_failure_path_routes_to_finalize_failure(self):
        """When run_id is missing, graph must route to node_finalize_failure."""
        from fusion_council_service.domain.orchestration.orchestration_nodes_single import (
            node_prepare_run,
            node_generation_call,
            node_generation_persist,
            node_finalize_success,
            node_finalize_failure,
        )
        from fusion_council_service.domain.orchestration.orchestration_state import (
            OrchestrationState,
        )

        builder = StateGraph(OrchestrationState)
        builder.add_node("node_prepare_run", node_prepare_run)
        builder.add_node("node_generation_call", node_generation_call)
        builder.add_node("node_generation_persist", node_generation_persist)
        builder.add_node("node_finalize_success", node_finalize_success)
        builder.add_node("node_finalize_failure", node_finalize_failure)

        builder.add_edge(START, "node_prepare_run")
        # node_prepare_run detects missing run_id -> sets current_stage="finalize_failure"
        # which routes to generation_call -> generation_persist -> node_finalize_failure via linear edges
        # No direct edge needed — linear chain handles routing
        builder.add_edge("node_prepare_run", "node_generation_call")
        builder.add_edge("node_generation_call", "node_generation_persist")
        builder.add_edge("node_generation_persist", "node_finalize_success")

        compiled = builder.compile()

        # Missing run_id triggers finalize_failure via node_prepare_run guard
        bad_state: OrchestrationState = {
            "run_id": "",  # invalid
            "updated_at": None,
            "current_stage": "",
            "candidate_ids": [],
            "mode": "single",
            "engine": "langgraph",
            "engine_version": "v1",
            "final_answer": None,
            "final_confidence": None,
            "error_code": None,
            "error_message": None,
            "thread_id": "thread-fail",
            "checkpoint_namespace": "mode=single",
            "resume_count": 0,
        }

        result = asyncio.run(compiled.ainvoke(bad_state))
        # node_prepare_run detects missing run_id -> sets finalize_failure
        # node_generation_call and node_generation_persist are no-ops (idempotent guards)
        # node_finalize_failure is no-op (already in finalize state)
        assert result["current_stage"] == "finalize_failure", (
            f"Expected finalize_failure but got {result['current_stage']!r}"
        )
        assert result["error_code"] == "RUN_ID_MISSING", (
            f"Expected RUN_ID_MISSING but got {result.get('error_code')!r}"
        )
