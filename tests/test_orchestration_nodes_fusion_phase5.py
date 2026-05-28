"""
Phase 5 RED tests — LangGraph fusion nodes and engine.

Tests are written BEFORE implementation (RED methodology).
These tests define the expected behavior of fusion-mode LangGraph orchestration.
"""
import sys
import unittest
import inspect

# Direct imports from the actual modules — bypass __init__.py which pulls in langgraph
sys.path.insert(0, "src")

from fusion_council_service.domain.orchestration.orchestration_state import OrchestrationState
from fusion_council_service.domain.orchestration.orchestration_nodes_fusion import (
    node_prepare_fusion,
    node_generation_parallel,
    node_synthesis_call,
    node_synthesis_persist,
    node_verification_call,
    node_verification_persist,
    node_finalize_fusion_success,
    node_finalize_fusion_failure,
)
from fusion_council_service.domain.orchestration.orchestration_langgraph_engine import (
    _build_fusion_graph,
    _build_single_graph,
)


def _make_state(overrides: dict) -> OrchestrationState:
    """Build a minimal valid OrchestrationState with field overrides."""
    base = {
        "run_id": "run-123",
        "mode": "fusion",
        "engine": "langgraph",
        "engine_version": "v1",
        "thread_id": "fusion-council:fusion:run-123",
        "checkpoint_namespace": "mode=fusion",
        "resume_count": 0,
        "current_stage": "",
        "candidate_ids": [],
        "current_candidate_id": None,
        "final_answer": None,
        "final_confidence": None,
        "error_code": None,
        "error_message": None,
        "updated_at": None,
        "raw_response": None,
        "candidate_summaries": None,
        "computed_final_answer": None,
        "computed_final_confidence": None,
    }
    base.update(overrides)
    return base  # type: ignore[return-value]


class TestFusionNodePrepareRun(unittest.TestCase):
    """Test node_prepare_fusion behavior."""

    def test_sets_current_stage_to_prepare_fusion(self):
        state = _make_state({"current_stage": ""})
        result = node_prepare_fusion(state)
        self.assertEqual(result["current_stage"], "prepare_fusion")

    def test_idempotent_if_already_at_generation(self):
        state = _make_state({"current_stage": "generation_parallel"})
        result = node_prepare_fusion(state)
        self.assertEqual(result["current_stage"], "generation_parallel")

    def test_idempotent_if_already_at_synthesis(self):
        state = _make_state({"current_stage": "synthesis_call"})
        result = node_prepare_fusion(state)
        self.assertEqual(result["current_stage"], "synthesis_call")

    def test_idempotent_if_already_finalized(self):
        state = _make_state({"current_stage": "finalize_success"})
        result = node_prepare_fusion(state)
        self.assertEqual(result["current_stage"], "finalize_success")

    def test_missing_run_id_sets_finalize_failure(self):
        state = _make_state({"run_id": ""})
        result = node_prepare_fusion(state)
        self.assertEqual(result["current_stage"], "finalize_failure")
        self.assertEqual(result["error_code"], "RUN_ID_MISSING")

    def test_returns_all_orchestration_state_fields(self):
        state = _make_state({})
        result = node_prepare_fusion(state)
        for field in OrchestrationState.__annotations__.keys():
            self.assertIn(field, result, f"Missing field: {field}")


class TestFusionNodeGenerationParallel(unittest.TestCase):
    """Test node_generation_parallel behavior."""

    def test_sets_stage_to_generation_parallel(self):
        state = _make_state({"current_stage": "prepare_fusion"})
        result = node_generation_parallel(state)
        self.assertEqual(result["current_stage"], "generation_parallel")

    def test_idempotent_if_past_generation(self):
        state = _make_state({"current_stage": "synthesis_call"})
        result = node_generation_parallel(state)
        self.assertEqual(result["current_stage"], "synthesis_call")

    def test_idempotent_if_at_verification(self):
        state = _make_state({"current_stage": "verification_call"})
        result = node_generation_parallel(state)
        self.assertEqual(result["current_stage"], "verification_call")

    def test_returns_all_orchestration_state_fields(self):
        state = _make_state({"current_stage": "prepare_fusion"})
        result = node_generation_parallel(state)
        for field in OrchestrationState.__annotations__.keys():
            self.assertIn(field, result, f"Missing field: {field}")


class TestFusionNodeSynthesisCall(unittest.TestCase):
    """Test node_synthesis_call behavior."""

    def test_sets_stage_to_synthesis_call(self):
        state = _make_state({"current_stage": "generation_persist"})
        result = node_synthesis_call(state)
        self.assertEqual(result["current_stage"], "synthesis_call")

    def test_idempotent_if_at_verification(self):
        state = _make_state({"current_stage": "verification_call"})
        result = node_synthesis_call(state)
        self.assertEqual(result["current_stage"], "verification_call")

    def test_idempotent_if_at_finalize(self):
        state = _make_state({"current_stage": "finalize_success"})
        result = node_synthesis_call(state)
        self.assertEqual(result["current_stage"], "finalize_success")


class TestFusionNodeSynthesisPersist(unittest.TestCase):
    """Test node_synthesis_persist behavior."""

    def test_sets_stage_to_synthesis_persist(self):
        state = _make_state({"current_stage": "synthesis_call"})
        result = node_synthesis_persist(state)
        self.assertEqual(result["current_stage"], "synthesis_persist")

    def test_appends_candidate_id(self):
        state = _make_state({
            "current_stage": "synthesis_call",
            "candidate_ids": ["c1", "c2"],
            "current_candidate_id": "c3",
        })
        result = node_synthesis_persist(state)
        self.assertIn("c3", result["candidate_ids"])

    def test_does_not_duplicate_candidate_id(self):
        state = _make_state({
            "current_stage": "synthesis_call",
            "candidate_ids": ["c1", "c2"],
            "current_candidate_id": "c2",
        })
        result = node_synthesis_persist(state)
        self.assertEqual(result["candidate_ids"].count("c2"), 1)

    def test_idempotent_if_at_verification(self):
        state = _make_state({"current_stage": "verification_call"})
        result = node_synthesis_persist(state)
        self.assertEqual(result["current_stage"], "verification_call")


class TestFusionNodeVerificationCall(unittest.TestCase):
    """Test node_verification_call behavior."""

    def test_sets_stage_to_verification_call(self):
        state = _make_state({"current_stage": "synthesis_persist"})
        result = node_verification_call(state)
        self.assertEqual(result["current_stage"], "verification_call")

    def test_idempotent_if_already_finalized(self):
        state = _make_state({"current_stage": "finalize_success"})
        result = node_verification_call(state)
        self.assertEqual(result["current_stage"], "finalize_success")


class TestFusionNodeVerificationPersist(unittest.TestCase):
    """Test node_verification_persist behavior."""

    def test_sets_stage_to_verification_persist(self):
        state = _make_state({"current_stage": "verification_call"})
        result = node_verification_persist(state)
        self.assertEqual(result["current_stage"], "verification_persist")

    def test_idempotent_if_at_finalize(self):
        state = _make_state({"current_stage": "finalize_success"})
        result = node_verification_persist(state)
        self.assertEqual(result["current_stage"], "finalize_success")


class TestFusionNodeFinalizeSuccess(unittest.TestCase):
    """Test node_finalize_fusion_success behavior."""

    def test_copies_computed_final_answer_to_final_answer(self):
        state = _make_state({
            "current_stage": "verification_persist",
            "computed_final_answer": "The fused answer is final.",
            "computed_final_confidence": 0.85,
        })
        result = node_finalize_fusion_success(state)
        self.assertEqual(result["final_answer"], "The fused answer is final.")
        self.assertEqual(result["final_confidence"], 0.85)
        self.assertEqual(result["current_stage"], "finalize_success")

    def test_idempotent_if_already_finalized(self):
        state = _make_state({
            "current_stage": "finalize_success",
            "final_answer": "Already set",
            "final_confidence": 0.9,
        })
        result = node_finalize_fusion_success(state)
        self.assertEqual(result["final_answer"], "Already set")

    def test_defaults_confidence_to_zero_when_none(self):
        state = _make_state({
            "current_stage": "verification_persist",
            "computed_final_answer": "Answer",
            "computed_final_confidence": None,
        })
        result = node_finalize_fusion_success(state)
        self.assertEqual(result["final_confidence"], 0.0)


class TestFusionNodeFinalizeFailure(unittest.TestCase):
    """Test node_finalize_fusion_failure behavior."""

    def test_sets_finalize_failure_stage(self):
        state = _make_state({
            "current_stage": "verification_persist",
            "error_code": "VERIFICATION_FAILED",
            "error_message": "Verification model returned no valid verdict",
        })
        result = node_finalize_fusion_failure(state)
        self.assertEqual(result["current_stage"], "finalize_failure")
        self.assertEqual(result["error_code"], "VERIFICATION_FAILED")

    def test_idempotent_if_already_finalized(self):
        state = _make_state({
            "current_stage": "finalize_failure",
            "error_code": "ALREADY_FAILED",
            "error_message": "Already failed",
        })
        result = node_finalize_fusion_failure(state)
        self.assertEqual(result["error_code"], "ALREADY_FAILED")

    def test_defaults_error_code_to_unknown(self):
        state = _make_state({
            "current_stage": "verification_persist",
            "error_code": None,
        })
        result = node_finalize_fusion_failure(state)
        self.assertEqual(result["error_code"], "UNKNOWN")


class TestFusionGraphStructure(unittest.TestCase):
    """Test that the fusion graph builds correctly."""

    def test_fusion_graph_builds_without_error(self):
        """Building a fusion graph must not raise."""
        graph = _build_fusion_graph()
        self.assertIsNotNone(graph)

    def test_single_graph_still_builds(self):
        """Single-mode graph must still build after fusion addition."""
        graph = _build_single_graph()
        self.assertIsNotNone(graph)


class TestLangGraphEngineHasFusionMethod(unittest.TestCase):
    """Test that LangGraphEngine has run_fusion method."""

    def test_run_fusion_exists_and_is_async(self):
        """LangGraphEngine.run_fusion must exist and be async."""
        from fusion_council_service.domain.orchestration import LangGraphEngine
        engine = LangGraphEngine()
        self.assertTrue(hasattr(engine, "run_fusion"))
        self.assertTrue(callable(getattr(engine, "run_fusion")))
        self.assertTrue(inspect.iscoroutinefunction(engine.run_fusion))


if __name__ == "__main__":
    unittest.main()