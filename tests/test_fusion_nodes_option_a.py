"""
Phase 5 Option A tests — Verify fusion nodes perform actual work.

Tests verify async nodes with RunnableConfig work correctly.
"""
import asyncio
import unittest
from unittest.mock import Mock

# Direct imports
import sys
sys.path.insert(0, "src")

from fusion_council_service.domain.orchestration.orchestration_state import OrchestrationState
from fusion_council_service.domain.orchestration.orchestration_nodes_fusion import (
    node_prepare_fusion,
    node_generation_parallel,
    node_generation_persist,
    node_synthesis_call,
    node_synthesis_persist,
    node_verification_call,
    node_verification_persist,
    node_finalize_fusion_success,
    node_finalize_fusion_failure,
)


def _make_state(overrides: dict = None) -> OrchestrationState:
    """Build a minimal valid OrchestrationState."""
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
    if overrides:
        base.update(overrides)
    return base  # type: ignore[return-value]


def _make_config(worker=None):
    """Make a RunnableConfig dict."""
    return {"configurable": {"worker": worker}}


class TestNodePrepareFusionIdempotency(unittest.IsolatedAsyncioTestCase):
    """Test node_prepare_fusion idempotency guards."""

    async def test_sets_stage_to_prepare_fusion(self):
        """Verify stage is set to prepare_fusion when starting."""
        state = _make_state({"current_stage": ""})
        result = await node_prepare_fusion(state)
        self.assertEqual(result["current_stage"], "prepare_fusion")

    async def test_idempotent_if_already_prepared(self):
        """Verify idempotency - doesn't go back to prepare from later stage."""
        state = _make_state({"current_stage": "prepare_fusion"})
        result = await node_prepare_fusion(state)
        self.assertEqual(result["current_stage"], "prepare_fusion")

    async def test_idempotent_past_prepare(self):
        """Verify idempotency - doesn't go back to prepare if past it."""
        state = _make_state({"current_stage": "generation_parallel"})
        result = await node_prepare_fusion(state)
        self.assertEqual(result["current_stage"], "generation_parallel")


class TestNodeGenerationParallelIdempotency(unittest.IsolatedAsyncioTestCase):
    """Test node_generation_parallel idempotency guards."""

    async def test_idempotent_if_past_generation(self):
        """Verify idempotency - doesn't go back to generation from synthesis."""
        state = _make_state({"current_stage": "synthesis_call"})
        config = _make_config(Mock())
        result = await node_generation_parallel(state, config)
        self.assertEqual(result["current_stage"], "synthesis_call")

    async def test_idempotent_if_at_verification(self):
        """Verify idempotency from verification stage."""
        state = _make_state({"current_stage": "verification_call"})
        config = _make_config(Mock())
        result = await node_generation_parallel(state, config)
        self.assertEqual(result["current_stage"], "verification_call")


class TestNodeSynthesisCallIdempotency(unittest.IsolatedAsyncioTestCase):
    """Test node_synthesis_call idempotency guards."""

    async def test_idempotent_if_at_verification(self):
        """Verify doesn't go back to synthesis from verification."""
        state = _make_state({"current_stage": "verification_call"})
        config = _make_config(Mock())
        result = await node_synthesis_call(state, config)
        self.assertEqual(result["current_stage"], "verification_call")

    async def test_idempotent_if_at_finalize(self):
        """Verify doesn't go back to synthesis from finalize."""
        state = _make_state({"current_stage": "finalize_success"})
        config = _make_config(Mock())
        result = await node_synthesis_call(state, config)
        self.assertEqual(result["current_stage"], "finalize_success")


class TestNodeSynthesisPersistIdempotency(unittest.IsolatedAsyncioTestCase):
    """Test node_synthesis_persist idempotency guards."""

    async def test_idempotent_if_at_verification(self):
        """Verify idempotency from verification."""
        state = _make_state({"current_stage": "verification_persist"})
        config = _make_config(Mock())
        result = await node_synthesis_persist(state, config)
        self.assertEqual(result["current_stage"], "verification_persist")

    async def test_appends_candidate_id(self):
        """Verify candidate ID is appended."""
        state = _make_state({
            "current_stage": "synthesis_call",
            "current_candidate_id": "cand-new",
            "candidate_ids": ["cand-old"],
        })
        config = _make_config(Mock())
        result = await node_synthesis_persist(state, config)
        self.assertIn("cand-new", result["candidate_ids"])

    async def test_no_duplicate_candidate_id(self):
        """Verify no duplicate when ID already in list."""
        state = _make_state({
            "current_stage": "synthesis_persist",
            "current_candidate_id": "cand-existing",
            "candidate_ids": ["cand-existing"],
        })
        config = _make_config(Mock())
        result = await node_synthesis_persist(state, config)
        self.assertEqual(result["candidate_ids"], ["cand-existing"])


class TestNodeVerificationIdempotency(unittest.IsolatedAsyncioTestCase):
    """Test verification node idempotency."""

    async def test_idempotent_if_already_finalized(self):
        """Verify idempotency from finalize stages."""
        state = _make_state({"current_stage": "finalize_success"})
        config = _make_config(Mock())
        result = await node_verification_call(state, config)
        self.assertEqual(result["current_stage"], "finalize_success")


class TestNodeFinalizeFusionSuccess(unittest.IsolatedAsyncioTestCase):
    """Test finalize success node."""

    async def test_copies_computed_final_answer(self):
        """Verify computed answer is copied to final_answer."""
        state = _make_state({
            "current_stage": "verification_persist",
            "computed_final_answer": "Final answer",
            "computed_final_confidence": 0.85,
        })
        config = _make_config()
        result = await node_finalize_fusion_success(state, config)
        self.assertEqual(result["final_answer"], "Final answer")
        self.assertEqual(result["final_confidence"], 0.85)
        self.assertEqual(result["current_stage"], "finalize_success")

    async def test_defaults_confidence_to_zero_when_none(self):
        """Verify confidence defaults to 0.0 when None."""
        state = _make_state({
            "current_stage": "verification_persist",
            "computed_final_answer": "Answer",
            "computed_final_confidence": None,
        })
        config = _make_config()
        result = await node_finalize_fusion_success(state, config)
        self.assertEqual(result["final_confidence"], 0.0)

    async def test_idempotent_if_already_finalized(self):
        """Verify idempotency - doesn't re-finalize."""
        state = _make_state({
            "current_stage": "finalize_success",
            "final_answer": "Already done",
        })
        config = _make_config()
        result = await node_finalize_fusion_success(state, config)
        self.assertEqual(result["final_answer"], "Already done")


class TestNodeFinalizeFusionFailure(unittest.IsolatedAsyncioTestCase):
    """Test finalize failure node."""

    async def test_sets_finalize_failure_stage(self):
        """Verify stage is set to finalize_failure."""
        state = _make_state({
            "current_stage": "synthesis_call",
            "error_code": "FAILED",
            "error_message": "Error message",
        })
        config = _make_config()
        result = await node_finalize_fusion_failure(state, config)
        self.assertEqual(result["current_stage"], "finalize_failure")
        self.assertEqual(result["error_code"], "FAILED")
        self.assertEqual(result["error_message"], "Error message")

    async def test_defaults_error_code_to_unknown(self):
        """Verify defaults to UNKNOWN when no error_code."""
        state = _make_state({"current_stage": "synthesis_call"})
        config = _make_config()
        result = await node_finalize_fusion_failure(state, config)
        self.assertEqual(result["error_code"], "UNKNOWN")

    async def test_idempotent_if_already_finalized(self):
        """Verify idempotency."""
        state = _make_state({
            "current_stage": "finalize_failure",
            "error_code": "ALREADY_DONE",
        })
        config = _make_config()
        result = await node_finalize_fusion_failure(state, config)
        self.assertEqual(result["error_code"], "ALREADY_DONE")


if __name__ == "__main__":
    unittest.main()