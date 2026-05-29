"""
Phase 6 Option A tests — Verify council nodes perform actual work.

Tests verify async nodes with RunnableConfig work correctly.
"""
import asyncio
import unittest
from unittest.mock import Mock

import sys
sys.path.insert(0, "src")

from fusion_council_service.domain.orchestration.orchestration_state import OrchestrationState
from fusion_council_service.domain.orchestration.orchestration_nodes_council import (
    node_prepare_council,
    node_first_opinion_parallel,
    node_peer_review_call,
    node_debate_call,
    node_synthesis_call,
    node_synthesis_persist,
    node_verification_call,
    node_verification_persist,
    node_finalize_council_success,
    node_finalize_council_failure,
    _compute_pairwise_agreement,
)


def _make_state(overrides: dict = None) -> OrchestrationState:
    """Build a minimal valid OrchestrationState."""
    base = {
        "run_id": "run-123",
        "mode": "council",
        "engine": "langgraph",
        "engine_version": "v1",
        "thread_id": "fusion-council:council:run-123",
        "checkpoint_namespace": "mode=council",
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


class TestComputePairwiseAgreement(unittest.TestCase):
    """Test agreement computation."""

    def test_identical_answers_returns_one(self):
        """Identical answers return 1.0."""
        results = [
            {"raw_text": "Answer A", "normalized_answer": "Yes"},
            {"raw_text": "Answer A", "normalized_answer": "Yes"},
        ]
        self.assertEqual(_compute_pairwise_agreement(results), 1.0)

    def test_different_answers_returns_low_value(self):
        """Different answers return low value."""
        results = [
            {"raw_text": "Answer A", "normalized_answer": "Yes"},
            {"raw_text": "Answer B", "normalized_answer": "No"},
        ]
        value = _compute_pairwise_agreement(results)
        self.assertLess(value, 1.0)

    def test_empty_returns_one(self):
        """Empty list returns 1.0."""
        self.assertEqual(_compute_pairwise_agreement([]), 1.0)

    def test_single_returns_one(self):
        """Single item returns 1.0."""
        self.assertEqual(_compute_pairwise_agreement([{"raw_text": "A"}]), 1.0)


class TestNodePrepareCouncil(unittest.IsolatedAsyncioTestCase):
    """Test prepare council node."""

    async def test_sets_stage_to_prepare_council(self):
        """Verify stage is set to prepare_council."""
        state = _make_state({"current_stage": ""})
        result = await node_prepare_council(state)
        self.assertEqual(result["current_stage"], "prepare_council")

    async def test_idempotent_if_past_prepare(self):
        """Verify idempotency."""
        state = _make_state({"current_stage": "first_opinion_parallel"})
        result = await node_prepare_council(state)
        self.assertEqual(result["current_stage"], "first_opinion_parallel")


class TestNodeFirstOpinionIdempotency(unittest.IsolatedAsyncioTestCase):
    """Test first opinion node idempotency."""

    async def test_idempotent_if_past(self):
        """Verify past first opinion skips."""
        state = _make_state({"current_stage": "peer_review_call"})
        config = _make_config(Mock())
        result = await node_first_opinion_parallel(state, config)
        self.assertEqual(result["current_stage"], "peer_review_call")

    async def test_graceful_degradation_no_worker(self):
        """Verify graceful degradation when no worker."""
        state = _make_state({"current_stage": "prepare_council"})
        result = await node_first_opinion_parallel(state)
        self.assertEqual(result["current_stage"], "first_opinion_parallel")


class TestNodePeerReviewSkipLogic(unittest.IsolatedAsyncioTestCase):
    """Test peer review skip logic."""

    async def test_skips_on_high_agreement(self):
        """Verify skips on high agreement (>0.55)."""
        # Create results with high agreement (same first 50 chars)
        state = _make_state({
            "current_stage": "first_opinion_persist",
            "candidate_results": [
                {"raw_text": "This is a very long answer text " * 10},
                {"raw_text": "This is a very long answer text " * 10},
            ],
        })
        config = _make_config(Mock())
        result = await node_peer_review_call(state, config)
        self.assertEqual(result["current_stage"], "peer_review_skip")

    async def test_skips_on_degradation_flag(self):
        """Verify skips on degradation flag."""
        state = _make_state({
            "current_stage": "first_opinion_persist",
            "degradation": "skip_peer",
        })
        config = _make_config(Mock())
        result = await node_peer_review_call(state, config)
        self.assertEqual(result["current_stage"], "peer_review_skip")


class TestNodeDebateSkipLogic(unittest.IsolatedAsyncioTestCase):
    """Test debate skip logic."""

    async def test_skips_on_high_agreement(self):
        """Verify skips on high agreement."""
        state = _make_state({
            "current_stage": "peer_review_persist",
            "candidate_results": [
                {"raw_text": "This is a very long answer text " * 10},
                {"raw_text": "This is a very long answer text " * 10},
            ],
        })
        config = _make_config(Mock())
        result = await node_debate_call(state, config)
        self.assertEqual(result["current_stage"], "debate_skip")


class TestNodeSynthesisIdempotency(unittest.IsolatedAsyncioTestCase):
    """Test synthesis node idempotency."""

    async def test_idempotent_past_synthesis(self):
        """Verify idempotency from later stages."""
        state = _make_state({"current_stage": "verification_call"})
        config = _make_config(Mock())
        result = await node_synthesis_call(state, config)
        self.assertEqual(result["current_stage"], "verification_call")


class TestNodeVerificationIdempotency(unittest.IsolatedAsyncioTestCase):
    """Test verification node idempotency."""

    async def test_idempotent_from_finalize(self):
        """Verify idempotency from finalize."""
        state = _make_state({"current_stage": "finalize_success"})
        config = _make_config(Mock())
        result = await node_verification_call(state, config)
        self.assertEqual(result["current_stage"], "finalize_success")


class TestNodeFinalizeCouncilSuccess(unittest.IsolatedAsyncioTestCase):
    """Test finalize success node."""

    async def test_copies_computed_answer(self):
        """Verify computed answer is copied."""
        state = _make_state({
            "current_stage": "verification_persist",
            "computed_final_answer": "Council answer",
            "computed_final_confidence": 0.9,
        })
        config = _make_config()
        result = await node_finalize_council_success(state, config)
        self.assertEqual(result["final_answer"], "Council answer")
        self.assertEqual(result["final_confidence"], 0.9)

    async def test_defaults_confidence(self):
        """Verify defaults to 0.0."""
        state = _make_state({
            "current_stage": "verification_persist",
            "computed_final_answer": "Answer",
            "computed_final_confidence": None,
        })
        config = _make_config()
        result = await node_finalize_council_success(state, config)
        self.assertEqual(result["final_confidence"], 0.0)

    async def test_idempotent(self):
        """Verify idempotency."""
        state = _make_state({
            "current_stage": "finalize_success",
            "final_answer": "Done",
        })
        config = _make_config()
        result = await node_finalize_council_success(state, config)
        self.assertEqual(result["final_answer"], "Done")


class TestNodeFinalizeCouncilFailure(unittest.IsolatedAsyncioTestCase):
    """Test finalize failure node."""

    async def test_sets_error_state(self):
        """Verify error state is set."""
        state = _make_state({
            "current_stage": "synthesis_call",
            "error_code": "FAILED",
            "error_message": "Error msg",
        })
        config = _make_config()
        result = await node_finalize_council_failure(state, config)
        self.assertEqual(result["current_stage"], "finalize_failure")
        self.assertEqual(result["error_code"], "FAILED")

    async def test_defaults_unknown(self):
        """Verify defaults to UNKNOWN."""
        state = _make_state({"current_stage": "synthesis_call"})
        config = _make_config()
        result = await node_finalize_council_failure(state, config)
        self.assertEqual(result["error_code"], "UNKNOWN")

    async def test_idempotent(self):
        """Verify idempotency."""
        state = _make_state({
            "current_stage": "finalize_failure",
            "error_code": "DONE",
        })
        config = _make_config()
        result = await node_finalize_council_failure(state, config)
        self.assertEqual(result["error_code"], "DONE")


if __name__ == "__main__":
    unittest.main()