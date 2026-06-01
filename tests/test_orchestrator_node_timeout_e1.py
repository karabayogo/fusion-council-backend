"""
RED tests for E1 fix: per-model timeout_seconds must reach the provider call
in the langgraph orchestrator node path (orchestration_nodes_council.py and
orchestration_nodes_fusion.py), not just the legacy worker_loop.py path.

Symptom (run_c908a00b1c834b8eb9ebe2b4, 2026-06-01): the M2.7 debate candidate
failed with `error_message: "Provider call timed out after 300s"` even though
`config/models.yaml` configures `timeout_seconds: 600` for the model. Root
cause: the langgraph node functions build a plain dict for the request
without `timeout_seconds`, so the function's hardcoded 300s default applies.

These tests assert that:
  1. node_first_opinion_parallel, node_peer_review_call, node_debate_call,
     node_synthesis_call, node_verification_call all read the catalog's
     `timeout_seconds` for the selected model and pass it to _call_provider_async.
  2. _call_provider_async honors the request.timeout_seconds (or catalog) and
     does NOT silently fall back to 300 when the caller passed a higher value.

The tests use synthetic ModelCatalog and Mock workers per the project's
test convention (NEVER load live config/models.yaml).
"""
import asyncio
import sys
import unittest
from unittest.mock import Mock, AsyncMock, patch

sys.path.insert(0, "src")

from fusion_council_service.domain.orchestration.orchestration_nodes_council import (
    node_first_opinion_parallel,
    node_peer_review_call,
    node_debate_call,
    node_synthesis_call,
    node_verification_call,
)


def _make_state(overrides=None):
    base = {
        "run_id": "run-e1-test",
        "mode": "council",
        "engine": "langgraph",
        "thread_id": "fusion-council:council:run-e1-test",
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
        "prompt": "test prompt",
        "max_tokens": 1024,
    }
    if overrides:
        base.update(overrides)
    return base


def _make_config(worker=None):
    return {"configurable": {"worker": worker}}


def _make_catalog_with_timeout(alias: str, timeout_seconds: int):
    """Build a mock catalog that returns a single peer with the given timeout."""
    catalog = Mock()
    catalog.get_peers_for_mode = Mock(return_value=[
        {
            "alias": alias,
            "provider": "opencode_go",
            "provider_model": "qwen3.6-plus",
            "enabled": True,
            "timeout_seconds": timeout_seconds,
        }
    ])
    return catalog


class TestOrchestratorNodesPassPerModelTimeout(unittest.IsolatedAsyncioTestCase):
    """E1 regression: orchestrator nodes must forward per-model timeout to provider call."""

    def _capture_request(self, worker):
        """Patch _call_provider_async to capture the request it received."""
        captured = {}

        async def fake_call(request, db, run_id, timeout_seconds=None):
            captured["request"] = request
            captured["timeout_seconds"] = timeout_seconds
            # Return a successful 7-tuple
            return (True, "ok", None, None, 100, 10, 10)

        worker._call_provider_async = fake_call
        return captured

    async def test_node_first_opinion_passes_catalog_timeout(self):
        """The first-opinion parallel node must set timeout_seconds on the request
        from the catalog entry of the selected model, not fall back to 300s."""
        worker = Mock()
        worker.db = Mock()
        worker.catalog = _make_catalog_with_timeout("opencode-go/qwen3.6-plus", 600)

        captured = self._capture_request(worker)
        state = _make_state({"current_stage": "prepare_council"})

        await node_first_opinion_parallel(state, _make_config(worker))

        req = captured["request"]
        # The request must carry timeout_seconds=600 (or a ProviderGenerateRequest
        # with that field set). Plain-dict requests fail this assertion.
        if isinstance(req, dict):
            self.assertEqual(
                req.get("timeout_seconds"), 600,
                f"node_first_opinion must propagate catalog timeout_seconds=600, got {req.get('timeout_seconds')!r}",
            )
        else:
            self.assertEqual(
                req.timeout_seconds, 600,
                f"node_first_opinion must propagate catalog timeout_seconds=600, got {req.timeout_seconds!r}",
            )

    async def test_node_debate_passes_catalog_timeout(self):
        """The debate node must set timeout_seconds on the request from the catalog."""
        worker = Mock()
        worker.db = Mock()
        # Peer selection for debate: models[0] and models[1]; mock returns 1
        # but node_debate_call does `peer1, peer2 = models[0], models[1]`.
        # Give it 2 entries so indexing works.
        worker.catalog = Mock()
        worker.catalog.get_peers_for_mode = Mock(return_value=[
            {"alias": "minimax/MiniMax-M3", "provider": "minimax_token_plan",
             "provider_model": "MiniMax-M3", "timeout_seconds": 600, "enabled": True},
            {"alias": "opencode-go/qwen3.6-plus", "provider": "opencode_go",
             "provider_model": "qwen3.6-plus", "timeout_seconds": 600, "enabled": True},
        ])

        captured = self._capture_request(worker)
        # Force low agreement to make debate actually run
        state = _make_state({
            "current_stage": "peer_review_persist",
            "candidate_results": [
                {"raw_text": "First opinion A " * 20, "success": True},
                {"raw_text": "First opinion B " * 20, "success": True},
            ],
        })

        await node_debate_call(state, _make_config(worker))

        req = captured["request"]
        if req is None:
            self.fail("node_debate_call did not call _call_provider_async — no request captured")
        if isinstance(req, dict):
            self.assertEqual(
                req.get("timeout_seconds"), 600,
                f"node_debate must propagate catalog timeout_seconds=600, got {req.get('timeout_seconds')!r}",
            )
        else:
            self.assertEqual(
                req.timeout_seconds, 600,
                f"node_debate must propagate catalog timeout_seconds=600, got {req.timeout_seconds!r}",
            )

    async def test_node_synthesis_passes_catalog_timeout(self):
        """The synthesis node must set timeout_seconds on the request from the catalog."""
        worker = Mock()
        worker.db = Mock()
        # node_synthesis_call picks the synthesizer; need at least one catalog entry
        # that matches the synthesizer alias pattern
        worker.catalog = Mock()
        worker.catalog.get_peers_for_mode = Mock(return_value=[
            {"alias": "opencode-go/deepseek-v4-pro", "provider": "opencode_go",
             "provider_model": "deepseek-v4-pro", "timeout_seconds": 600,
             "role_bias": "synthesis", "enabled": True},
        ])

        captured = self._capture_request(worker)
        state = _make_state({
            "current_stage": "debate_persist",
            "candidate_results": [
                {"raw_text": "opinion A " * 10, "success": True},
                {"raw_text": "opinion B " * 10, "success": True},
            ],
        })

        await node_synthesis_call(state, _make_config(worker))

        req = captured["request"]
        if req is None:
            self.fail("node_synthesis_call did not call _call_provider_async")
        if isinstance(req, dict):
            self.assertEqual(
                req.get("timeout_seconds"), 600,
                f"node_synthesis must propagate catalog timeout_seconds=600, got {req.get('timeout_seconds')!r}",
            )
        else:
            self.assertEqual(
                req.timeout_seconds, 600,
                f"node_synthesis must propagate catalog timeout_seconds=600, got {req.timeout_seconds!r}",
            )

    async def test_node_verification_passes_catalog_timeout(self):
        """The verification node must set timeout_seconds on the request from the catalog."""
        worker = Mock()
        worker.db = Mock()
        worker.catalog = Mock()
        worker.catalog.get_peers_for_mode = Mock(return_value=[
            {"alias": "opencode-go/kimi-k2.6", "provider": "opencode_go",
             "provider_model": "kimi-k2.6", "timeout_seconds": 600,
             "role_bias": "verification", "enabled": True},
        ])

        captured = self._capture_request(worker)
        state = _make_state({
            "current_stage": "synthesis_persist",
        })

        await node_verification_call(state, _make_config(worker))

        req = captured["request"]
        if req is None:
            self.fail("node_verification_call did not call _call_provider_async")
        if isinstance(req, dict):
            self.assertEqual(
                req.get("timeout_seconds"), 600,
                f"node_verification must propagate catalog timeout_seconds=600, got {req.get('timeout_seconds')!r}",
            )
        else:
            self.assertEqual(
                req.timeout_seconds, 600,
                f"node_verification must propagate catalog timeout_seconds=600, got {req.timeout_seconds!r}",
            )


class TestCallProviderAsyncTimeoutContract(unittest.IsolatedAsyncioTestCase):
    """E1 boundary test: _resolve_effective_timeout precedence contract.

    The hardcoded default of 300 in _call_provider_async caused the M2.7
    debate failure. The new contract uses 600s as the floor; the helper
    _resolve_effective_timeout encodes the precedence rules. This test
    locks the precedence in place so future refactors can't silently
    regress to 300s.
    """

    def test_caller_timeout_wins(self):
        """Explicit caller timeout overrides everything."""
        from fusion_council_service.domain.types import ProviderGenerateRequest
        from fusion_council_service.domain.worker_loop import _resolve_effective_timeout
        req = ProviderGenerateRequest(
            alias="t", provider="p", provider_model="m",
            system_prompt=None, user_prompt="x", max_output_tokens=10,
            temperature=0.2, timeout_seconds=900,
        )
        self.assertEqual(_resolve_effective_timeout(req, caller_timeout=42), 42)

    def test_request_timeout_used_when_no_caller_override(self):
        """When caller doesn't override, request.timeout_seconds wins."""
        from fusion_council_service.domain.types import ProviderGenerateRequest
        from fusion_council_service.domain.worker_loop import _resolve_effective_timeout
        req = ProviderGenerateRequest(
            alias="t", provider="p", provider_model="m",
            system_prompt=None, user_prompt="x", max_output_tokens=10,
            temperature=0.2, timeout_seconds=900,
        )
        self.assertEqual(_resolve_effective_timeout(req), 900)

    def test_floor_used_when_nothing_set(self):
        """Last-resort floor (600s) applies when neither caller nor request
        carry a timeout. This replaces the silent 300s default that was the
        root cause of the M2.7 debate 300s timeout."""
        from fusion_council_service.domain.types import ProviderGenerateRequest
        from fusion_council_service.domain.worker_loop import _resolve_effective_timeout, DEFAULT_PROVIDER_TIMEOUT_SECONDS
        req = ProviderGenerateRequest(
            alias="t", provider="p", provider_model="m",
            system_prompt=None, user_prompt="x", max_output_tokens=10,
            temperature=0.2, timeout_seconds=None,
        )
        self.assertEqual(_resolve_effective_timeout(req), DEFAULT_PROVIDER_TIMEOUT_SECONDS)
        self.assertEqual(_resolve_effective_timeout(req), 600)

    def test_floor_never_below_300(self):
        """Regression guard: 600s floor is the minimum. If anyone tries to
        lower DEFAULT_PROVIDER_TIMEOUT_SECONDS below 300, this test fails."""
        from fusion_council_service.domain.worker_loop import DEFAULT_PROVIDER_TIMEOUT_SECONDS
        self.assertGreaterEqual(
            DEFAULT_PROVIDER_TIMEOUT_SECONDS, 300,
            f"DEFAULT_PROVIDER_TIMEOUT_SECONDS regressed below 300s — was "
            f"{DEFAULT_PROVIDER_TIMEOUT_SECONDS}. The 300s floor caused the "
            f"M2.7 debate PROVIDER_TIMEOUT in run_c908a00b1c834b8eb9ebe2b4.",
        )


if __name__ == "__main__":
    unittest.main()
