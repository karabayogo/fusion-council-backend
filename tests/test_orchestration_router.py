"""Regression tests for orchestration engine routing and mode gating."""

import pytest


class _FakeEngine:
    def __init__(self):
        self.calls: list[str] = []

    async def run_single(self, db, run: dict, worker_ctx: dict) -> None:
        self.calls.append(f"single:{run['run_id']}")

    async def run_fusion(self, db, run: dict, worker_ctx: dict) -> None:
        self.calls.append(f"fusion:{run['run_id']}")

    async def run_council(self, db, run: dict, worker_ctx: dict) -> None:
        self.calls.append(f"council:{run['run_id']}")


@pytest.mark.asyncio
async def test_router_uses_legacy_engine_for_legacy_mode():
    from fusion_council_service.domain.orchestration.orchestration_engine_router import (
        OrchestrationEngineRouter,
    )

    legacy = _FakeEngine()
    langgraph = _FakeEngine()
    router = OrchestrationEngineRouter(
        orchestrator_engine="legacy",
        langgraph_modes={"single", "fusion", "council"},
        legacy_engine=legacy,
        langgraph_engine=langgraph,
    )

    await router.execute(db=object(), run={"run_id": "r1", "mode": "single"}, worker_ctx={})
    assert legacy.calls == ["single:r1"]
    assert langgraph.calls == []


@pytest.mark.asyncio
async def test_router_langgraph_mode_gating_falls_back_to_legacy():
    from fusion_council_service.domain.orchestration.orchestration_engine_router import (
        OrchestrationEngineRouter,
    )

    legacy = _FakeEngine()
    langgraph = _FakeEngine()
    router = OrchestrationEngineRouter(
        orchestrator_engine="langgraph",
        langgraph_modes={"single"},
        legacy_engine=legacy,
        langgraph_engine=langgraph,
    )

    await router.execute(db=object(), run={"run_id": "r2", "mode": "fusion"}, worker_ctx={})
    assert legacy.calls == ["fusion:r2"]
    assert langgraph.calls == []


@pytest.mark.asyncio
async def test_router_shadow_executes_legacy_then_langgraph():
    from fusion_council_service.domain.orchestration.orchestration_engine_router import (
        OrchestrationEngineRouter,
    )

    legacy = _FakeEngine()
    langgraph = _FakeEngine()
    router = OrchestrationEngineRouter(
        orchestrator_engine="shadow",
        langgraph_modes={"single"},
        legacy_engine=legacy,
        langgraph_engine=langgraph,
    )

    await router.execute(db=object(), run={"run_id": "r3", "mode": "single"}, worker_ctx={})
    assert legacy.calls == ["single:r3"]
    assert langgraph.calls == ["single:r3"]

