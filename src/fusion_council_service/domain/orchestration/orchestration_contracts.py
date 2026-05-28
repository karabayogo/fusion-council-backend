"""Contracts for orchestration engines."""

from typing import Any, Protocol


class OrchestrationEngine(Protocol):
    async def run_single(self, db: Any, run: dict, worker_ctx: dict) -> None: ...

    async def run_fusion(self, db: Any, run: dict, worker_ctx: dict) -> None: ...

    async def run_council(self, db: Any, run: dict, worker_ctx: dict) -> None: ...

