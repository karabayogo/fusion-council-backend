"""Regression tests for W1 — Catalog/Health Reconciliation.

The helper `reconcile_provider_health_with_catalog(db, catalog)` deletes any
provider_health row whose `(provider, provider_model)` is not in the current
enabled model catalog. It is the W1 startup hook.

Before W1 lands, this helper does not exist — so the import test fails
(RED). After W1 lands, the four core tests should pass.
"""

from __future__ import annotations

import sqlite3

import pytest

from fusion_council_service.db import execute_sql
from fusion_council_service.domain.model_selection import (
    reconcile_provider_health_with_catalog,
)


def _seed_provider_health_row(
    db: sqlite3.Connection,
    provider: str,
    provider_model: str,
    health_score: float = 0.5,
) -> None:
    """Insert a minimal provider_health row for tests."""
    execute_sql(
        db,
        """
        INSERT INTO provider_health
            (provider, provider_model, total_attempts, successes, failures,
             last_failure_at, last_success_at, avg_latency_ms, health_score, updated_at)
        VALUES
            (:provider, :provider_model, 1, 1, 0, NULL, '2026-06-02T00:00:00Z', 100,
             :health_score, '2026-06-02T00:00:00Z')
        """,
        {"provider": provider, "provider_model": provider_model, "health_score": health_score},
    )


def _all_pairs(db: sqlite3.Connection) -> set[tuple[str, str]]:
    """Read the (provider, provider_model) pairs currently in provider_health."""
    from fusion_council_service.db import execute_sql_all

    rows = execute_sql_all(db, "SELECT provider, provider_model FROM provider_health")
    return {(r["provider"], r["provider_model"]) for r in rows}


class _SyntheticCatalog:
    """Minimal stand-in for ModelCatalog that exposes enabled_models()."""

    def __init__(self, models: list[dict]) -> None:
        self._models = models

    def enabled_models(self) -> list[dict]:
        return [m for m in self._models if m.get("enabled", True)]


def test_reconcile_deletes_stale_pair(tmp_db: sqlite3.Connection) -> None:
    """M2.7 row must be deleted when M2.7 is not in the enabled catalog."""
    _seed_provider_health_row(tmp_db, "minimax_token_plan", "MiniMax-M2.7", health_score=0.07)
    _seed_provider_health_row(tmp_db, "minimax_token_plan", "MiniMax-M3", health_score=0.66)

    catalog = _SyntheticCatalog(
        models=[
            {"provider": "minimax_token_plan", "provider_model": "MiniMax-M3", "enabled": True},
        ]
    )
    deleted = reconcile_provider_health_with_catalog(tmp_db, catalog)

    assert deleted == 1, f"expected 1 deletion (M2.7), got {deleted}"
    remaining = _all_pairs(tmp_db)
    assert ("minimax_token_plan", "MiniMax-M2.7") not in remaining
    assert ("minimax_token_plan", "MiniMax-M3") in remaining


def test_reconcile_preserves_all_in_catalog_pairs(tmp_db: sqlite3.Connection) -> None:
    """No deletions when all rows are in the catalog."""
    _seed_provider_health_row(tmp_db, "opencode_go", "qwen3.6-plus", health_score=1.0)
    _seed_provider_health_row(tmp_db, "opencode_go", "kimi-k2.6", health_score=0.93)

    catalog = _SyntheticCatalog(
        models=[
            {"provider": "opencode_go", "provider_model": "qwen3.6-plus", "enabled": True},
            {"provider": "opencode_go", "provider_model": "kimi-k2.6", "enabled": True},
        ]
    )
    deleted = reconcile_provider_health_with_catalog(tmp_db, catalog)

    assert deleted == 0
    assert len(_all_pairs(tmp_db)) == 2


def test_reconcile_is_idempotent(tmp_db: sqlite3.Connection) -> None:
    """Re-running on a clean DB returns 0."""
    _seed_provider_health_row(tmp_db, "minimax_token_plan", "MiniMax-M2.7", health_score=0.07)

    catalog = _SyntheticCatalog(
        models=[
            {"provider": "minimax_token_plan", "provider_model": "MiniMax-M3", "enabled": True},
        ]
    )
    first = reconcile_provider_health_with_catalog(tmp_db, catalog)
    second = reconcile_provider_health_with_catalog(tmp_db, catalog)
    third = reconcile_provider_health_with_catalog(tmp_db, catalog)

    assert first == 1
    assert second == 0
    assert third == 0


def test_reconcile_handles_empty_provider_health(tmp_db: sqlite3.Connection) -> None:
    """Empty provider_health is a no-op."""
    catalog = _SyntheticCatalog(
        models=[
            {"provider": "opencode_go", "provider_model": "qwen3.6-plus", "enabled": True},
        ]
    )
    deleted = reconcile_provider_health_with_catalog(tmp_db, catalog)
    assert deleted == 0
    assert len(_all_pairs(tmp_db)) == 0


def test_reconcile_handles_empty_catalog(tmp_db: sqlite3.Connection) -> None:
    """Empty catalog deletes everything in provider_health."""
    _seed_provider_health_row(tmp_db, "opencode_go", "qwen3.6-plus", health_score=1.0)
    _seed_provider_health_row(tmp_db, "minimax_token_plan", "MiniMax-M3", health_score=0.66)

    catalog = _SyntheticCatalog(models=[])
    deleted = reconcile_provider_health_with_catalog(tmp_db, catalog)
    assert deleted == 2
    assert len(_all_pairs(tmp_db)) == 0


def test_reconcile_only_uses_enabled_models(tmp_db: sqlite3.Connection) -> None:
    """A disabled model in the catalog should NOT count as valid (delete it)."""
    _seed_provider_health_row(tmp_db, "opencode_go", "qwen3.7-max", health_score=0.0)

    catalog = _SyntheticCatalog(
        models=[
            {"provider": "opencode_go", "provider_model": "qwen3.7-max", "enabled": False},
        ]
    )
    deleted = reconcile_provider_health_with_catalog(tmp_db, catalog)
    assert deleted == 1
    assert len(_all_pairs(tmp_db)) == 0
