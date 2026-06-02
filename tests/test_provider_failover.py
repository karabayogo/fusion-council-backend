"""Regression tests for provider failover and catalog property fixes.

These tests lock in the fixes for run_14898b9d836340a2a6c50bf0:
- RCA-1: Missing @property catalog on Worker class (LangGraph crash)
- RCA-3: _try_fallback doesn't skip failed providers (burns deadline budget)
"""

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from fusion_council_service.domain.worker_loop import Worker
from fusion_council_service.model_catalog import ModelCatalog, load_yaml_catalog
from fusion_council_service.domain.candidate_repository import insert_candidate
from fusion_council_service.clock import utc_now_iso


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def real_catalog():
    """Load the real model catalog from config/models.yaml."""
    catalog = load_yaml_catalog("config/models.yaml")
    return ModelCatalog(catalog)


@pytest.fixture
def worker_with_catalog(real_catalog):
    """Worker with real catalog, in-memory DB, mock registry."""
    mock_registry = MagicMock()
    worker = Worker(
        db_path=":memory:",
        registry=mock_registry,
        catalog=real_catalog,
        poll_interval_ms=50,
        heartbeat_interval_ms=5000,
        stale_run_threshold_seconds=30,
    )
    # Initialize the in-memory DB
    worker._db = worker._get_db()
    return worker


# ---------------------------------------------------------------------------
# RCA-1: Worker.catalog property must exist for LangGraph nodes
# ---------------------------------------------------------------------------

def test_worker_catalog_property_exists(worker_with_catalog, real_catalog):
    """Worker must expose .catalog as a public property.

    Without this, LangGraph nodes crash with:
    AttributeError: 'Worker' object has no attribute 'catalog'
    (root cause of PEER_CATALOG_ERROR in run_14898b9d836340a2a6c50bf0)
    """
    # Must not raise AttributeError
    assert hasattr(worker_with_catalog, "catalog")
    # Must return the same ModelCatalog instance
    assert worker_with_catalog.catalog is real_catalog


def test_worker_catalog_property_returns_catalog(worker_with_catalog):
    """Worker.catalog must return a ModelCatalog, not None or a dict."""
    catalog = worker_with_catalog.catalog
    assert isinstance(catalog, ModelCatalog)
    # Must have enabled models
    assert len(list(catalog.enabled_models())) > 0


# ---------------------------------------------------------------------------
# RCA-3: _failed_providers detects systemic provider outages
# ---------------------------------------------------------------------------

def test_failed_providers_empty_when_no_failures(worker_with_catalog):
    """When no candidates have failed, _failed_providers returns empty set."""
    db = worker_with_catalog._db
    # No candidates inserted yet
    failed = worker_with_catalog._failed_providers(db, "nonexistent_run")
    assert failed == set()


def _ensure_run_exists(db, run_id):
    """Insert a minimal run record so candidate FK constraints pass."""
    from fusion_council_service.domain.run_repository import insert_run
    from fusion_council_service.clock import utc_now_plus_seconds
    try:
        insert_run(
            db=db, run_id=run_id, mode="council", prompt="test",
            system_prompt=None, temperature=0.2, max_output_tokens=1000,
            deadline_seconds=900, deadline_at=utc_now_plus_seconds(900),
            owner_token_hash="hash", metadata_json="{}", requested_models_json=None,
            created_at=utc_now_iso(),
        )
    except Exception:
        pass  # run may already exist


def test_failed_providers_detects_fully_down_provider(worker_with_catalog):
    """When ALL models from a provider fail, that provider is detected."""
    db = worker_with_catalog._db
    run_id = "test_failed_provider"
    _ensure_run_exists(db, run_id)

    # Insert failed candidates for ALL opencode_go models
    catalog = worker_with_catalog.catalog
    for model in catalog.enabled_models():
        if model.get("provider") == "opencode_go":
            insert_candidate(
                db, run_id, f"cand_{model['alias']}", model["alias"],
                model["provider"], model["provider_model"],
                "first_opinion", "failed", utc_now_iso(),
            )

    failed = worker_with_catalog._failed_providers(db, run_id)
    assert "opencode_go" in failed


def test_failed_providers_ignores_partial_provider_failure(worker_with_catalog):
    """When only SOME models from a provider fail, provider is NOT marked down."""
    db = worker_with_catalog._db
    run_id = "test_partial_failure"
    _ensure_run_exists(db, run_id)

    # Only fail ONE opencode_go model, not all
    catalog = worker_with_catalog.catalog
    opencode_models = [m for m in catalog.enabled_models() if m.get("provider") == "opencode_go"]
    assert len(opencode_models) > 1, "Need multiple opencode_go models for this test"

    # Fail only the first one
    m = opencode_models[0]
    insert_candidate(
        db, run_id, "cand_partial", m["alias"],
        m["provider"], m["provider_model"],
        "first_opinion", "failed", utc_now_iso(),
    )

    failed = worker_with_catalog._failed_providers(db, run_id)
    assert "opencode_go" not in failed


def test_failed_providers_multiple_providers(worker_with_catalog):
    """Can detect multiple fully-down providers simultaneously."""
    db = worker_with_catalog._db
    run_id = "test_multi_provider"
    _ensure_run_exists(db, run_id)

    catalog = worker_with_catalog.catalog
    # Fail ALL opencode_go models
    for model in catalog.enabled_models():
        if model.get("provider") == "opencode_go":
            insert_candidate(
                db, run_id, f"cand_{model['alias']}", model["alias"],
                model["provider"], model["provider_model"],
                "first_opinion", "failed", utc_now_iso(),
            )

    # Fail ALL minimax_token_plan models
    for model in catalog.enabled_models():
        if model.get("provider") == "minimax_token_plan":
            insert_candidate(
                db, run_id, f"cand_{model['alias']}", model["alias"],
                model["provider"], model["provider_model"],
                "first_opinion", "failed", utc_now_iso(),
            )

    failed = worker_with_catalog._failed_providers(db, run_id)
    assert "opencode_go" in failed
    assert "minimax_token_plan" in failed


# ---------------------------------------------------------------------------
# RCA-3: _try_fallback skips fully-down providers
# ---------------------------------------------------------------------------

def test_try_fallback_skips_fully_down_provider(worker_with_catalog):
    """When a provider is fully down, _try_fallback skips ALL its models."""
    db = worker_with_catalog._db
    run_id = "test_fallback_skip"

    # Insert a queued run
    from fusion_council_service.domain.run_repository import insert_run
    from fusion_council_service.clock import utc_now_plus_seconds
    insert_run(
        db=db, run_id=run_id, mode="council", prompt="test",
        system_prompt=None, temperature=0.2, max_output_tokens=1000,
        deadline_seconds=900, deadline_at=utc_now_plus_seconds(900),
        owner_token_hash="hash", metadata_json="{}", requested_models_json=None,
        created_at=utc_now_iso(),
    )

    catalog = worker_with_catalog.catalog
    # Fail ALL opencode_go models
    for model in catalog.enabled_models():
        if model.get("provider") == "opencode_go":
            insert_candidate(
                db, run_id, f"cand_{model['alias']}", model["alias"],
                model["provider"], model["provider_model"],
                "first_opinion", "failed", utc_now_iso(),
            )

    # Now try fallback — should skip opencode_go models entirely
    # and only try models from other providers (e.g. minimax)
    run = {"mode": "council", "run_id": run_id}
    fallback = worker_with_catalog._try_fallback(db, run, "primary-researcher")

    if fallback is not None:
        # If a fallback was found, it must NOT be from opencode_go
        assert fallback.get("provider") != "opencode_go", \
            f"Fallback should skip opencode_go but got {fallback['provider']}"


def test_try_fallback_returns_none_when_all_providers_down(worker_with_catalog):
    """When ALL providers are fully down, _try_fallback returns None."""
    db = worker_with_catalog._db
    run_id = "test_all_down"

    from fusion_council_service.domain.run_repository import insert_run
    from fusion_council_service.clock import utc_now_plus_seconds
    insert_run(
        db=db, run_id=run_id, mode="council", prompt="test",
        system_prompt=None, temperature=0.2, max_output_tokens=1000,
        deadline_seconds=900, deadline_at=utc_now_plus_seconds(900),
        owner_token_hash="hash", metadata_json="{}", requested_models_json=None,
        created_at=utc_now_iso(),
    )

    catalog = worker_with_catalog.catalog
    # Fail ALL models from ALL providers
    for model in catalog.enabled_models():
        insert_candidate(
            db, run_id, f"cand_{model['alias']}", model["alias"],
            model["provider"], model["provider_model"],
            "first_opinion", "failed", utc_now_iso(),
        )

    run = {"mode": "council", "run_id": run_id}
    fallback = worker_with_catalog._try_fallback(db, run, "primary-researcher")
    assert fallback is None
