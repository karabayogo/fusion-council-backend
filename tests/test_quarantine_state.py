"""Regression tests for W2 — Durable Quarantine State.

The quarantine state machine is implemented by:
- `evaluate_quarantine_transition(db, provider, provider_model, new_health_score)` —
  updates the streak, transitions to quarantined if streak >= 3, appends an
  audit row.
- `get_quarantined_pairs(db) -> set[tuple[str, str]]` — returns the set of
  currently quarantined pairs.
- `unquarantine(db, provider, provider_model, reason)` — clears quarantine state
  and appends a `provider_quarantine_events` row with `event_type='unquarantine'`.

W2 also extends `select_healthy_stage_model` to filter out quarantined pairs.

These tests are RED before W2 lands (ImportError) and GREEN after.
"""

from __future__ import annotations

import sqlite3

import pytest

from fusion_council_service.db import execute_sql
from fusion_council_service.domain.model_selection import (
    evaluate_quarantine_transition,
    get_quarantined_pairs,
    select_healthy_stage_model,
)
from fusion_council_service.domain.unquarantine import unquarantine


def _seed_provider_health_row(
    db: sqlite3.Connection,
    provider: str,
    provider_model: str,
    health_score: float = 0.5,
    consecutive_low_health_count: int = 0,
    quarantined: int = 0,
) -> None:
    """Seed a provider_health row with the post-W2 schema (quarantine columns)."""
    execute_sql(
        db,
        """
        INSERT INTO provider_health
            (provider, provider_model, total_attempts, successes, failures,
             last_failure_at, last_success_at, avg_latency_ms, health_score, updated_at,
             consecutive_low_health_count, quarantined, quarantined_at, quarantine_reason)
        VALUES
            (:provider, :provider_model, 5, 1, 4, '2026-06-02T00:00:00Z', NULL, 500,
             :health_score, '2026-06-02T00:00:00Z',
             :consecutive_low_health_count, :quarantined, NULL, NULL)
        """,
        {
            "provider": provider,
            "provider_model": provider_model,
            "health_score": health_score,
            "consecutive_low_health_count": consecutive_low_health_count,
            "quarantined": quarantined,
        },
    )


def _events_count(db: sqlite3.Connection, event_type: str | None = None) -> int:
    from fusion_council_service.db import execute_sql_one

    if event_type is None:
        row = execute_sql_one(db, "SELECT COUNT(*) AS n FROM provider_quarantine_events")
        return int(row["n"])
    row = execute_sql_one(
        db,
        "SELECT COUNT(*) AS n FROM provider_quarantine_events WHERE event_type = :et",
        {"et": event_type},
    )
    return int(row["n"])


# ── evaluate_quarantine_transition ──────────────────────────────────────────


def test_low_score_increments_streak(tmp_db: sqlite3.Connection) -> None:
    """A health_score below threshold increments the streak by 1."""
    _seed_provider_health_row(tmp_db, "opencode_go", "kimi-k2.6", health_score=0.5)

    evaluate_quarantine_transition(tmp_db, "opencode_go", "kimi-k2.6", new_health_score=0.1)

    from fusion_council_service.db import execute_sql_one
    row = execute_sql_one(
        tmp_db,
        "SELECT consecutive_low_health_count, quarantined FROM provider_health "
        "WHERE provider='opencode_go' AND provider_model='kimi-k2.6'",
    )
    assert row["consecutive_low_health_count"] == 1
    assert row["quarantined"] == 0


def test_healthy_score_resets_streak(tmp_db: sqlite3.Connection) -> None:
    """A health_score >= threshold resets the streak to 0."""
    _seed_provider_health_row(
        tmp_db, "opencode_go", "kimi-k2.6", health_score=0.5, consecutive_low_health_count=2,
    )

    evaluate_quarantine_transition(tmp_db, "opencode_go", "kimi-k2.6", new_health_score=0.9)

    from fusion_council_service.db import execute_sql_one
    row = execute_sql_one(
        tmp_db,
        "SELECT consecutive_low_health_count, quarantined FROM provider_health "
        "WHERE provider='opencode_go' AND provider_model='kimi-k2.6'",
    )
    assert row["consecutive_low_health_count"] == 0
    assert row["quarantined"] == 0


def test_third_consecutive_low_update_quarantines(tmp_db: sqlite3.Connection) -> None:
    """When the streak hits 3, the pair is quarantined and an audit row is appended."""
    _seed_provider_health_row(
        tmp_db, "opencode_go", "qwen3.7-max", health_score=0.0, consecutive_low_health_count=2,
    )

    evaluate_quarantine_transition(tmp_db, "opencode_go", "qwen3.7-max", new_health_score=0.1)

    from fusion_council_service.db import execute_sql_one
    row = execute_sql_one(
        tmp_db,
        "SELECT consecutive_low_health_count, quarantined, quarantined_at, quarantine_reason "
        "FROM provider_health WHERE provider='opencode_go' AND provider_model='qwen3.7-max'",
    )
    assert row["consecutive_low_health_count"] == 3
    assert row["quarantined"] == 1
    assert row["quarantined_at"] is not None
    assert "streak=3" in (row["quarantine_reason"] or "")
    assert _events_count(tmp_db, event_type="quarantine") == 1


def test_audit_row_appended_exactly_once_on_quarantine(tmp_db: sqlite3.Connection) -> None:
    """Re-applying a low score after the pair is already quarantined does NOT
    append another audit row. The 'not already quarantined' guard is the
    idempotency boundary."""
    _seed_provider_health_row(
        tmp_db, "opencode_go", "qwen3.7-max", health_score=0.0, consecutive_low_health_count=2,
    )

    # First call: triggers quarantine transition.
    evaluate_quarantine_transition(tmp_db, "opencode_go", "qwen3.7-max", new_health_score=0.1)
    # Second call with another low score: already quarantined, no new audit row.
    evaluate_quarantine_transition(tmp_db, "opencode_go", "qwen3.7-max", new_health_score=0.1)
    # Third call: also no new audit row.
    evaluate_quarantine_transition(tmp_db, "opencode_go", "qwen3.7-max", new_health_score=0.1)

    assert _events_count(tmp_db, event_type="quarantine") == 1


def test_evaluate_returns_silently_when_no_provider_health_row(tmp_db: sqlite3.Connection) -> None:
    """If provider_health has no row for the pair, the function returns silently
    (this is a legitimate race: candidate outcome not yet recorded)."""
    # No seed. Should not raise.
    evaluate_quarantine_transition(tmp_db, "opencode_go", "no-such-model", new_health_score=0.1)


# ── get_quarantined_pairs ───────────────────────────────────────────────────


def test_get_quarantined_pairs_returns_only_quarantined(tmp_db: sqlite3.Connection) -> None:
    """Returns the set of (provider, provider_model) where quarantined=1."""
    _seed_provider_health_row(tmp_db, "opencode_go", "qwen3.7-max", quarantined=1)
    _seed_provider_health_row(tmp_db, "opencode_go", "qwen3.6-plus", quarantined=0)
    _seed_provider_health_row(tmp_db, "minimax_token_plan", "MiniMax-M3", quarantined=1)

    pairs = get_quarantined_pairs(tmp_db)
    assert ("opencode_go", "qwen3.7-max") in pairs
    assert ("minimax_token_plan", "MiniMax-M3") in pairs
    assert ("opencode_go", "qwen3.6-plus") not in pairs


# ── unquarantine ────────────────────────────────────────────────────────────


def test_unquarantine_clears_state_and_audits(tmp_db: sqlite3.Connection) -> None:
    """Clears quarantine state and appends a single unquarantine audit row."""
    _seed_provider_health_row(
        tmp_db,
        "opencode_go",
        "qwen3.7-max",
        health_score=0.0,
        consecutive_low_health_count=3,
        quarantined=1,
    )

    unquarantine(tmp_db, "opencode_go", "qwen3.7-max", reason="manual recovery after PR #30")

    from fusion_council_service.db import execute_sql_one
    row = execute_sql_one(
        tmp_db,
        "SELECT quarantined, quarantined_at, quarantine_reason, consecutive_low_health_count "
        "FROM provider_health WHERE provider='opencode_go' AND provider_model='qwen3.7-max'",
    )
    assert row["quarantined"] == 0
    assert row["quarantined_at"] is None
    assert row["quarantine_reason"] is None
    assert row["consecutive_low_health_count"] == 0
    assert _events_count(tmp_db, event_type="unquarantine") == 1


def test_unquarantine_rejects_empty_reason(tmp_db: sqlite3.Connection) -> None:
    """Empty reason is rejected — operator must provide a non-empty audit trail."""
    _seed_provider_health_row(tmp_db, "opencode_go", "qwen3.7-max", quarantined=1)

    with pytest.raises(ValueError, match="non-empty reason"):
        unquarantine(tmp_db, "opencode_go", "qwen3.7-max", reason="")


def test_unquarantine_rejects_unquarantined_pair(tmp_db: sqlite3.Connection) -> None:
    """Cannot unquarantine a pair that isn't quarantined — guards against typos."""
    _seed_provider_health_row(tmp_db, "opencode_go", "qwen3.6-plus", quarantined=0)

    with pytest.raises(ValueError, match="not currently quarantined"):
        unquarantine(tmp_db, "opencode_go", "qwen3.6-plus", reason="test")


# ── select_healthy_stage_model filter ──────────────────────────────────────


def test_select_healthy_stage_model_excludes_quarantined(tmp_db: sqlite3.Connection) -> None:
    """A quarantined pair must not be selected by select_healthy_stage_model."""
    # Build a tiny catalog with 2 models, one quarantined.
    _seed_provider_health_row(tmp_db, "opencode_go", "qwen3.7-max", quarantined=1, health_score=0.0)
    _seed_provider_health_row(tmp_db, "opencode_go", "qwen3.6-plus", quarantined=0, health_score=0.95)

    catalog = [
        {"alias": "creative", "provider": "opencode_go", "provider_model": "qwen3.7-max",
         "enabled": True, "role_bias": "primary", "family": "x", "tier": "primary"},
        {"alias": "summariser", "provider": "opencode_go", "provider_model": "qwen3.6-plus",
         "enabled": True, "role_bias": "reviewer", "family": "x", "tier": "primary"},
    ]
    from fusion_council_service.model_catalog import ModelCatalog
    mc = ModelCatalog(catalog)

    # role_order ['reviewer', 'primary'] — first role wins. The only 'reviewer' model
    # is qwen3.6-plus, which is un-quarantined. If the quarantine filter is broken,
    # the selection would still pick qwen3.6-plus (it's also a higher health_score)
    # so this test is weak on its own. Add an assertion on the un-quarantined alias
    # to verify the model is reachable.
    selected = select_healthy_stage_model(
        db=tmp_db,
        catalog=mc,
        run_id="run_test_xyz",
        role_order=["reviewer", "primary"],
        avoid_aliases=set(),
    )
    assert selected is not None
    # The reviewer-role model is the un-quarantined one. The quarantined one
    # has a higher original_order index (1) so it would not be picked anyway,
    # but the test still verifies the function returns a usable model.
    assert selected["provider_model"] == "qwen3.6-plus"


def test_select_healthy_stage_model_with_only_quarantined_models_returns_none(
    tmp_db: sqlite3.Connection,
) -> None:
    """If every enabled model is quarantined, select_healthy_stage_model returns None.

    This proves the quarantine filter is wired: with no usable model, the function
    must NOT return a quarantined one.
    """
    _seed_provider_health_row(tmp_db, "opencode_go", "qwen3.7-max", quarantined=1, health_score=0.0)

    catalog = [
        {"alias": "only_model", "provider": "opencode_go", "provider_model": "qwen3.7-max",
         "enabled": True, "role_bias": "primary", "family": "x", "tier": "primary"},
    ]
    from fusion_council_service.model_catalog import ModelCatalog
    mc = ModelCatalog(catalog)

    selected = select_healthy_stage_model(
        db=tmp_db,
        catalog=mc,
        run_id="run_test_only_quarantined",
        role_order=["primary"],
        avoid_aliases=set(),
    )
    assert selected is None, (
        f"with all models quarantined, selection must return None. Got: {selected!r}"
    )
