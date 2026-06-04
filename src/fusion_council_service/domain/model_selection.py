"""Model-selection policy for council/fusion orchestration stages.

This module keeps selection rules out of worker control flow so stage routing can
be tested independently from async orchestration. The policy has two guardrails:

1. Never reuse an alias or upstream (provider, provider_model) pair that already
   failed in the same run.
2. De-prioritize upstream pairs with recent failures across runs so a flaky
   provider/model stops burning downstream budget while healthier peers exist.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Optional

from fusion_council_service.clock import utc_now_iso
from fusion_council_service.db import execute_sql, execute_sql_one, execute_sql_all, commit_tx, is_postgresql
from fusion_council_service.logging_utils import get_logger
from fusion_council_service.model_catalog import ModelCatalog

logger = get_logger("fusion_council_service.domain.model_selection")


def current_run_failed_identities(db: object, run_id: str) -> tuple[set[str], set[tuple[str, str]]]:
    """Return aliases and upstream provider/model pairs failed in this run."""
    rows = execute_sql_all(
        db,
        """
        SELECT alias, provider, provider_model
        FROM run_candidates
        WHERE run_id = :run_id AND status = 'failed'
        """,
        {"run_id": run_id},
    )
    aliases: set[str] = set()
    upstream_pairs: set[tuple[str, str]] = set()
    for row in rows:
        alias = row.get("alias")
        provider = row.get("provider")
        provider_model = row.get("provider_model")
        if alias:
            aliases.add(alias)
        if provider and provider_model:
            upstream_pairs.add((provider, provider_model))
    return aliases, upstream_pairs


def recent_failure_counts_by_upstream(db: object, run_id: str) -> dict[tuple[str, str], int]:
    """Return cross-run failure counts by upstream provider/model pair.

    The current run is excluded because same-run failures are a hard block, not a
    soft health score. This intentionally uses persisted candidate artifacts, so
    it works for both SQLite tests and PostgreSQL production without a new table.
    """
    rows = execute_sql_all(
        db,
        """
        SELECT provider, provider_model, COUNT(*) AS failure_count
        FROM run_candidates
        WHERE status = 'failed' AND run_id != :run_id
        GROUP BY provider, provider_model
        """,
        {"run_id": run_id},
    )
    counts: dict[tuple[str, str], int] = {}
    for row in rows:
        provider = row.get("provider")
        provider_model = row.get("provider_model")
        if provider and provider_model:
            counts[(provider, provider_model)] = int(row.get("failure_count") or 0)
    return counts


# ── Persistent health scoring ─────────────────────────────────────────────────

_LATENCY_EMA_ALPHA = 0.3
_FAILURE_HALFLIFE_HOURS = 168  # 1 week


def _compute_health_score(successes: int, total_attempts: int, last_failure_at: Optional[str]) -> float:
    """Compute health_score = success_rate * recency_decay."""
    success_rate = successes / max(total_attempts, 1)
    if last_failure_at is None:
        recency_decay = 1.0
    else:
        try:
            failure_dt = datetime.fromisoformat(last_failure_at.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            hours_since = (now - failure_dt).total_seconds() / 3600.0
            recency_decay = math.exp(-hours_since / _FAILURE_HALFLIFE_HOURS)
        except Exception:
            recency_decay = 1.0
    return success_rate * recency_decay


def _update_ema(current_avg: float, new_value: float, alpha: float = _LATENCY_EMA_ALPHA) -> float:
    """Exponential moving average update."""
    if current_avg == 0:
        return new_value
    return alpha * new_value + (1.0 - alpha) * current_avg


def record_candidate_outcome(
    db: object,
    provider: str,
    provider_model: str,
    success: bool,
    latency_ms: Optional[float],
) -> None:
    """Upsert a provider_health row, updating counts, avg latency, and health_score.

    Uses INSERT OR REPLACE for SQLite and INSERT ... ON CONFLICT DO UPDATE for
    PostgreSQL, matching the existing upsert pattern in model_catalog.py.
    """
    now = utc_now_iso()

    # Read existing row
    existing = execute_sql_one(
        db,
        "SELECT total_attempts, successes, failures, avg_latency_ms, last_failure_at "
        "FROM provider_health WHERE provider = :provider AND provider_model = :provider_model",
        {"provider": provider, "provider_model": provider_model},
    )

    if existing:
        total_attempts = int(existing.get("total_attempts") or 0)
        successes = int(existing.get("successes") or 0)
        failures = int(existing.get("failures") or 0)
        avg_latency = float(existing.get("avg_latency_ms") or 0)
        last_failure_at = existing.get("last_failure_at")
    else:
        total_attempts = 0
        successes = 0
        failures = 0
        avg_latency = 0.0
        last_failure_at = None

    total_attempts += 1
    if success:
        successes += 1
    else:
        failures += 1
        last_failure_at = now

    if latency_ms is not None:
        avg_latency = _update_ema(avg_latency, float(latency_ms))

    health_score = _compute_health_score(successes, total_attempts, last_failure_at)

    if is_postgresql():
        execute_sql(
            db,
            """
            INSERT INTO provider_health
                (provider, provider_model, total_attempts, successes, failures,
                 last_failure_at, last_success_at, avg_latency_ms, health_score, updated_at)
            VALUES
                (:provider, :provider_model, :total_attempts, :successes, :failures,
                 :last_failure_at, :last_success_at, :avg_latency_ms, :health_score, :updated_at)
            ON CONFLICT (provider, provider_model) DO UPDATE SET
                total_attempts = EXCLUDED.total_attempts,
                successes = EXCLUDED.successes,
                failures = EXCLUDED.failures,
                last_failure_at = EXCLUDED.last_failure_at,
                last_success_at = EXCLUDED.last_success_at,
                avg_latency_ms = EXCLUDED.avg_latency_ms,
                health_score = EXCLUDED.health_score,
                updated_at = EXCLUDED.updated_at
            """,
            {
                "provider": provider,
                "provider_model": provider_model,
                "total_attempts": total_attempts,
                "successes": successes,
                "failures": failures,
                "last_failure_at": last_failure_at,
                "last_success_at": now if success else None,
                "avg_latency_ms": avg_latency,
                "health_score": health_score,
                "updated_at": now,
            },
        )
    else:
        execute_sql(
            db,
            """
            INSERT OR REPLACE INTO provider_health
                (provider, provider_model, total_attempts, successes, failures,
                 last_failure_at, last_success_at, avg_latency_ms, health_score, updated_at)
            VALUES
                (:provider, :provider_model, :total_attempts, :successes, :failures,
                 :last_failure_at, :last_success_at, :avg_latency_ms, :health_score, :updated_at)
            """,
            {
                "provider": provider,
                "provider_model": provider_model,
                "total_attempts": total_attempts,
                "successes": successes,
                "failures": failures,
                "last_failure_at": last_failure_at,
                "last_success_at": now if success else None,
                "avg_latency_ms": avg_latency,
                "health_score": health_score,
                "updated_at": now,
            },
        )
    commit_tx(db)

    # W2: feed the post-update health_score into the quarantine state machine.
    # `health_score` is the local variable computed above from the post-update counters
    # and EMA latency. This runs AFTER the commit so the quarantine decision is based
    # on the persisted state, not the in-memory mid-transaction view.
    evaluate_quarantine_transition(db, provider, provider_model, health_score)


def get_health_scores(db: object) -> dict[tuple[str, str], float]:
    """Return a dict mapping (provider, provider_model) -> health_score."""
    rows = execute_sql_all(
        db,
        "SELECT provider, provider_model, health_score FROM provider_health",
    )
    scores: dict[tuple[str, str], float] = {}
    for row in rows:
        provider = row.get("provider")
        provider_model = row.get("provider_model")
        if provider and provider_model:
            scores[(provider, provider_model)] = float(row.get("health_score") or 0.0)
    return scores


def get_health_latencies(db: object) -> dict[tuple[str, str], float | None]:
    """Return a dict mapping (provider, provider_model) -> avg_latency_ms (EMA smoothed)."""
    rows = execute_sql_all(
        db,
        "SELECT provider, provider_model, avg_latency_ms FROM provider_health",
    )
    latencies: dict[tuple[str, str], float | None] = {}
    for row in rows:
        provider = row.get("provider")
        provider_model = row.get("provider_model")
        if provider and provider_model:
            lat = row.get("avg_latency_ms")
            latencies[(provider, provider_model)] = float(lat) if lat is not None else None
    return latencies


def update_health_for_candidate(
    db: object,
    provider: str,
    provider_model: str,
    success: bool,
    latency_ms: Optional[float],
) -> None:
    """Helper for the worker loop: record a candidate outcome into provider_health.

    This is a thin wrapper around record_candidate_outcome for readability at
    call sites in worker_loop.py.
    """
    record_candidate_outcome(db, provider, provider_model, success, latency_ms)


def select_healthy_stage_model(
    *,
    db: object,
    catalog: ModelCatalog,
    run_id: str,
    role_order: list[str],
    avoid_aliases: Optional[set[str]] = None,
) -> Optional[dict]:
    """Select the best currently usable model for a downstream stage.

    Health score is the primary criterion. When provider_health data exists,
    higher health_score wins. When provider_health is empty (tests or fresh
    deployments), recent cross-run failure counts from run_candidates are used
    as the soft de-prioritization signal. Within the same health tier, role fit
    is the tiebreaker, followed by original catalog order. Same-run failures
    remain a hard exclusion to prevent duplicate failed-upstream reuse.
    """
    avoid_aliases = avoid_aliases or set()
    failed_aliases, failed_pairs = current_run_failed_identities(db, run_id)
    health_scores = get_health_scores(db)
    recent_failures = recent_failure_counts_by_upstream(db, run_id)
    original_order = {model.get("alias", ""): idx for idx, model in enumerate(catalog.enabled_models())}
    # W2: durable quarantine exclusion. Compute once at the top of the function so
    # the per-model `usable()` check is O(1) per model.
    quarantined_pairs = get_quarantined_pairs(db)

    def usable(model: dict) -> bool:
        alias = model.get("alias", "")
        pair = (model.get("provider", ""), model.get("provider_model", ""))
        return (
            alias not in avoid_aliases
            and alias not in failed_aliases
            and pair not in failed_pairs
            and pair not in quarantined_pairs  # W2: durable quarantine exclusion
        )

    def health_key(model: dict) -> tuple[float, int]:
        pair = (model.get("provider", ""), model.get("provider_model", ""))
        # Use provider_health score when available, otherwise derive from recent failure counts
        if pair in health_scores:
            score = -health_scores[pair]
        else:
            # Default to 1.0 minus a penalty per recent failure
            score = -(1.0 - recent_failures.get(pair, 0) * 0.1)
        return (score, original_order.get(model.get("alias", ""), 999999))

    enabled = sorted([model for model in catalog.enabled_models() if usable(model)], key=health_key)
    for role in role_order:
        role_matches = [model for model in enabled if model.get("role_bias") == role]
        if role_matches:
            return role_matches[0]
    return enabled[0] if enabled else None


# ── W1: Catalog/Health reconciliation ────────────────────────────────────────


def reconcile_provider_health_with_catalog(db: object, catalog: ModelCatalog) -> int:
    """Delete any provider_health row whose (provider, provider_model) is not in catalog.

    Returns the number of deleted rows. Safe to call at startup — it only deletes
    rows whose upstream pair is no longer in the model catalog. Logs the deleted
    count at INFO. Idempotent: re-running after a clean reconcile is a no-op.

    The dual schema.sql + Alembic pattern means this is also enforced at migration
    time (003_reconcile_provider_health.py) — this helper closes the same gap at
    startup for deploys that pre-date the migration.
    """
    valid_pairs: set[tuple[str, str]] = set()
    for model in catalog.enabled_models():
        provider = model.get("provider")
        provider_model = model.get("provider_model")
        if provider and provider_model:
            valid_pairs.add((provider, provider_model))

    rows = execute_sql_all(
        db,
        "SELECT provider, provider_model FROM provider_health",
    )
    deleted = 0
    for row in rows:
        pair = (row.get("provider"), row.get("provider_model"))
        if pair not in valid_pairs:
            execute_sql(
                db,
                "DELETE FROM provider_health WHERE provider = :p AND provider_model = :m",
                {"p": pair[0], "m": pair[1]},
            )
            deleted += 1
    # Commit the cleanup immediately so we do not hold row locks across the rest
    # of startup. Multiple pods can call this helper during a rollout, and an
    # uncommitted DELETE would keep the transaction open long enough to block the
    # next pod's reconcile pass and starve startup.
    commit_tx(db)

    if deleted:
        logger.info(
            f"reconcile_provider_health: deleted {deleted} stale rows not in catalog",
            event_type="provider_health.reconciled",
        )
    return deleted


# ── W2: Durable Quarantine State ────────────────────────────────────────────

QUARANTINE_HEALTH_THRESHOLD = 0.3
QUARANTINE_STREAK_REQUIRED = 3


def evaluate_quarantine_transition(
    db: object,
    provider: str,
    provider_model: str,
    new_health_score: float,
) -> None:
    """Update the streak/quarantine columns for one upstream pair after a health score change.

    - If new_health_score < QUARANTINE_HEALTH_THRESHOLD: increment consecutive_low_health_count
    - If new_health_score >= QUARANTINE_HEALTH_THRESHOLD: reset consecutive_low_health_count to 0
    - If the post-increment streak reaches QUARANTINE_STREAK_REQUIRED AND the pair is not already
      quarantined: set quarantined=1, quarantined_at=now, quarantine_reason; append one audit row.

    Idempotent in the sense that re-applying the same health score does not append a duplicate
    audit row (the "not already quarantined" guard).
    """
    now = utc_now_iso()
    row = execute_sql_one(
        db,
        "SELECT consecutive_low_health_count, quarantined FROM provider_health "
        "WHERE provider = :p AND provider_model = :m",
        {"p": provider, "m": provider_model},
    )
    if row is None:
        return  # no provider_health row yet (candidate outcome not yet recorded)

    current_streak = int(row.get("consecutive_low_health_count") or 0)
    is_quarantined = bool(row.get("quarantined") or 0)

    if new_health_score < QUARANTINE_HEALTH_THRESHOLD:
        new_streak = current_streak + 1
    else:
        new_streak = 0

    became_quarantined = (
        new_streak >= QUARANTINE_STREAK_REQUIRED and not is_quarantined
    )

    if became_quarantined:
        reason = (
            f"streak={new_streak} consecutive low-health updates "
            f"(health_score={new_health_score:.4f} < {QUARANTINE_HEALTH_THRESHOLD})"
        )
        execute_sql(
            db,
            "UPDATE provider_health SET consecutive_low_health_count=:s, quarantined=1, "
            "quarantined_at=:at, quarantine_reason=:r "
            "WHERE provider=:p AND provider_model=:m",
            {"s": new_streak, "at": now, "r": reason, "p": provider, "m": provider_model},
        )
        execute_sql(
            db,
            "INSERT INTO provider_quarantine_events "
            "(provider, provider_model, event_type, reason, health_score, "
            "consecutive_low_health_count, created_at) "
            "VALUES (:p, :m, 'quarantine', :r, :h, :s, :at)",
            {
                "p": provider, "m": provider_model, "r": reason,
                "h": new_health_score, "s": new_streak, "at": now,
            },
        )
        logger.warning(
            f"quarantined ({provider}, {provider_model}): {reason}",
            event_type="provider.quarantined",
        )
    else:
        execute_sql(
            db,
            "UPDATE provider_health SET consecutive_low_health_count=:s "
            "WHERE provider=:p AND provider_model=:m",
            {"s": new_streak, "p": provider, "m": provider_model},
        )


def get_quarantined_pairs(db: object) -> set[tuple[str, str]]:
    """Return the set of (provider, provider_model) pairs currently quarantined."""
    rows = execute_sql_all(
        db,
        "SELECT provider, provider_model FROM provider_health WHERE quarantined = 1",
    )
    return {(r.get("provider"), r.get("provider_model")) for r in rows}
