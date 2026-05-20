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
from fusion_council_service.model_catalog import ModelCatalog


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

    def usable(model: dict) -> bool:
        alias = model.get("alias", "")
        pair = (model.get("provider", ""), model.get("provider_model", ""))
        return alias not in avoid_aliases and alias not in failed_aliases and pair not in failed_pairs

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
