"""Model-selection policy for council/fusion orchestration stages.

This module keeps selection rules out of worker control flow so stage routing can
be tested independently from async orchestration. The policy has two guardrails:

1. Never reuse an alias or upstream (provider, provider_model) pair that already
   failed in the same run.
2. De-prioritize upstream pairs with recent failures across runs so a flaky
   provider/model stops burning downstream budget while healthier peers exist.
"""

from __future__ import annotations

from typing import Optional

from fusion_council_service.db import execute_sql_all
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


def select_healthy_stage_model(
    *,
    db: object,
    catalog: ModelCatalog,
    run_id: str,
    role_order: list[str],
    avoid_aliases: Optional[set[str]] = None,
) -> Optional[dict]:
    """Select the best currently usable model for a downstream stage.

    Role fit remains the primary criterion. Within a role, recent cross-run
    failures act as a rolling health score: lower failure count wins. Same-run
    failures remain a hard exclusion to prevent duplicate failed-upstream reuse.
    """
    avoid_aliases = avoid_aliases or set()
    failed_aliases, failed_pairs = current_run_failed_identities(db, run_id)
    recent_failures = recent_failure_counts_by_upstream(db, run_id)
    original_order = {model.get("alias", ""): idx for idx, model in enumerate(catalog.enabled_models())}

    def usable(model: dict) -> bool:
        alias = model.get("alias", "")
        pair = (model.get("provider", ""), model.get("provider_model", ""))
        return alias not in avoid_aliases and alias not in failed_aliases and pair not in failed_pairs

    def health_key(model: dict) -> tuple[int, int]:
        pair = (model.get("provider", ""), model.get("provider_model", ""))
        return (recent_failures.get(pair, 0), original_order.get(model.get("alias", ""), 999999))

    enabled = sorted([model for model in catalog.enabled_models() if usable(model)], key=health_key)
    for role in role_order:
        role_matches = [model for model in enabled if model.get("role_bias") == role]
        if role_matches:
            return role_matches[0]
    return enabled[0] if enabled else None
