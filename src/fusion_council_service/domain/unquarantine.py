"""Deterministic unquarantine path.

This is the ONLY repo-tracked way to clear a quarantine. It is called by the
unquarantine_cli.py CLI (operator-facing) and by tests. It never accepts
untrusted input from the API surface.

Usage from a kubectl exec shell inside a pod:
    python -m fusion_council_service.scripts.unquarantine_cli \\
        <provider> <provider_model> "<reason>"

Usage from tests:
    from fusion_council_service.domain.unquarantine import unquarantine
    unquarantine(db, "opencode_go", "qwen3.7-max", reason="manual recovery after PR #X")
"""
from __future__ import annotations

from fusion_council_service.clock import utc_now_iso
from fusion_council_service.db import execute_sql, execute_sql_one

# Module-level logger is created on demand to avoid circular imports.
def _get_logger():
    from fusion_council_service.logging_utils import get_logger
    return get_logger("fusion_council_service.domain.unquarantine")


def unquarantine(db: object, provider: str, provider_model: str, reason: str) -> None:
    """Clear quarantine state for (provider, provider_model) and append an audit row.

    - Validates that the pair is currently quarantined (raises ValueError if not).
    - Validates that `reason` is non-empty (raises ValueError if blank).
    - Does NOT verify the catalog still intends to use this upstream — that check is
      the operator's responsibility (see docs/operations/unquarantine.md).
    """
    if not reason or not reason.strip():
        raise ValueError("unquarantine requires a non-empty reason")

    row = execute_sql_one(
        db,
        "SELECT quarantined FROM provider_health WHERE provider=:p AND provider_model=:m",
        {"p": provider, "m": provider_model},
    )
    if row is None or not bool(row.get("quarantined") or 0):
        raise ValueError(
            f"({provider}, {provider_model}) is not currently quarantined"
        )

    now = utc_now_iso()
    execute_sql(
        db,
        "UPDATE provider_health SET quarantined=0, quarantined_at=NULL, "
        "quarantine_reason=NULL, consecutive_low_health_count=0 "
        "WHERE provider=:p AND provider_model=:m",
        {"p": provider, "m": provider_model},
    )
    execute_sql(
        db,
        "INSERT INTO provider_quarantine_events "
        "(provider, provider_model, event_type, reason, health_score, "
        "consecutive_low_health_count, created_at) "
        "VALUES (:p, :m, 'unquarantine', :r, NULL, 0, :at)",
        {"p": provider, "m": provider_model, "r": reason, "at": now},
    )
    _get_logger().warning(
        f"unquarantined ({provider}, {provider_model}): {reason}",
        event_type="provider.unquarantined",
    )
