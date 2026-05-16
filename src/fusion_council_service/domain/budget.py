"""Budget engine — stage budgets, deadline management, degradation rules."""

from dataclasses import dataclass
from typing import Optional


# --- Budget tables ---

SINGLE_BUDGET_TABLE = {
    "generation": 0.65,
}

FUSION_BUDGET_TABLE = {
    "generation": 0.30,
    "synthesis": 0.20,
    "verification": 0.15,
    "buffer": 0.10,
}

COUNCIL_BUDGET_TABLE = {
    "first_opinion": 0.25,
    "peer_review": 0.25,
    "debate": 0.15,
    "synthesis": 0.15,
    "verification": 0.10,
    "buffer": 0.05,
}

EXTENDED_COUNCIL_BUDGET_TABLE = {
    "first_opinion": 0.20,
    "peer_review": 0.20,
    "debate": 0.20,
    "synthesis": 0.15,
    "verification": 0.10,
    "buffer": 0.05,
    "reserve": 0.10,
}

# Deadline ceilings
FUSION_TOTAL_CEILING_SECONDS = 900
COUNCIL_TOTAL_CEILING_SECONDS = 1800
EXTENDED_COUNCIL_CEILING_SECONDS = 3600

# Mode defaults
MODE_DEFAULT_DEADLINE = {
    "single": 60,
    "fusion": 900,
    "council": 1800,
}

MODE_MAX_DEADLINE = {
    "single": 300,
    "fusion": 900,
    "council": 1800,
}


@dataclass
class StageBudget:
    stage: str
    budget_seconds: float
    soft_deadline_seconds: float  # 80% of budget_seconds


@dataclass
class RunBudget:
    mode: str
    total_seconds: int
    stages: list[StageBudget]


def compute_budget(mode: str, deadline_seconds: int) -> RunBudget:
    """Compute stage budgets for a given mode and deadline."""
    if mode == "single":
        table = SINGLE_BUDGET_TABLE
    elif mode == "fusion":
        table = FUSION_BUDGET_TABLE
    elif mode == "council":
        if deadline_seconds > COUNCIL_TOTAL_CEILING_SECONDS:
            table = EXTENDED_COUNCIL_BUDGET_TABLE
        else:
            table = COUNCIL_BUDGET_TABLE
    else:
        raise ValueError(f"Unknown mode: {mode}")

    stages = []
    for stage_name, fraction in table.items():
        budget = deadline_seconds * fraction
        stages.append(StageBudget(
            stage=stage_name,
            budget_seconds=budget,
            soft_deadline_seconds=budget * 0.80,
        ))

    return RunBudget(mode=mode, total_seconds=deadline_seconds, stages=stages)


def resolve_deadline(mode: str, requested: Optional[int]) -> tuple[int, int]:
    """Resolve the actual deadline and the applied deadline for a run request.
    Returns (deadline_seconds, deadline_applied) where deadline_applied:
      0 = user-specified
      1 = mode default
      2 = mode ceiling
    """
    if requested is not None and requested > 0:
        ceiling = MODE_MAX_DEADLINE.get(mode, 300)
        if requested > ceiling:
            return ceiling, 2
        return requested, 0

    # No deadline specified, use mode default
    default = MODE_DEFAULT_DEADLINE.get(mode, 60)
    return default, 1


def should_degrade(mode: str, elapsed_seconds: float, total_seconds: int) -> Optional[str]:
    """Check if a running job should degrade due to deadline pressure.
    Returns degradation reason or None.
    """
    if mode == "single":
        if elapsed_seconds > total_seconds * 0.80:
            return "single_approaching_deadline"
    elif mode == "fusion":
        if elapsed_seconds > total_seconds * 0.95:
            return "fusion_deadline_imminent_return_best_candidate"
        if elapsed_seconds > total_seconds * 0.85:
            return "fusion_approaching_deadline_skip_verification"
    elif mode == "council":
        if elapsed_seconds > total_seconds * 0.95:
            return "council_deadline_imminent_return_best_opinion"
        if elapsed_seconds > total_seconds * 0.90:
            return "council_skip_peer_review"
        if elapsed_seconds > total_seconds * 0.80:
            return "council_skip_debate"
    return None


def _models_ordered_by_role(catalog, role_order: list[str]) -> list[dict]:
    """Return enabled catalog models ordered by role_bias and YAML order.

    The YAML catalog is the source of truth. Code only defines role preference;
    it does not hardcode aliases or providers.
    """
    enabled = catalog.enabled_models()
    role_rank = {role: idx for idx, role in enumerate(role_order)}
    yaml_order = {id(model): idx for idx, model in enumerate(enabled)}
    return sorted(
        enabled,
        key=lambda m: (role_rank.get(m.get("role_bias"), len(role_rank)), yaml_order[id(m)]),
    )


def select_models_for_mode(mode: str, catalog, requested_models: Optional[list[str]] = None) -> list[dict]:
    """Select enabled models for a run mode from config/models.yaml.

    requested_models, when supplied, are still filtered through the catalog and
    must be enabled. Without explicit requests, selection is derived from the
    enabled entries and their role_bias values in the YAML file.
    """
    from fusion_council_service.model_catalog import (
        COUNCIL_ROLE_ORDER,
        FUSION_ROLE_ORDER,
        SINGLE_ROLE_ORDER,
    )

    if requested_models:
        models = []
        for alias in requested_models:
            m = catalog.get(alias)
            if m and m.get("enabled", False):
                models.append(m)
        if models:
            return models

    if mode == "single":
        return _models_ordered_by_role(catalog, SINGLE_ROLE_ORDER)[:1]
    if mode == "fusion":
        return _models_ordered_by_role(catalog, FUSION_ROLE_ORDER)[:3]
    if mode == "council":
        return _models_ordered_by_role(catalog, COUNCIL_ROLE_ORDER)[:3]
    return []
