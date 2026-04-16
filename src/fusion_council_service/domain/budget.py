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
FUSION_TOTAL_CEILING_SECONDS = 300
COUNCIL_TOTAL_CEILING_SECONDS = 1200
EXTENDED_COUNCIL_CEILING_SECONDS = 1800

# Mode defaults
MODE_DEFAULT_DEADLINE = {
    "single": 60,
    "fusion": 120,
    "council": 120,
}

MODE_MAX_DEADLINE = {
    "single": 300,
    "fusion": 300,
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


def select_models_for_mode(mode: str, catalog, requested_models: Optional[list[str]] = None) -> list[dict]:
    """Select which models to use based on mode and catalog.
    Returns list of model dicts from the catalog.
    """
    from fusion_council_service.model_catalog import (
        FUSION_ACTIVE_TRIO, FUSION_FALLBACK_QUEUE,
        COUNCIL_ACTIVE_TRIO, COUNCIL_FALLBACK_QUEUE,
        SINGLE_DEFAULT_MODEL,
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
        m = catalog.get(SINGLE_DEFAULT_MODEL)
        return [m] if m and m.get("enabled", False) else []
    elif mode == "fusion":
        models = []
        for alias in FUSION_ACTIVE_TRIO:
            m = catalog.get(alias)
            if m and m.get("enabled", False):
                models.append(m)
        if len(models) < 3:
            for alias in FUSION_FALLBACK_QUEUE:
                m = catalog.get(alias)
                if m and m.get("enabled", False) and m not in models:
                    models.append(m)
                    if len(models) >= 3:
                        break
        return models[:3]
    elif mode == "council":
        models = []
        for alias in COUNCIL_ACTIVE_TRIO:
            m = catalog.get(alias)
            if m and m.get("enabled", False):
                models.append(m)
        if len(models) < 3:
            for alias in COUNCIL_FALLBACK_QUEUE:
                m = catalog.get(alias)
                if m and m.get("enabled", False) and m not in models:
                    models.append(m)
                    if len(models) >= 3:
                        break
        return models[:3]
    return []