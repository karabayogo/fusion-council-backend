"""Dependency-free Prometheus metrics for Fusion Council candidate pipelines."""

from __future__ import annotations

from collections import defaultdict
from threading import Lock

_CANDIDATE_COUNT_BUCKETS = (0, 1, 3, 6, 9, 12, float("inf"))
_STAGE_DURATION_BUCKETS = (1, 5, 15, 30, 60, 120, 300, float("inf"))
_lock = Lock()

_candidate_status_total: dict[str, int] = defaultdict(int)
_terminal_without_candidates_total = 0
_answers_candidate_count_observations: list[float] = []
_stage_duration_observations: dict[str, list[float]] = defaultdict(list)
_observed_terminal_run_ids: set[str] = set()
_observed_answers_payload_run_ids: set[str] = set()


def reset_metrics() -> None:
    """Reset in-memory metrics. Intended for tests only."""
    global _terminal_without_candidates_total
    with _lock:
        _candidate_status_total.clear()
        _terminal_without_candidates_total = 0
        _answers_candidate_count_observations.clear()
        _stage_duration_observations.clear()
        _observed_terminal_run_ids.clear()
        _observed_answers_payload_run_ids.clear()


def increment_candidate_status(status: str) -> None:
    with _lock:
        _candidate_status_total[status] += 1


def increment_terminal_without_candidates() -> None:
    global _terminal_without_candidates_total
    with _lock:
        _terminal_without_candidates_total += 1


def observe_council_answers_candidate_count(count: int) -> None:
    with _lock:
        _answers_candidate_count_observations.append(float(count))


def observe_terminal_council_run(run_id: str, candidate_count: int) -> None:
    global _terminal_without_candidates_total
    with _lock:
        if run_id in _observed_terminal_run_ids:
            return
        _observed_terminal_run_ids.add(run_id)
        _answers_candidate_count_observations.append(float(candidate_count))
        if candidate_count == 0:
            _terminal_without_candidates_total += 1


def observe_stage_duration(stage: str, seconds: float) -> None:
    with _lock:
        _stage_duration_observations[stage].append(float(seconds))


def observe_answers_payload_once(run_id: str, candidates: list[dict]) -> None:
    """Observe candidate-level metrics once per run from persisted answers payload."""
    with _lock:
        if run_id in _observed_answers_payload_run_ids:
            return
        _observed_answers_payload_run_ids.add(run_id)

    observe_council_answers_candidate_count(len(candidates))
    for candidate in candidates:
        increment_candidate_status(candidate.get("status") or "unknown")
        latency_ms = candidate.get("latency_ms")
        if latency_ms is not None:
            observe_stage_duration(candidate.get("stage") or "unknown", float(latency_ms) / 1000.0)


def _fmt(value: float | int) -> str:
    if isinstance(value, float) and value.is_integer():
        return f"{value:.1f}"
    return str(value)


def _bucket_label(bound: float | int) -> str:
    return "+Inf" if bound == float("inf") else str(bound)


def _render_histogram(name: str, observations: list[float], buckets: tuple[float | int, ...], labels: str = "") -> list[str]:
    suffix = f"{{{labels}," if labels else "{"
    lines: list[str] = []
    for bucket in buckets:
        count = sum(1 for value in observations if value <= bucket)
        lines.append(f'{name}_bucket{suffix}le="{_bucket_label(bucket)}"}} {count}')
    label_block = f"{{{labels}}}" if labels else ""
    lines.append(f"{name}_count{label_block} {len(observations)}")
    lines.append(f"{name}_sum{label_block} {_fmt(float(sum(observations)))}")
    return lines


def render_prometheus(app_env: str | None = None, catalog_models: int | None = None) -> str:
    with _lock:
        candidate_status = dict(_candidate_status_total)
        terminal_without_candidates = _terminal_without_candidates_total
        answer_counts = list(_answers_candidate_count_observations)
        stage_durations = {stage: list(values) for stage, values in _stage_duration_observations.items()}

    lines = [
        "# HELP fusion_council_app_info Static application metadata.",
        "# TYPE fusion_council_app_info gauge",
    ]
    if app_env is not None and catalog_models is not None:
        lines.append(f'fusion_council_app_info{{app_env="{app_env}",catalog_models="{catalog_models}"}} 1')

    lines.extend([
        "# HELP council_answers_candidate_count Candidate count distribution for terminal council answer payloads.",
        "# TYPE council_answers_candidate_count histogram",
        *_render_histogram("council_answers_candidate_count", answer_counts, _CANDIDATE_COUNT_BUCKETS),
        "# HELP council_runs_terminal_without_candidates_total Terminal runs that had zero candidate artifacts.",
        "# TYPE council_runs_terminal_without_candidates_total counter",
        f"council_runs_terminal_without_candidates_total {terminal_without_candidates}",
        "# HELP council_stage_duration_seconds Candidate stage latency distribution in seconds.",
        "# TYPE council_stage_duration_seconds histogram",
    ])
    for stage in sorted(stage_durations):
        lines.extend(_render_histogram("council_stage_duration_seconds", stage_durations[stage], _STAGE_DURATION_BUCKETS, f'stage="{stage}"'))

    lines.extend([
        "# HELP council_candidate_status_total Candidate status transitions recorded by the worker.",
        "# TYPE council_candidate_status_total counter",
    ])
    for status, count in sorted(candidate_status.items()):
        lines.append(f'council_candidate_status_total{{status="{status}"}} {count}')

    return "\n".join(lines) + "\n"
