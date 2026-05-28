"""Nightly shadow parity gate for LangGraph vs legacy routing."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MIN_CONSECUTIVE_RUNS = 100
MIN_STAGE_PARITY_RATE = 0.95
MAX_TERMINAL_CORRUPTION = 0


@dataclass(frozen=True)
class GateDecision:
    failures: list[str]
    overall: str


def _compute_metrics_from_rows(rows: list[dict[str, Any]], lookback_hours: int) -> dict[str, Any]:
    if not rows:
        return {
            "total_runs": 0,
            "stage_parity_rate": None,
            "answer_presence_rate": None,
            "error_rate": None,
            "terminal_corruption_count": 0,
            "runs_by_engine": {},
            "lookback_hours": lookback_hours,
            "error": "No runs in lookback window",
        }

    total = len(rows)
    stage_match = sum(1 for r in rows if r.get("stage_order_match") is True)
    answer_present = sum(1 for r in rows if r.get("final_answer_present") is True)
    has_errors = sum(1 for r in rows if r.get("error_codes"))
    terminal_corrupt = sum(
        1
        for r in rows
        if r.get("final_answer_present") is True and r.get("final_status") in {"failed", "error"}
    )

    by_engine: dict[str, int] = {}
    for r in rows:
        key = r.get("engine") or "unknown"
        by_engine[key] = by_engine.get(key, 0) + 1

    return {
        "total_runs": total,
        "stage_parity_rate": stage_match / total if total else None,
        "answer_presence_rate": answer_present / total if total else None,
        "error_rate": has_errors / total if total else None,
        "terminal_corruption_count": terminal_corrupt,
        "runs_by_engine": by_engine,
        "lookback_hours": lookback_hours,
    }


def evaluate_gate(metrics: dict[str, Any]) -> GateDecision:
    failures: list[str] = []

    if metrics.get("total_runs", 0) < MIN_CONSECUTIVE_RUNS:
        failures.append(
            f"total_runs={metrics.get('total_runs', 0)} < {MIN_CONSECUTIVE_RUNS} (not enough data yet)"
        )

    stage_parity = metrics.get("stage_parity_rate")
    if stage_parity is not None and stage_parity < MIN_STAGE_PARITY_RATE:
        failures.append(
            f"stage_parity_rate={stage_parity:.3f} < {MIN_STAGE_PARITY_RATE} (below threshold)"
        )

    terminal_corrupt = metrics.get("terminal_corruption_count", 0)
    if terminal_corrupt > MAX_TERMINAL_CORRUPTION:
        failures.append(
            f"terminal_corruption_count={terminal_corrupt} > {MAX_TERMINAL_CORRUPTION} (must be zero)"
        )

    return GateDecision(failures=failures, overall="PASS" if not failures else "FAIL")


async def compute_parity_metrics(conn: asyncpg.Connection, lookback_hours: int = 168) -> dict[str, Any]:
    cutoff = datetime.now(tz=UTC) - timedelta(hours=lookback_hours)

    records = await conn.fetch(
        """
        SELECT engine, final_status, final_answer_present, stage_order_match, error_codes
        FROM run_shadow_diff
        WHERE logged_at >= $1
        ORDER BY logged_at DESC
        """,
        cutoff.isoformat().replace("+00:00", "Z"),
    )
    rows = []
    for rec in records:
        row = dict(rec)
        raw_error_codes = row.get("error_codes")
        if isinstance(raw_error_codes, str):
            try:
                row["error_codes"] = json.loads(raw_error_codes)
            except Exception:
                row["error_codes"] = [raw_error_codes]
        rows.append(row)
    return _compute_metrics_from_rows(rows, lookback_hours)


async def main() -> int:
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        logger.error("DATABASE_URL not set")
        return 1

    dsn = db_url.replace("postgresql+asyncpg://", "postgresql://")
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=5, command_timeout=30)
    try:
        async with pool.acquire() as conn:
            metrics = await compute_parity_metrics(conn)
    finally:
        await pool.close()

    decision = evaluate_gate(metrics)
    report = {
        "timestamp": datetime.now(tz=UTC).isoformat(),
        "metrics": metrics,
        "exit_criteria": {
            "min_consecutive_runs": MIN_CONSECUTIVE_RUNS,
            "min_stage_parity_rate": MIN_STAGE_PARITY_RATE,
            "max_terminal_corruption": MAX_TERMINAL_CORRUPTION,
        },
        "failures": decision.failures,
        "overall": decision.overall,
    }

    print(json.dumps(report, indent=2))
    if decision.failures:
        logger.error("Shadow validation FAILED: %s", "; ".join(decision.failures))
        return 1
    logger.info("Shadow validation PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
