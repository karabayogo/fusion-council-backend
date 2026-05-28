"""Persistence helpers for orchestration state and shadow diffs."""

import json
from typing import Optional

from fusion_council_service.clock import utc_now_iso
from fusion_council_service.db import commit_tx, execute_sql, execute_sql_one


def upsert_orchestration_state(
    db,
    *,
    run_id: str,
    thread_id: str,
    orchestrator_engine: str,
    orchestrator_mode: str,
    engine_version: str,
    orchestration_status: str,
    last_checkpoint_id: Optional[str] = None,
    resume_count_increment: bool = False,
    last_error_code: Optional[str] = None,
    last_error_message: Optional[str] = None,
) -> None:
    now = utc_now_iso()
    execute_sql(
        db,
        """
        INSERT INTO run_orchestration_state (
            run_id,
            thread_id,
            orchestrator_engine,
            orchestrator_mode,
            engine_version,
            orchestration_status,
            last_checkpoint_id,
            resume_count,
            last_error_code,
            last_error_message,
            created_at,
            updated_at
        )
        VALUES (
            :run_id,
            :thread_id,
            :orchestrator_engine,
            :orchestrator_mode,
            :engine_version,
            :orchestration_status,
            :last_checkpoint_id,
            :resume_count,
            :last_error_code,
            :last_error_message,
            :created_at,
            :updated_at
        )
        ON CONFLICT(run_id) DO UPDATE SET
            thread_id = excluded.thread_id,
            orchestrator_engine = excluded.orchestrator_engine,
            orchestrator_mode = excluded.orchestrator_mode,
            engine_version = excluded.engine_version,
            orchestration_status = excluded.orchestration_status,
            last_checkpoint_id = excluded.last_checkpoint_id,
            resume_count = run_orchestration_state.resume_count + :resume_delta,
            last_error_code = excluded.last_error_code,
            last_error_message = excluded.last_error_message,
            updated_at = excluded.updated_at
        """,
        {
            "run_id": run_id,
            "thread_id": thread_id,
            "orchestrator_engine": orchestrator_engine,
            "orchestrator_mode": orchestrator_mode,
            "engine_version": engine_version,
            "orchestration_status": orchestration_status,
            "last_checkpoint_id": last_checkpoint_id,
            "resume_count": 1 if resume_count_increment else 0,
            "last_error_code": last_error_code,
            "last_error_message": last_error_message,
            "created_at": now,
            "updated_at": now,
            "resume_delta": 1 if resume_count_increment else 0,
        },
    )
    commit_tx(db)


def get_orchestration_state(db, run_id: str) -> Optional[dict]:
    return execute_sql_one(
        db,
        "SELECT * FROM run_orchestration_state WHERE run_id = :run_id",
        {"run_id": run_id},
    )


def insert_shadow_diff(
    db,
    *,
    run_id: str,
    engine: str,
    final_status: Optional[str],
    final_answer_present: bool,
    stage_count: Optional[int] = None,
    stage_order_match: Optional[bool] = None,
    candidate_counts: Optional[dict] = None,
    error_codes: Optional[list[str]] = None,
    diff_summary: Optional[dict] = None,
) -> None:
    execute_sql(
        db,
        """
        INSERT INTO run_shadow_diff (
            run_id,
            engine,
            final_status,
            final_answer_present,
            stage_count,
            stage_order_match,
            candidate_counts,
            error_codes,
            diff_summary,
            logged_at
        )
        VALUES (
            :run_id,
            :engine,
            :final_status,
            :final_answer_present,
            :stage_count,
            :stage_order_match,
            :candidate_counts,
            :error_codes,
            :diff_summary,
            :logged_at
        )
        """,
        {
            "run_id": run_id,
            "engine": engine,
            "final_status": final_status,
            "final_answer_present": 1 if final_answer_present else 0,
            "stage_count": stage_count,
            "stage_order_match": None if stage_order_match is None else (1 if stage_order_match else 0),
            "candidate_counts": json.dumps(candidate_counts or {}),
            "error_codes": json.dumps(error_codes or []),
            "diff_summary": json.dumps(diff_summary or {}),
            "logged_at": utc_now_iso(),
        },
    )
    commit_tx(db)
