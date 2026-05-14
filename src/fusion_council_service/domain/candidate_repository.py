"""Candidate repository — CRUD operations on run_candidates table."""

from typing import Optional

from fusion_council_service.clock import utc_now_iso
from fusion_council_service.db import (
    commit_tx,
    execute_sql_all,
    execute_sql_one,
    execute_sql,
)


def insert_candidate(
    db,
    run_id: str,
    candidate_id: str,
    alias: str,
    provider: str,
    provider_model: str,
    stage: str,
    status: str,
    created_at: str,
) -> Optional[dict]:
    execute_sql(
        db,
        """
        INSERT INTO run_candidates
            (candidate_id, run_id, alias, provider, provider_model, stage, status, created_at, updated_at)
        VALUES (:candidate_id, :run_id, :alias, :provider, :provider_model, :stage, :status, :created_at, :updated_at)
        """,
        {
            "candidate_id": candidate_id,
            "run_id": run_id,
            "alias": alias,
            "provider": provider,
            "provider_model": provider_model,
            "stage": stage,
            "status": status,
            "created_at": created_at,
            "updated_at": created_at,
        },
    )
    commit_tx(db)
    return get_candidate(db, candidate_id)


def get_candidate(db, candidate_id: str) -> Optional[dict]:
    return execute_sql_one(
        db,
        "SELECT * FROM run_candidates WHERE candidate_id = :candidate_id",
        {"candidate_id": candidate_id},
    )


def update_candidate_result(
    db,
    candidate_id: str,
    status: str,
    raw_answer: Optional[str] = None,
    normalized_answer: Optional[str] = None,
    score_json: Optional[str] = None,
    latency_ms: Optional[int] = None,
    input_tokens: Optional[int] = None,
    output_tokens: Optional[int] = None,
    error_code: Optional[str] = None,
    error_message: Optional[str] = None,
) -> None:
    execute_sql(
        db,
        """
        UPDATE run_candidates
        SET status = :status, raw_answer = :raw_answer, normalized_answer = :normalized_answer,
            score_json = :score_json, latency_ms = :latency_ms, input_tokens = :input_tokens,
            output_tokens = :output_tokens, error_code = :error_code,
            error_message = :error_message, updated_at = :updated_at
        WHERE candidate_id = :candidate_id
        """,
        {
            "status": status,
            "raw_answer": raw_answer,
            "normalized_answer": normalized_answer,
            "score_json": score_json,
            "latency_ms": latency_ms,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "error_code": error_code,
            "error_message": error_message,
            "updated_at": utc_now_iso(),
            "candidate_id": candidate_id,
        },
    )
    commit_tx(db)


def list_candidates_for_run(db, run_id: str) -> list[dict]:
    return execute_sql_all(
        db,
        "SELECT * FROM run_candidates WHERE run_id = :run_id ORDER BY created_at",
        {"run_id": run_id},
    )
