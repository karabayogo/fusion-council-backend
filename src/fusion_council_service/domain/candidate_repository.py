"""Candidate repository — CRUD operations on run_candidates table."""

import sqlite3
from typing import Optional

from fusion_council_service.clock import utc_now_iso


def insert_candidate(
    db: sqlite3.Connection,
    run_id: str,
    candidate_id: str,
    alias: str,
    provider: str,
    provider_model: str,
    stage: str,
    status: str,
    created_at: str,
) -> dict:
    cursor = db.cursor()
    cursor.execute(
        """
        INSERT INTO run_candidates
            (candidate_id, run_id, alias, provider, provider_model, stage, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (candidate_id, run_id, alias, provider, provider_model, stage, status, created_at, created_at),
    )
    db.commit()
    return get_candidate(db, candidate_id)


def get_candidate(db: sqlite3.Connection, candidate_id: str) -> Optional[dict]:
    cursor = db.cursor()
    cursor.execute("SELECT * FROM run_candidates WHERE candidate_id = ?", (candidate_id,))
    row = cursor.fetchone()
    return dict(row) if row else None


def update_candidate_result(
    db: sqlite3.Connection,
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
    cursor = db.cursor()
    cursor.execute(
        """
        UPDATE run_candidates
        SET status = ?, raw_answer = ?, normalized_answer = ?, score_json = ?,
            latency_ms = ?, input_tokens = ?, output_tokens = ?,
            error_code = ?, error_message = ?, updated_at = ?
        WHERE candidate_id = ?
        """,
        (status, raw_answer, normalized_answer, score_json,
         latency_ms, input_tokens, output_tokens,
         error_code, error_message, utc_now_iso(), candidate_id),
    )
    db.commit()


def list_candidates_for_run(db: sqlite3.Connection, run_id: str) -> list[dict]:
    cursor = db.cursor()
    cursor.execute(
        "SELECT * FROM run_candidates WHERE run_id = ? ORDER BY created_at",
        (run_id,),
    )
    return [dict(row) for row in cursor.fetchall()]