"""Run repository — CRUD operations on the runs table."""

import sqlite3
from typing import Optional


def insert_run(
    db: sqlite3.Connection,
    run_id: str,
    mode: str,
    prompt: str,
    system_prompt: str | None,
    temperature: float,
    max_output_tokens: int,
    deadline_seconds: int,
    deadline_at: str,
    owner_token_hash: str,
    metadata_json: str | None,
    requested_models_json: str | None,
    created_at: str,
) -> dict:
    cursor = db.cursor()
    cursor.execute(
        """
        INSERT INTO runs (run_id, mode, prompt, system_prompt, temperature, max_output_tokens,
                         deadline_seconds, deadline_at, status, owner_token_hash,
                         metadata_json, requested_models_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?)
        """,
        (run_id, mode, prompt, system_prompt, temperature, max_output_tokens,
         deadline_seconds, deadline_at, owner_token_hash, metadata_json,
         requested_models_json, created_at),
    )
    db.commit()
    return get_run(db, run_id)


def get_run(db: sqlite3.Connection, run_id: str) -> Optional[dict]:
    cursor = db.cursor()
    cursor.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,))
    row = cursor.fetchone()
    return dict(row) if row else None


def update_run_status(db: sqlite3.Connection, run_id: str, status: str, **kwargs) -> None:
    fields = ["status"] + list(kwargs.keys())
    values = [status] + list(kwargs.values()) + [run_id]
    set_clause = ", ".join(f"{f} = ?" for f in fields)
    cursor = db.cursor()
    cursor.execute(f"UPDATE runs SET {set_clause} WHERE run_id = ?", values)
    db.commit()


def list_runs(db: sqlite3.Connection, limit: int = 50) -> list[dict]:
    cursor = db.cursor()
    cursor.execute("SELECT * FROM runs ORDER BY created_at DESC LIMIT ?", (limit,))
    return [dict(row) for row in cursor.fetchall()]


def reset_stale_running_runs(
    db: sqlite3.Connection, stale_threshold_seconds: int = 30
) -> int:
    """Reset runs stuck in 'running' status past the stale threshold.

    A run is considered stale if:
    - status = 'running', AND
    - last_heartbeat_at IS NULL, OR
    - last_heartbeat_at is older than the threshold.

    Returns the number of rows recovered.
    """
    cursor = db.cursor()
    cursor.execute(
        """
        UPDATE runs
        SET status = 'queued', started_at = NULL, current_stage = 'queued'
        WHERE status = 'running'
          AND (
            last_heartbeat_at IS NULL
            OR datetime(last_heartbeat_at) < datetime('now', ?)
          )
        """,
        (f'-{stale_threshold_seconds} seconds',),
    )
    db.commit()
    return cursor.rowcount


def claim_next_run(db: sqlite3.Connection) -> Optional[dict]:
    """Atomically claim the next queued run. Returns None if no run available."""
    cursor = db.cursor()
    # Use BEGIN IMMEDIATE for atomic claim
    cursor.execute("BEGIN IMMEDIATE")
    try:
        cursor.execute(
            """
            SELECT run_id FROM runs
            WHERE status = 'queued'
            ORDER BY created_at ASC
            LIMIT 1
            """
        )
        row = cursor.fetchone()
        if row is None:
            db.commit()
            return None
        run_id = row["run_id"]
        cursor.execute(
            """
            UPDATE runs SET status = 'running', started_at = ?
            WHERE run_id = ? AND status = 'queued'
            """,
            (None, run_id),  # started_at set by worker when it actually begins execution
        )
        if cursor.rowcount == 0:
            # Another worker claimed it first
            db.commit()
            return None
        db.commit()
        return get_run(db, run_id)
    except Exception:
        db.rollback()
        raise