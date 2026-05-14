"""Run repository — CRUD operations on the runs table."""

from typing import Optional

from fusion_council_service.db import (
    begin_immediate,
    commit_tx,
    execute_sql_all,
    execute_sql_one,
    execute_sql,
    is_postgresql,
    rollback_tx,
)


def insert_run(
    db,
    run_id: str,
    mode: str,
    prompt: str,
    system_prompt: Optional[str],
    temperature: float,
    max_output_tokens: int,
    deadline_seconds: int,
    deadline_at: str,
    owner_token_hash: str,
    metadata_json: Optional[str],
    requested_models_json: Optional[str],
    created_at: str,
) -> Optional[dict]:
    execute_sql(
        db,
        """
        INSERT INTO runs (run_id, mode, prompt, system_prompt, temperature, max_output_tokens,
                         deadline_seconds, deadline_at, status, owner_token_hash,
                         metadata_json, requested_models_json, created_at)
        VALUES (:run_id, :mode, :prompt, :system_prompt, :temperature, :max_output_tokens,
                :deadline_seconds, :deadline_at, 'queued', :owner_token_hash,
                :metadata_json, :requested_models_json, :created_at)
        """,
        {
            "run_id": run_id,
            "mode": mode,
            "prompt": prompt,
            "system_prompt": system_prompt,
            "temperature": temperature,
            "max_output_tokens": max_output_tokens,
            "deadline_seconds": deadline_seconds,
            "deadline_at": deadline_at,
            "owner_token_hash": owner_token_hash,
            "metadata_json": metadata_json,
            "requested_models_json": requested_models_json,
            "created_at": created_at,
        },
    )
    commit_tx(db)
    return get_run(db, run_id)


def get_run(db, run_id: str) -> Optional[dict]:
    return execute_sql_one(
        db,
        "SELECT * FROM runs WHERE run_id = :run_id",
        {"run_id": run_id},
    )


def update_run_status(db, run_id: str, status: str, **kwargs) -> None:
    fields = ["status"] + list(kwargs.keys())
    values = {"status": status, **kwargs, "run_id": run_id}
    set_clause = ", ".join(f"{f} = :{f}" for f in fields)
    execute_sql(
        db,
        f"UPDATE runs SET {set_clause} WHERE run_id = :run_id",
        values,
    )
    commit_tx(db)


def list_runs(db, limit: int = 50) -> list[dict]:
    return execute_sql_all(
        db,
        "SELECT * FROM runs ORDER BY created_at DESC LIMIT :limit",
        {"limit": limit},
    )


def reset_stale_running_runs(
    db, stale_threshold_seconds: int = 30
) -> int:
    """Reset runs stuck in 'running' status past the stale threshold.

    A run is considered stale if:
    - status = 'running', AND
    - last_heartbeat_at IS NULL, OR
    - last_heartbeat_at is older than the threshold.

    Returns the number of rows recovered.
    """
    if is_postgresql():
        result = execute_sql(
            db,
            """
            UPDATE runs
            SET status = 'queued', started_at = NULL, current_stage = 'queued'
            WHERE status = 'running'
              AND (
                last_heartbeat_at IS NULL
                OR last_heartbeat_at::timestamp < NOW() - (:threshold || ' seconds')::interval
              )
            """,
            {"threshold": str(stale_threshold_seconds)},
        )
    else:
        result = execute_sql(
            db,
            """
            UPDATE runs
            SET status = 'queued', started_at = NULL, current_stage = 'queued'
            WHERE status = 'running'
              AND (
                last_heartbeat_at IS NULL
                OR datetime(last_heartbeat_at) < datetime('now', :threshold)
              )
            """,
            {"threshold": f"-{stale_threshold_seconds} seconds"},
        )
    commit_tx(db)
    return result.rowcount


def claim_next_run(db) -> Optional[dict]:
    """Atomically claim the next queued run. Returns None if no run available."""
    if is_postgresql():
        begin_immediate(db)
        try:
            result = execute_sql_one(
                db,
                """
                SELECT run_id FROM runs
                WHERE status = 'queued'
                ORDER BY created_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """,
            )
            if result is None:
                commit_tx(db)
                return None
            run_id = result["run_id"]
            execute_sql(
                db,
                """
                UPDATE runs SET status = 'running', started_at = :started_at
                WHERE run_id = :run_id AND status = 'queued'
                """,
                {"run_id": run_id, "started_at": None},
            )
            commit_tx(db)
            return get_run(db, run_id)
        except Exception:
            rollback_tx(db)
            raise
    else:
        # SQLite: use BEGIN IMMEDIATE for atomic claim
        db.execute("BEGIN IMMEDIATE")
        try:
            cursor = db.execute(
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
            db.execute(
                """
                UPDATE runs SET status = 'running', started_at = ?
                WHERE run_id = ? AND status = 'queued'
                """,
                (None, run_id),
            )
            if cursor.rowcount == 0:
                db.commit()
                return None
            db.commit()
            return get_run(db, run_id)
        except Exception:
            db.rollback()
            raise
