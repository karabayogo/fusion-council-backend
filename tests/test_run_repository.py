"""Regression tests for run_repository.py — stale run recovery."""

import sqlite3

import pytest

from fusion_council_service.domain.run_repository import (
    insert_run,
    reset_stale_running_runs,
)
from fusion_council_service.clock import utc_now_iso, utc_now_plus_seconds


def _insert_run_with_status(db: sqlite3.Connection, run_id: str, status: str,
                            heartbeat_offset_seconds: int | None = None) -> None:
    """Helper to insert a run and set its status + heartbeat."""
    insert_run(
        db=db, run_id=run_id, mode="single", prompt="test",
        system_prompt=None, temperature=0.2, max_output_tokens=1000,
        deadline_seconds=60, deadline_at=utc_now_plus_seconds(60),
        owner_token_hash="hash", metadata_json="{}", requested_models_json=None,
        created_at=utc_now_iso(),
    )
    # Set status directly via SQL (claim_next_run sets running, but we want direct control)
    db.execute("UPDATE runs SET status = ? WHERE run_id = ?", (status, run_id))
    if heartbeat_offset_seconds is not None:
        db.execute(
            "UPDATE runs SET last_heartbeat_at = datetime('now', ?) WHERE run_id = ?",
            (f'-{heartbeat_offset_seconds} seconds', run_id),
        )
    else:
        # Null heartbeat (never sent)
        db.execute("UPDATE runs SET last_heartbeat_at = NULL WHERE run_id = ?", (run_id,))
    db.commit()


def test_reset_stale_running_resets_old_heartbeat(tmp_db):
    """Run with heartbeat 60s ago should be reset to queued."""
    _insert_run_with_status(tmp_db, "run_stale", "running", heartbeat_offset_seconds=60)
    recovered = reset_stale_running_runs(tmp_db, stale_threshold_seconds=30)
    assert recovered == 1
    cursor = tmp_db.execute("SELECT status FROM runs WHERE run_id = ?", ("run_stale",))
    assert cursor.fetchone()["status"] == "queued"


def test_reset_stale_running_keeps_recent_heartbeat(tmp_db):
    """Run with heartbeat 10s ago should NOT be reset."""
    _insert_run_with_status(tmp_db, "run_recent", "running", heartbeat_offset_seconds=10)
    recovered = reset_stale_running_runs(tmp_db, stale_threshold_seconds=30)
    assert recovered == 0
    cursor = tmp_db.execute("SELECT status FROM runs WHERE run_id = ?", ("run_recent",))
    assert cursor.fetchone()["status"] == "running"


def test_reset_stale_running_resets_null_heartbeat(tmp_db):
    """Run with NULL heartbeat should be reset (never sent heartbeat)."""
    _insert_run_with_status(tmp_db, "run_null", "running", heartbeat_offset_seconds=None)
    recovered = reset_stale_running_runs(tmp_db, stale_threshold_seconds=30)
    assert recovered == 1
    cursor = tmp_db.execute("SELECT status FROM runs WHERE run_id = ?", ("run_null",))
    assert cursor.fetchone()["status"] == "queued"


def test_reset_stale_running_ignores_other_statuses(tmp_db):
    """Succeeded runs should NOT be touched."""
    _insert_run_with_status(tmp_db, "run_succeeded", "succeeded", heartbeat_offset_seconds=60)
    recovered = reset_stale_running_runs(tmp_db, stale_threshold_seconds=30)
    assert recovered == 0
    cursor = tmp_db.execute("SELECT status FROM runs WHERE run_id = ?", ("run_succeeded",))
    assert cursor.fetchone()["status"] == "succeeded"


def test_reset_stale_running_count_matches(tmp_db):
    """Multiple stale runs should all be recovered."""
    for i in range(3):
        _insert_run_with_status(tmp_db, f"run_stale_{i}", "running", heartbeat_offset_seconds=60)
    recovered = reset_stale_running_runs(tmp_db, stale_threshold_seconds=30)
    assert recovered == 3
