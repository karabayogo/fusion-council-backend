"""Tests for worker recovery after failures."""

import json
import pytest

from fusion_council_service.domain.candidate_repository import insert_candidate, list_candidates_for_run
from fusion_council_service.domain.run_repository import insert_run, get_run, update_run_status, claim_next_run
from fusion_council_service.domain.event_emitter import emit_run_failed
from fusion_council_service.clock import utc_now_iso, utc_now_plus_seconds


def test_claim_next_run_returns_queued(tmp_db):
    """claim_next_run should return the oldest queued run."""
    run_id_1 = "run_queued_1"
    run_id_2 = "run_queued_2"
    insert_run(
        db=tmp_db, run_id=run_id_1, mode="single", prompt="test1",
        system_prompt=None, temperature=0.2, max_output_tokens=1000,
        deadline_seconds=60, deadline_at=utc_now_plus_seconds(60),
        owner_token_hash="hash", metadata_json="{}", requested_models_json=None,
        created_at=utc_now_iso(),
    )
    insert_run(
        db=tmp_db, run_id=run_id_2, mode="single", prompt="test2",
        system_prompt=None, temperature=0.2, max_output_tokens=1000,
        deadline_seconds=60, deadline_at=utc_now_plus_seconds(60),
        owner_token_hash="hash", metadata_json="{}", requested_models_json=None,
        created_at=utc_now_iso(),
    )

    # Claim first run
    claimed = claim_next_run(tmp_db)
    assert claimed is not None
    assert claimed["run_id"] == run_id_1
    assert claimed["status"] == "running"


def test_claim_next_run_returns_none_when_empty(tmp_db):
    """claim_next_run returns None when no queued runs exist."""
    result = claim_next_run(tmp_db)
    assert result is None


def test_claim_next_run_skips_running(tmp_db):
    """claim_next_run should skip runs that are already running."""
    insert_run(
        db=tmp_db, run_id="run_running", mode="single", prompt="test",
        system_prompt=None, temperature=0.2, max_output_tokens=1000,
        deadline_seconds=60, deadline_at=utc_now_plus_seconds(60),
        owner_token_hash="hash", metadata_json="{}", requested_models_json=None,
        created_at=utc_now_iso(),
    )
    update_run_status(tmp_db, "run_running", "running")

    result = claim_next_run(tmp_db)
    assert result is None


def test_failed_run_does_not_block_claim(tmp_db):
    """After a run fails, claim_next_run should return the next queued run."""
    # Insert two runs
    for run_id in ["run_fail_1", "run_next"]:
        insert_run(
            db=tmp_db, run_id=run_id, mode="single", prompt="test",
            system_prompt=None, temperature=0.2, max_output_tokens=1000,
            deadline_seconds=60, deadline_at=utc_now_plus_seconds(60),
            owner_token_hash="hash", metadata_json="{}", requested_models_json=None,
            created_at=utc_now_iso(),
        )

    # Fail the first run
    update_run_status(tmp_db, "run_fail_1", "failed", error_code="TEST_FAIL", error_message="Test failure")

    # Claim the next one
    claimed = claim_next_run(tmp_db)
    assert claimed is not None
    assert claimed["run_id"] == "run_next"


def test_worker_continues_after_one_bad_job(tmp_db, mock_failed_provider_result):
    """Worker should continue processing after a failed run."""
    from fusion_council_service.domain.candidate_repository import insert_candidate
    from fusion_council_service.ids import new_candidate_id

    # First run: fails
    insert_run(
        db=tmp_db, run_id="run_fail", mode="single", prompt="test",
        system_prompt=None, temperature=0.2, max_output_tokens=1000,
        deadline_seconds=60, deadline_at=utc_now_plus_seconds(60),
        owner_token_hash="hash", metadata_json="{}", requested_models_json=None,
        created_at=utc_now_iso(),
    )
    update_run_status(tmp_db, "run_fail", "failed", error_code="PROVIDER_ERROR")

    # Second run: queued
    insert_run(
        db=tmp_db, run_id="run_ok", mode="single", prompt="test",
        system_prompt=None, temperature=0.2, max_output_tokens=1000,
        deadline_seconds=60, deadline_at=utc_now_plus_seconds(60),
        owner_token_hash="hash", metadata_json="{}", requested_models_json=None,
        created_at=utc_now_iso(),
    )

    # Simulate worker claiming next run
    claimed = claim_next_run(tmp_db)
    assert claimed["run_id"] == "run_ok"
    # The first failed run did not prevent claiming
    assert claimed["status"] == "running"
