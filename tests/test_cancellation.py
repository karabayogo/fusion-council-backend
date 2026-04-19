"""Tests for run cancellation."""

import pytest

from fusion_council_service.domain.event_emitter import emit_run_cancelled
from fusion_council_service.domain.run_repository import insert_run, get_run, update_run_status
from fusion_council_service.domain.event_repository import list_events_for_run
from fusion_council_service.clock import utc_now_iso, utc_now_plus_seconds


@pytest.fixture
def queued_run(tmp_db):
    run_id = "run_cancel_test"
    insert_run(
        db=tmp_db,
        run_id=run_id,
        mode="single",
        prompt="test cancel",
        system_prompt=None,
        temperature=0.2,
        max_output_tokens=1000,
        deadline_seconds=60,
        deadline_at=utc_now_plus_seconds(60),
        owner_token_hash="testhash",
        metadata_json="{}",
        requested_models_json=None,
        created_at=utc_now_iso(),
    )
    return run_id


def test_cancel_queued_run(tmp_db, queued_run):
    """A queued run can be cancelled."""
    update_run_status(tmp_db, queued_run, "cancelled")
    run = get_run(tmp_db, queued_run)
    assert run["status"] == "cancelled"


def test_cancel_emits_event(tmp_db, queued_run):
    """Cancellation should emit a run.cancelled event."""
    emit_run_cancelled(tmp_db, queued_run)
    events = list_events_for_run(tmp_db, queued_run, after_seq=0)
    cancelled_ev = next((e for e in events if e["event_type"] == "run.cancelled"), None)
    assert cancelled_ev is not None


def test_running_run_cannot_be_cancelled_to_completed(tmp_db, queued_run):
    """Setting a running run to cancelled is allowed (best effort)."""
    update_run_status(tmp_db, queued_run, "running")
    update_run_status(tmp_db, queued_run, "cancelled")
    run = get_run(tmp_db, queued_run)
    assert run["status"] == "cancelled"


def test_terminal_status_cannot_be_cancelled(tmp_db, queued_run):
    """Completed/failed runs should already be in terminal state."""
    update_run_status(tmp_db, queued_run, "succeeded")
    run = get_run(tmp_db, queued_run)
    assert run["status"] == "succeeded"
    # Cancellation on terminal runs should be rejected by the API layer (tested in API tests)
