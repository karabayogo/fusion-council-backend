"""Tests for SSE event streaming."""

import json
import pytest

from fusion_council_service.domain.event_emitter import (
    emit_run_accepted,
    emit_run_completed,
    emit_run_failed,
    emit_run_started,
    emit_stage_started,
    emit_heartbeat,
)
from fusion_council_service.domain.event_repository import (
    list_events_for_run,
    get_next_seq,
)
from fusion_council_service.domain.run_repository import insert_run
from fusion_council_service.clock import utc_now_iso, utc_now_plus_seconds


@pytest.fixture
def run_with_events(tmp_db):
    run_id = "run_sse_test"
    insert_run(
        db=tmp_db,
        run_id=run_id,
        mode="single",
        prompt="test",
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


def test_emit_run_accepted(tmp_db, run_with_events):
    ev = emit_run_accepted(tmp_db, run_with_events, "single", 60)
    assert ev["event_type"] == "run.accepted"
    assert ev["run_id"] == run_with_events


def test_emit_run_started(tmp_db, run_with_events):
    ev = emit_run_started(tmp_db, run_with_events, "single")
    assert ev["event_type"] == "run.started"


def test_emit_stage_started(tmp_db, run_with_events):
    ev = emit_stage_started(tmp_db, run_with_events, "generation", ["model-a"])
    assert ev["event_type"] == "stage.started"


def test_emit_heartbeat(tmp_db, run_with_events):
    ev = emit_heartbeat(tmp_db, run_with_events, "generation")
    assert ev["event_type"] == "heartbeat"


def test_emit_run_completed(tmp_db, run_with_events):
    ev = emit_run_completed(tmp_db, run_with_events, "The answer is 2", confidence=0.9)
    assert ev["event_type"] == "run.completed"


def test_emit_run_failed(tmp_db, run_with_events):
    ev = emit_run_failed(tmp_db, run_with_events, "NO_MODELS", "No enabled models")
    assert ev["event_type"] == "run.failed"


def test_events_ordered_by_seq(tmp_db, run_with_events):
    """Events should be stored with incrementing sequence numbers."""
    seq1 = get_next_seq(tmp_db, run_with_events)
    ev1 = emit_run_started(tmp_db, run_with_events, "single")
    assert ev1["seq"] == seq1

    seq2 = get_next_seq(tmp_db, run_with_events)
    ev2 = emit_stage_started(tmp_db, run_with_events, "generation", [])
    assert ev2["seq"] == seq2
    assert ev2["seq"] > ev1["seq"]


def test_list_events_for_run_with_after_seq(tmp_db, run_with_events):
    """list_events_for_run should filter by seq correctly."""
    emit_run_started(tmp_db, run_with_events, "single")
    emit_stage_started(tmp_db, run_with_events, "generation", [])

    events_after_0 = list_events_for_run(tmp_db, run_with_events, after_seq=0)
    events_after_1 = list_events_for_run(tmp_db, run_with_events, after_seq=1)

    assert len(events_after_0) >= 2
    assert len(events_after_1) == 1  # only stage.started (seq=2) is > 1
    assert events_after_1[0]["seq"] == 2


def test_sse_event_payload_parsed(tmp_db, run_with_events):
    """Event payload should be stored as JSON and retrievable."""
    emit_run_completed(tmp_db, run_with_events, "final answer", confidence=0.95)
    events = list_events_for_run(tmp_db, run_with_events, after_seq=0)
    completed = next(e for e in events if e["event_type"] == "run.completed")
    payload = json.loads(completed["payload_json"])
    assert payload["final_answer"] == "final answer"
    assert payload["confidence"] == 0.95
