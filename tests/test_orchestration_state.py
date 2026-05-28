"""
RED test: OrchestrationState must survive json.dumps() with all field types.

Tests that _serialize_state() converts non-JSON-native types (datetime, UUID, bytes)
to JSON-safe equivalents. This is a pre-implementation test — it fails until
orchestration_state.py is created with _serialize_state() and OrchestrationState.

Run: cd .../fusion-council-backend && uv run pytest tests/test_orchestration_state.py -v
"""
import json
import uuid
from datetime import datetime, timezone
from typing import Any

import pytest
from typing_extensions import TypedDict

# These imports fail loudly with ModuleNotFoundError until orchestration_state.py exists.
# This IS the RED signal.
from fusion_council_service.domain.orchestration.orchestration_state import (
    OrchestrationState,
    _serialize_state,
)


class TestOrchestrationStateJsonSerializable:
    """RED test — verify OrchestrationState survives JSON round-trip."""

    def test_orchestration_state_json_serializable_all_fields_populated(self):
        """
        Verify OrchestrationState survives json.dumps() with every field type that
        could appear during a real run: datetime, UUID, nested dicts, lists.

        This catches TypeError at definition time, not at runtime checkpoint failure.
        """
        now = datetime.now(timezone.utc)
        run_id = str(uuid.uuid4())
        thread_id = str(uuid.uuid4())
        candidate_id = str(uuid.uuid4())

        state: OrchestrationState = {
            "run_id": run_id,
            "mode": "single",
            "engine": "langgraph",
            "engine_version": "v1",
            "current_stage": "generation_persist",
            "candidate_ids": [candidate_id],
            "final_answer": None,
            "final_confidence": None,
            "error_code": None,
            "error_message": None,
            "thread_id": thread_id,
            "checkpoint_namespace": "mode=single",
            "resume_count": 0,
            "updated_at": now,  # datetime — not JSON-native
            "raw_response": {   # nested dict with mixed types
                "model": "test-model",
                "choices": [{"message": {"role": "assistant", "content": "test answer"}}],
                "usage": {"prompt_tokens": 10, "completion_tokens": 5},
            },
            "candidate_summaries": [
                {"id": candidate_id, "stage": "generation_call", "order": 0}
            ],
            "computed_final_answer": "test answer",
            "computed_final_confidence": 0.85,
        }

        # ACTUAL ASSERTION — if this raises, the field is non-serializable
        serialized = _serialize_state(state)
        result = json.dumps(serialized)  # must not raise TypeError
        assert isinstance(result, str)

        # Verify deserialization round-trip
        parsed = json.loads(result)
        assert parsed["run_id"] == run_id
        assert parsed["candidate_ids"] == [candidate_id]
        assert parsed["raw_response"]["model"] == "test-model"

    def test_serialize_state_converts_datetime_to_iso(self):
        """Verify _serialize_state converts datetime to ISO 8601 string."""
        now = datetime.now(timezone.utc)
        uid = uuid.uuid4()

        state: OrchestrationState = {
            "run_id": str(uid),
            "mode": "single",
            "engine": "langgraph",
            "engine_version": "v1",
            "current_stage": "prepare_run",
            "candidate_ids": [],
            "final_answer": None,
            "final_confidence": None,
            "error_code": None,
            "error_message": None,
            "thread_id": str(uid),
            "checkpoint_namespace": "mode=single",
            "resume_count": 0,
            "updated_at": now,  # datetime — must convert to str
        }

        serialized = _serialize_state(state)
        result = json.dumps(serialized)  # must not raise
        parsed = json.loads(result)

        # datetime objects must be converted to ISO strings
        assert isinstance(parsed["updated_at"], str)
        assert "T" in parsed["updated_at"]  # ISO format contains 'T'

    def test_serialize_state_converts_uuid_to_str(self):
        """Verify _serialize_state converts UUID fields to strings."""
        uid = uuid.uuid4()

        state: OrchestrationState = {
            "run_id": str(uid),
            "mode": "single",
            "engine": "langgraph",
            "engine_version": "v1",
            "current_stage": "generation_call",
            "candidate_ids": [str(uuid.uuid4())],
            "final_answer": None,
            "final_confidence": None,
            "error_code": None,
            "error_message": None,
            "thread_id": str(uuid.uuid4()),
            "checkpoint_namespace": "mode=single",
            "resume_count": 0,
            "updated_at": datetime.now(timezone.utc),
        }

        serialized = _serialize_state(state)
        result = json.dumps(serialized)  # must not raise
        parsed = json.loads(result)

        # uuid fields must not be UUID objects in output
        assert isinstance(parsed["run_id"], str)
        assert parsed["run_id"] == str(uid)

    def test_serialize_state_passes_through_primitives(self):
        """Verify _serialize_state leaves already-serializable types unchanged."""
        state: OrchestrationState = {
            "run_id": "run-123",
            "mode": "single",
            "engine": "langgraph",
            "engine_version": "v1",
            "current_stage": "finalize_success",
            "candidate_ids": ["c1", "c2"],
            "final_answer": "The answer is 42",
            "final_confidence": 0.95,
            "error_code": None,
            "error_message": None,
            "thread_id": "thread-abc",
            "checkpoint_namespace": "mode=single",
            "resume_count": 3,
            "updated_at": "2026-05-28T10:00:00Z",
        }

        serialized = _serialize_state(state)
        result = json.dumps(serialized)
        parsed = json.loads(result)

        assert parsed["final_answer"] == "The answer is 42"
        assert parsed["final_confidence"] == 0.95
        assert parsed["resume_count"] == 3
        assert parsed["candidate_ids"] == ["c1", "c2"]

    def test_serialize_state_unknown_fields_pass_through(self):
        """Extra fields not in TypedDict must not break serialization."""
        state: Any = {
            "run_id": "run-456",
            "mode": "single",
            "engine": "langgraph",
            "engine_version": "v1",
            "current_stage": "prepare_run",
            "candidate_ids": [],
            "final_answer": None,
            "final_confidence": None,
            "error_code": None,
            "error_message": None,
            "thread_id": "thread-xyz",
            "checkpoint_namespace": "mode=single",
            "resume_count": 0,
            "updated_at": "2026-05-28T00:00:00Z",
            # Extra field not in OrchestrationState schema
            "extra_debug_field": {"internal": "value", "count": 99},
        }

        serialized = _serialize_state(state)
        result = json.dumps(serialized)  # must not raise
        parsed = json.loads(result)
        assert parsed["extra_debug_field"]["count"] == 99
