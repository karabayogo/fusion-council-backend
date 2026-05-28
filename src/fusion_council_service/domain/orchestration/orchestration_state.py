"""LangGraph state schema for orchestration checkpointing."""
import base64
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from typing_extensions import TypedDict


class OrchestrationState(TypedDict, total=False):
    """LangGraph state for the orchestration checkpointing system.

    All fields use JSON-native primitives only (str, int, float, bool, list, dict, None).
    No datetime, UUID, or bytes objects — those are converted by _serialize_state().

    Canonical source: plan doc Section Phase 4 — Required state fields (schema).
    """

    run_id: str
    mode: str  # "single" | "fusion" | "council"
    engine: str  # "langgraph"
    engine_version: str  # e.g. "v1"

    # ---- checkpoint routing ----
    thread_id: str       # uuid4 str — unique per run
    checkpoint_namespace: str  # e.g. "mode=single" — MUST NOT be "checkpoint_ns" (reserved by LangGraph)
    resume_count: int    # number of times this run was resumed from checkpoint

    # ---- stage tracking ----
    current_stage: str
    # values: "prepare_run" | "generation_call" | "generation_persist" |
    #         "finalize_success" | "finalize_failure"

    # ---- generation output ----
    candidate_ids: list[str]  # UUID strings only — full data lives in run_candidates table
    current_candidate_id: Optional[str]  # ephemeral — set by caller between node steps; NOT persisted to DB
    final_answer: Optional[str]
    final_confidence: Optional[float]

    # ---- error handling ----
    error_code: Optional[str]
    error_message: Optional[str]

    # ---- internals (present in raw LangGraph state, not in DB schema) ----
    updated_at: str  # ISO 8601 after _serialize_state()
    raw_response: Optional[dict[str, Any]]
    candidate_summaries: Optional[list[dict[str, Any]]]
    computed_final_answer: Optional[str]
    computed_final_confidence: Optional[float]


def _serialize_state(state: OrchestrationState) -> dict[str, Any]:
    """
    Convert all non-JSON-native types to JSON-safe equivalents.

    Called before EVERY saver.put() call in the checkpoint write path.
    This is the single point of serialization for LangGraph checkpointing.

    Converts:
      datetime → ISO 8601 string
      UUID     → str
      bytes    → base64 string
      other    → unchanged (pass-through for primitives)

    CRITICAL: A dumber LLM will define this function but forget to call it.
    The caller is responsible for invoking this before every checkpoint write.
    """
    result: dict[str, Any] = {}

    for key, value in state.items():
        if value is None:
            result[key] = None
        elif isinstance(value, datetime):
            # Naive datetimes are assumed UTC.
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            result[key] = value.isoformat()
        elif isinstance(value, uuid.UUID):
            result[key] = str(value)
        elif isinstance(value, bytes):
            result[key] = base64.b64encode(value).decode("ascii")
        elif isinstance(value, dict):
            result[key] = _serialize_state(value)  # recursive
        elif isinstance(value, list):
            result[key] = [
                _serialize_state({"_v": item})["_v"]  # recursive item handling
                if isinstance(item, (dict, datetime, uuid.UUID))
                else item
                for item in value
            ]
        else:
            # Primitives (str, int, float, bool) pass through unchanged.
            result[key] = value

    return result
