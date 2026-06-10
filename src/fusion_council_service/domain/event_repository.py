"""Event repository — durable run-event persistence and replay helpers."""

from __future__ import annotations

import json
from typing import Any

from fusion_council_service.clock import utc_now_iso
from fusion_council_service.db import commit_tx, execute_sql, execute_sql_all, execute_sql_one
from fusion_council_service.ids import new_event_id


def get_next_seq(db, run_id: str) -> int:
    """Return the next 1-based sequence number for a run."""
    row = execute_sql_one(
        db,
        """
        SELECT COALESCE(MAX(seq), 0) + 1 AS next_seq
        FROM run_events
        WHERE run_id = :run_id
        """,
        {"run_id": run_id},
    )
    return int(row["next_seq"]) if row and row.get("next_seq") is not None else 1


def _payload_to_json(payload: Any) -> tuple[str, dict[str, Any]]:
    if isinstance(payload, str):
        try:
            parsed = json.loads(payload) if payload else {}
        except json.JSONDecodeError:
            parsed = {"raw_payload": payload}
        return payload, parsed

    if payload is None:
        return "{}", {}

    if isinstance(payload, dict):
        return json.dumps(payload, ensure_ascii=False), payload

    serialized = json.dumps(payload, ensure_ascii=False)
    try:
        parsed = json.loads(serialized)
    except json.JSONDecodeError:
        parsed = {"raw_payload": serialized}
    return serialized, parsed


def append_event(
    db,
    run_id: str,
    event_type: str,
    payload: Any,
    seq: int | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    """Persist one run event and return its replay envelope."""
    seq = get_next_seq(db, run_id) if seq is None else seq
    created_at = created_at or utc_now_iso()
    payload_json, payload_obj = _payload_to_json(payload)
    event_id = new_event_id()

    execute_sql(
        db,
        """
        INSERT INTO run_events (event_id, run_id, seq, event_type, payload_json, created_at)
        VALUES (:event_id, :run_id, :seq, :event_type, :payload_json, :created_at)
        """,
        {
            "event_id": event_id,
            "run_id": run_id,
            "seq": seq,
            "event_type": event_type,
            "payload_json": payload_json,
            "created_at": created_at,
        },
    )
    commit_tx(db)

    return {
        "event_id": event_id,
        "run_id": run_id,
        "seq": seq,
        "event_type": event_type,
        "created_at": created_at,
        "payload": payload_obj,
    }


def list_events_for_run(db, run_id: str, after_seq: int = 0):
    """Return raw event rows for a run, ordered by seq ascending."""
    return execute_sql_all(
        db,
        """
        SELECT * FROM run_events
        WHERE run_id = :run_id AND seq > :after_seq
        ORDER BY seq ASC
        """,
        {"run_id": run_id, "after_seq": after_seq},
    )


def _row_to_event_envelope(row: dict[str, Any]) -> dict[str, Any]:
    payload_raw = row.get("payload_json")
    try:
        payload = json.loads(payload_raw) if payload_raw else {}
    except json.JSONDecodeError:
        payload = {"raw_payload": payload_raw}

    return {
        "seq": row["seq"],
        "run_id": row.get("run_id"),
        "event_type": row["event_type"],
        "created_at": row.get("created_at"),
        "payload": payload,
    }


def list_event_envelopes_for_run(db, run_id: str, after_seq: int = 0):
    return [_row_to_event_envelope(row) for row in list_events_for_run(db, run_id, after_seq)]
