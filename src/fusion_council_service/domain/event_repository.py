"""Event repository — append and list run events."""

from fusion_council_service.db import (
    commit_tx,
    execute_sql_all,
    execute_sql_scalar,
    execute_sql,
)
from fusion_council_service.ids import new_event_id


def append_event(
    db,
    run_id: str,
    event_type: str,
    payload_json: str,
    seq: int,
    created_at: str,
) -> dict:
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
    return {"event_id": event_id, "run_id": run_id, "seq": seq, "event_type": event_type}


def list_events_for_run(db, run_id: str, after_seq: int = 0) -> list[dict]:
    return execute_sql_all(
        db,
        "SELECT * FROM run_events WHERE run_id = :run_id AND seq > :after_seq ORDER BY seq",
        {"run_id": run_id, "after_seq": after_seq},
    )


def get_next_seq(db, run_id: str) -> int:
    """Get the next sequence number for a run's events."""
    result = execute_sql_scalar(
        db,
        "SELECT COALESCE(MAX(seq), 0) + 1 FROM run_events WHERE run_id = :run_id",
        {"run_id": run_id},
    )
    return result if result is not None else 1
