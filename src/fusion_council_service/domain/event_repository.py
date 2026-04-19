"""Event repository — append and list run events."""

import sqlite3

from fusion_council_service.ids import new_event_id


def append_event(
    db: sqlite3.Connection,
    run_id: str,
    event_type: str,
    payload_json: str,
    seq: int,
    created_at: str,
) -> dict:
    event_id = new_event_id()
    cursor = db.cursor()
    cursor.execute(
        """
        INSERT INTO run_events (event_id, run_id, seq, event_type, payload_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (event_id, run_id, seq, event_type, payload_json, created_at),
    )
    db.commit()
    return {"event_id": event_id, "run_id": run_id, "seq": seq, "event_type": event_type}


def list_events_for_run(db: sqlite3.Connection, run_id: str, after_seq: int = 0) -> list[dict]:
    cursor = db.cursor()
    cursor.execute(
        "SELECT * FROM run_events WHERE run_id = ? AND seq > ? ORDER BY seq",
        (run_id, after_seq),
    )
    return [dict(row) for row in cursor.fetchall()]


def get_next_seq(db: sqlite3.Connection, run_id: str) -> int:
    """Get the next sequence number for a run's events."""
    cursor = db.cursor()
    cursor.execute(
        "SELECT COALESCE(MAX(seq), 0) + 1 FROM run_events WHERE run_id = ?",
        (run_id,),
    )
    return cursor.fetchone()[0]