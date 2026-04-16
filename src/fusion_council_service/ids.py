"""ID generation for runs, events, and candidates."""

import uuid


def new_run_id() -> str:
    return f"run_{uuid.uuid4().hex[:24]}"


def new_event_id() -> str:
    return f"evt_{uuid.uuid4().hex[:24]}"


def new_candidate_id() -> str:
    return f"cand_{uuid.uuid4().hex[:20]}"