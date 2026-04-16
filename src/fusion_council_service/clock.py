"""Clock helpers for consistent UTC ISO-8601 timestamps."""

from datetime import datetime, timezone, timedelta


def utc_now_iso() -> str:
    """Return current UTC time as ISO-8601 string (no microseconds)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def utc_now_plus_seconds(seconds: int) -> str:
    """Return UTC time `seconds` seconds from now as ISO-8601 string."""
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")


def iso_now() -> str:
    """Alias for utc_now_iso for convenience."""
    return utc_now_iso()