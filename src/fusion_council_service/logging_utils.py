"""JSON-lines logging for fusion-council-service."""

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Optional


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "level": record.levelname,
            "component": record.name,
            "message": record.getMessage(),
        }
        if hasattr(record, "run_id") and record.run_id:
            entry["run_id"] = record.run_id
        if hasattr(record, "event_type") and record.event_type:
            entry["event_type"] = record.event_type
        candidate_id = getattr(record, "candidate_id", "")
        if candidate_id:
            entry["candidate_id"] = candidate_id
        structured_fields = getattr(record, "structured_fields", {}) or {}
        for key, value in structured_fields.items():
            if value is not None and key not in entry:
                entry[key] = value
        return json.dumps(entry, ensure_ascii=False, default=str)


class SafeLogger:
    """Logger that attaches structured context to every log entry."""

    def __init__(self, name: str):
        self._logger = logging.getLogger(name)

    def _log(
        self,
        level: int,
        msg: str,
        *args,
        run_id: Optional[str] = None,
        event_type: Optional[str] = None,
        candidate_id: Optional[str] = None,
        **fields: Any,
    ):
        record = self._logger.makeRecord(
            self._logger.name, level, "(unknown)", 0, msg, args, None
        )
        record.run_id = run_id or ""
        record.event_type = event_type or ""
        record.candidate_id = candidate_id or ""
        record.structured_fields = fields
        self._logger.handle(record)

    def info(self, msg: str, *args, **kwargs):
        self._log(logging.INFO, msg, *args, **kwargs)

    def warning(self, msg: str, *args, **kwargs):
        self._log(logging.WARNING, msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs):
        self._log(logging.ERROR, msg, *args, **kwargs)


def setup_logging() -> None:
    """Configure root logger for JSON-lines output."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(logging.INFO)


def get_logger(name: str) -> SafeLogger:
    return SafeLogger(name)
