"""JSON-lines logging for fusion-council-service."""

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Optional


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
        return json.dumps(entry, ensure_ascii=False)


class SafeLogger:
    """Logger that attaches run_id and event_type to every log entry."""

    def __init__(self, name: str):
        self._logger = logging.getLogger(name)

    def _log(self, level: int, msg: str, run_id: Optional[str] = None, event_type: Optional[str] = None):
        record = self._logger.makeRecord(
            self._logger.name, level, "(unknown)", 0, msg, (), None
        )
        record.run_id = run_id or ""
        record.event_type = event_type or ""
        self._logger.handle(record)

    def info(self, msg: str, **kwargs):
        self._log(logging.INFO, msg, **kwargs)

    def warning(self, msg: str, **kwargs):
        self._log(logging.WARNING, msg, **kwargs)

    def error(self, msg: str, **kwargs):
        self._log(logging.ERROR, msg, **kwargs)


def setup_logging() -> None:
    """Configure root logger for JSON-lines output."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(logging.INFO)


def get_logger(name: str) -> SafeLogger:
    return SafeLogger(name)