
from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple
import pandas as pd

from shared.logger.python_main_logger import logger_manager

# ---------------------------------------------------------------------------
# Subsystem 5: Error Event Schema
# ---------------------------------------------------------------------------

class ErrorLevel(str, Enum):
    WARNING = "WARNING"
    ERROR   = "ERROR"
    FATAL   = "FATAL"
    


@dataclass
class ErrorEvent:
    """
    Canonical error event record.
    Subsystem 5 (Error Event Schema) – every cleansing failure produces one.
    Maps directly to silver_meta_etl_error Delta table.
    """
    err_id:        str
    run_id:        str
    job_name:      str
    error_level:   ErrorLevel
    error_message: str
    record_id:     Optional[str] = None   # natural key of the failed record
    raw_json:      Optional[str] = None   # serialized raw record
    err_time:      datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    retry_count:   int = 0

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["error_level"] = self.error_level.value
        d["err_time"] = self.err_time.isoformat()
        return d

    @staticmethod
    def _make_id(run_id: str, record_id: str, message: str) -> str:
        raw = f"{run_id}|{record_id}|{message}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    @classmethod
    def create(
        cls,
        run_id: str,
        job_name: str,
        level: ErrorLevel,
        message: str,
        record: Optional[Dict[str, Any]] = None,
        record_id_fields: Optional[List[str]] = None,
    ) -> "ErrorEvent":
        record = record or {}
        record_id = "|".join(str(record.get(f, "")) for f in (record_id_fields or []))
        err_id = cls._make_id(run_id, record_id, message)
        raw_json = json.dumps(record, default=str, ensure_ascii=False) if record else None
        return cls(
            err_id=err_id,
            run_id=run_id,
            job_name=job_name,
            error_level=level,
            error_message=message,
            record_id=record_id or None,
            raw_json=raw_json,
        )


class ErrorEventLog:
    """
    Collects ErrorEvents during a pipeline run.
    Subsystem 5 + 30 (Problem Escalation).
    """

    def __init__(self, run_id: str, job_name: str, logger: Optional[logging.Logger] = None):
        self.run_id = run_id
        self.job_name = job_name
        self.logger = logger or logger_manager.get_logger(__name__)
        self._events: List[ErrorEvent] = []

    def add(
        self,
        level: ErrorLevel,
        message: str,
        record: Optional[Dict[str, Any]] = None,
        record_id_fields: Optional[List[str]] = None,
    ) -> ErrorEvent:
        evt = ErrorEvent.create(
            run_id=self.run_id,
            job_name=self.job_name,
            level=level,
            message=message,
            record=record,
            record_id_fields=record_id_fields,
        )
        self._events.append(evt)

        # Log to Python logger
        log_fn = {
            ErrorLevel.WARNING: self.logger.warning,
            ErrorLevel.ERROR:   self.logger.error,
            ErrorLevel.FATAL:   self.logger.critical,
        }.get(level, self.logger.error)
        log_fn("[%s] %s | record_id=%s", level.value, message, evt.record_id)

        # Subsystem 30: escalate FATAL immediately
        if level == ErrorLevel.FATAL:
            self._escalate(evt)

        return evt

    def _escalate(self, evt: ErrorEvent) -> None:
        """Subsystem 30: Problem Escalation – hook for alerting (Slack, PagerDuty, etc.)."""
        self.logger.critical(
            "FATAL ERROR ESCALATION | run_id=%s | job=%s | %s",
            evt.run_id, evt.job_name, evt.error_message,
        )
        # TODO: integrate with alerting system (Slack webhook, email, etc.)

    @property
    def events(self) -> List[ErrorEvent]:
        return list(self._events)

    @property
    def has_fatal(self) -> bool:
        return any(e.error_level == ErrorLevel.FATAL for e in self._events)

    @property
    def has_errors(self) -> bool:
        return any(e.error_level in (ErrorLevel.ERROR, ErrorLevel.FATAL) for e in self._events)

    def summary(self) -> Dict[str, int]:
        counts: Dict[str, int] = {level.value: 0 for level in ErrorLevel}
        for e in self._events:
            counts[e.error_level.value] += 1
        return counts

    def to_records(self) -> List[Dict[str, Any]]:
        return [e.to_dict() for e in self._events]

    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame(self.to_records()) if self._events else pd.DataFrame()