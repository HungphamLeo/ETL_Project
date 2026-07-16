"""
Unit tests – Subsystem 5: ErrorEvent & ErrorEventLog
"""
from __future__ import annotations

import json
import logging
import pytest

from platforms.processing.base_processing_subsystem.subsystem5_and_30_error_event_schema_and_escalate import (
    ErrorEvent,
    ErrorEventLog,
    ErrorLevel,
)


# ---------------------------------------------------------------------------
# ErrorEvent
# ---------------------------------------------------------------------------

class TestErrorEventCreate:
    def test_basic_creation(self):
        evt = ErrorEvent.create(
            run_id="run_01",
            job_name="ingestion",
            level=ErrorLevel.WARNING,
            message="symbol is null",
        )
        assert evt.run_id == "run_01"
        assert evt.job_name == "ingestion"
        assert evt.error_level == ErrorLevel.WARNING
        assert evt.error_message == "symbol is null"
        assert evt.record_id is None
        assert evt.raw_json is None

    def test_record_id_computed_from_fields(self):
        record = {"symbol": "FPT", "date": "2024-01-01"}
        evt = ErrorEvent.create(
            run_id="r",
            job_name="j",
            level=ErrorLevel.ERROR,
            message="bad data",
            record=record,
            record_id_fields=["symbol", "date"],
        )
        assert evt.record_id == "FPT|2024-01-01"

    def test_raw_json_is_serialised(self):
        record = {"symbol": "FPT"}
        evt = ErrorEvent.create(
            run_id="r",
            job_name="j",
            level=ErrorLevel.WARNING,
            message="msg",
            record=record,
        )
        assert evt.raw_json is not None
        parsed = json.loads(evt.raw_json)
        assert parsed["symbol"] == "FPT"

    def test_err_id_is_deterministic(self):
        evt1 = ErrorEvent.create("r", "j", ErrorLevel.ERROR, "msg")
        evt2 = ErrorEvent.create("r", "j", ErrorLevel.ERROR, "msg")
        assert evt1.err_id == evt2.err_id

    def test_err_id_changes_with_different_inputs(self):
        evt1 = ErrorEvent.create("r1", "j", ErrorLevel.ERROR, "msg")
        evt2 = ErrorEvent.create("r2", "j", ErrorLevel.ERROR, "msg")
        assert evt1.err_id != evt2.err_id

    def test_to_dict_error_level_is_string(self):
        evt = ErrorEvent.create("r", "j", ErrorLevel.FATAL, "critical")
        d = evt.to_dict()
        assert d["error_level"] == "FATAL"
        assert isinstance(d["err_time"], str)


# ---------------------------------------------------------------------------
# ErrorEventLog
# ---------------------------------------------------------------------------

class TestErrorEventLog:
    def _make_log(self):
        return ErrorEventLog(run_id="run_test", job_name="test_job")

    def test_add_returns_event(self):
        log = self._make_log()
        evt = log.add(ErrorLevel.WARNING, "test warning")
        assert isinstance(evt, ErrorEvent)

    def test_events_accumulate(self):
        log = self._make_log()
        log.add(ErrorLevel.WARNING, "w1")
        log.add(ErrorLevel.ERROR, "e1")
        assert len(log.events) == 2

    def test_has_fatal_false_when_no_fatal(self):
        log = self._make_log()
        log.add(ErrorLevel.ERROR, "error")
        assert not log.has_fatal

    def test_has_fatal_true_when_fatal_added(self):
        log = self._make_log()
        log.add(ErrorLevel.FATAL, "critical!")
        assert log.has_fatal

    def test_has_errors_true_for_error_level(self):
        log = self._make_log()
        log.add(ErrorLevel.ERROR, "err")
        assert log.has_errors

    def test_has_errors_false_for_warning_only(self):
        log = self._make_log()
        log.add(ErrorLevel.WARNING, "warn")
        assert not log.has_errors

    def test_summary_counts_by_level(self):
        log = self._make_log()
        log.add(ErrorLevel.WARNING, "w1")
        log.add(ErrorLevel.WARNING, "w2")
        log.add(ErrorLevel.ERROR, "e1")
        summary = log.summary()
        assert summary["WARNING"] == 2
        assert summary["ERROR"] == 1
        assert summary["FATAL"] == 0

    def test_to_records_returns_dicts(self):
        log = self._make_log()
        log.add(ErrorLevel.WARNING, "msg")
        records = log.to_records()
        assert isinstance(records, list)
        assert "error_message" in records[0]

    def test_to_dataframe_has_correct_shape(self):
        log = self._make_log()
        log.add(ErrorLevel.ERROR, "e1")
        log.add(ErrorLevel.WARNING, "w1")
        df = log.to_dataframe()
        assert len(df) == 2

    def test_events_returns_copy(self):
        log = self._make_log()
        log.add(ErrorLevel.WARNING, "w")
        events_a = log.events
        events_b = log.events
        # mutating one should not affect the other
        events_a.clear()
        assert len(events_b) == 1
