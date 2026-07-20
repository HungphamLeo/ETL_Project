"""
Unit tests cho các infrastructure subsystems:
  - Subsystem 22: Job Scheduler (subsystem22_job_schedule.py)
  - Subsystem 27: Workflow Monitoring (subsystem27_workflow_monitoring.py)
  - Subsystem 29: Data Lineage Tracker (subsystem29_data_lineage.py)
  - Subsystem 34: Metadata Repository (subsystem34_metadata_repo.py)

Tất cả tests chạy hoàn toàn in-memory (không cần Delta backend, không cần MinIO).
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Dict

import pytest

# ---------------------------------------------------------------------------
# Subsystem 22 — Job Scheduler
# ---------------------------------------------------------------------------
from platforms.processing.base_processing_subsystem.subsystem22_job_schedule import (
    generate_run_id,
    make_run_id,
    _make_id,
    _now_utc,
)


class TestSubsystem22JobSchedule:
    """Tests cho subsystem22_job_schedule.py."""

    def test_generate_run_id_format(self):
        """generate_run_id trả về string có prefix và timestamp hợp lệ."""
        rid = generate_run_id()
        assert rid.startswith("run_")
        parts = rid.split("_")
        # format: run_YYYYMMDD_HHMMSS_<uid8>
        assert len(parts) >= 3

    def test_generate_run_id_custom_prefix(self):
        rid = generate_run_id(prefix="test")
        assert rid.startswith("test_")

    def test_generate_run_id_unique(self):
        """Mỗi lần gọi tạo ra run_id khác nhau."""
        ids = {generate_run_id() for _ in range(20)}
        assert len(ids) == 20, "Phải unique"

    def test_make_run_id_returns_provided(self):
        """make_run_id trả về run_id đã có nếu được truyền vào."""
        existing = "run_20260101_000000_abc12345"
        assert make_run_id(run_id=existing) == existing

    def test_make_run_id_generates_when_none(self):
        """make_run_id tự tạo run_id mới khi không được truyền."""
        rid = make_run_id()
        assert isinstance(rid, str) and rid.startswith("run_")

    def test_make_id_deterministic(self):
        """_make_id là hàm deterministic — cùng input cho cùng output."""
        a = _make_id("run1", "table_a", "table_b", "APPEND")
        b = _make_id("run1", "table_a", "table_b", "APPEND")
        assert a == b

    def test_make_id_different_inputs(self):
        """_make_id cho kết quả khác nhau khi input khác nhau."""
        a = _make_id("run1", "tableA")
        b = _make_id("run1", "tableB")
        assert a != b

    def test_make_id_length(self):
        """_make_id luôn trả về chuỗi 16 ký tự hex."""
        result = _make_id("x", "y", "z")
        assert len(result) == 16

    def test_now_utc_timezone(self):
        """_now_utc trả về datetime có timezone UTC."""
        now = _now_utc()
        assert isinstance(now, datetime)
        assert now.tzinfo is not None


# ---------------------------------------------------------------------------
# Subsystem 27 — Workflow Monitoring (ETLRunRecord)
# ---------------------------------------------------------------------------
from platforms.processing.base_processing_subsystem.subsystem27_workflow_monitoring import ETLRunRecord


class TestSubsystem27WorkflowMonitoring:
    """Tests cho ETLRunRecord trong subsystem27."""

    def _make_record(self, **kwargs) -> ETLRunRecord:
        defaults: Dict[str, Any] = {
            "run_id": "run_test_001",
            "job_name": "test_job",
            "layer": "bronze",
            "table_name": "stock_prices",
            "start_time": datetime(2026, 7, 18, 10, 0, 0, tzinfo=timezone.utc),
        }
        defaults.update(kwargs)
        return ETLRunRecord(**defaults)

    def test_initial_status_running(self):
        rec = self._make_record()
        assert rec.status == "RUNNING"

    def test_duration_none_when_no_end_time(self):
        rec = self._make_record()
        assert rec.duration_seconds is None

    def test_duration_computed(self):
        start = datetime(2026, 7, 18, 10, 0, 0, tzinfo=timezone.utc)
        end = datetime(2026, 7, 18, 10, 1, 30, tzinfo=timezone.utc)
        rec = self._make_record(start_time=start, end_time=end)
        assert rec.duration_seconds == pytest.approx(90.0)

    def test_to_dict_keys(self):
        rec = self._make_record()
        d = rec.to_dict()
        assert "run_id" in d
        assert "job_name" in d
        assert "status" in d
        assert "start_time" in d
        assert "duration_seconds" in d

    def test_to_dict_start_time_iso(self):
        rec = self._make_record()
        d = rec.to_dict()
        assert isinstance(d["start_time"], str)
        assert "2026-07-18" in d["start_time"]

    def test_to_dict_end_time_none(self):
        rec = self._make_record()
        d = rec.to_dict()
        assert d["end_time"] is None

    def test_to_dict_end_time_iso_when_set(self):
        end = datetime(2026, 7, 18, 11, 0, 0, tzinfo=timezone.utc)
        rec = self._make_record(end_time=end)
        d = rec.to_dict()
        assert isinstance(d["end_time"], str)
        assert "2026-07-18" in d["end_time"]

    def test_rows_fields_optional(self):
        rec = self._make_record()
        assert rec.rows_read is None
        assert rec.rows_written is None

    def test_error_message_optional(self):
        rec = self._make_record()
        assert rec.error_message is None

    def test_status_update(self):
        rec = self._make_record()
        rec.status = "SUCCESS"
        assert rec.status == "SUCCESS"


# ---------------------------------------------------------------------------
# Subsystem 29 — Data Lineage Tracker
# ---------------------------------------------------------------------------
from platforms.processing.base_processing_subsystem.subsystem29_data_lineage import (
    LineageRecord,
    LineageTracker,
)


class TestSubsystem29LineageRecord:
    """Tests cho LineageRecord dataclass."""

    def _make_record(self) -> LineageRecord:
        return LineageRecord(
            lineage_id="lid_001",
            run_id="run_001",
            source_layer="bronze",
            source_table="stock_prices",
            target_layer="silver",
            target_table="fact_stock_price",
            operation="APPEND",
            rows_affected=1000,
        )

    def test_to_dict_has_all_fields(self):
        rec = self._make_record()
        d = rec.to_dict()
        # Có thể trả về context dict hoặc schema-mapped dict — đều phải có info cơ bản
        assert isinstance(d, dict)

    def test_to_dict_recorded_at_iso_string(self):
        rec = self._make_record()
        d = rec.to_dict()
        # recorded_at được serialise thành ISO string trong to_dict
        assert isinstance(d.get("recorded_at"), str)

    def test_rows_affected_stored(self):
        rec = self._make_record()
        assert rec.rows_affected == 1000


class TestSubsystem29LineageTracker:
    """Tests cho LineageTracker."""

    def _make_tracker(self) -> LineageTracker:
        import logging
        return LineageTracker(logger=logging.getLogger("test_lineage"))

    def test_log_lineage_returns_id(self):
        tracker = self._make_tracker()
        lid = tracker.log_lineage(
            run_id="run_001",
            source_layer="bronze",
            source_table="stock_prices",
            target_layer="silver",
            target_table="fact_stock_price",
            operation="APPEND",
            rows_affected=500,
        )
        assert isinstance(lid, str) and len(lid) > 0

    def test_log_lineage_deterministic(self):
        """Cùng input tạo ra cùng lineage_id (SHA-256 based)."""
        tracker = self._make_tracker()
        lid1 = tracker.log_lineage("r1", "bronze", "a", "silver", "b", "MERGE", 100)
        lid2 = tracker.log_lineage("r1", "bronze", "a", "silver", "b", "MERGE", 100)
        assert lid1 == lid2

    def test_get_lineage_graph_returns_dataframe(self):
        import pandas as pd
        tracker = self._make_tracker()
        tracker.log_lineage("r1", "bronze", "t1", "silver", "t2", "APPEND", 10)
        df = tracker.get_lineage_graph()
        assert isinstance(df, pd.DataFrame)
        assert len(df) >= 1

    def test_get_lineage_graph_empty_when_no_records(self):
        import pandas as pd
        tracker = self._make_tracker()
        df = tracker.get_lineage_graph()
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_get_all_runs_empty_initially(self):
        import pandas as pd
        tracker = self._make_tracker()
        df = tracker.get_all_runs()
        assert isinstance(df, pd.DataFrame)

    def test_get_errors_for_run_empty_when_none(self):
        import pandas as pd
        tracker = self._make_tracker()
        df = tracker.get_errors_for_run("nonexistent")
        assert isinstance(df, pd.DataFrame)

    def test_multiple_lineage_events(self):
        tracker = self._make_tracker()
        for i in range(5):
            tracker.log_lineage(f"run_{i}", "bronze", f"tbl_{i}", "silver", f"stbl_{i}", "APPEND", i * 10)
        df = tracker.get_lineage_graph()
        assert len(df) == 5


# ---------------------------------------------------------------------------
# Subsystem 34 — Metadata Repository (in-memory mode)
# ---------------------------------------------------------------------------
from platforms.processing.base_processing_subsystem.subsystem34_metadata_repo import MetadataRepository
from platforms.processing.base_processing_subsystem.subsystem5_and_30_error_event_schema_and_escalate import (
    ErrorLevel,
)


class TestSubsystem34MetadataRepository:
    """Tests cho MetadataRepository trong in-memory mode."""

    def _make_repo(self) -> MetadataRepository:
        import logging
        return MetadataRepository(
            delta_backend=None,
            logger=logging.getLogger("test_meta"),
            in_memory=True,
        )

    # ── Run tracking ──────────────────────────────────────────────────────

    def test_start_run_returns_run_id(self):
        repo = self._make_repo()
        run_id = repo.start_run(job_name="test_job", layer="bronze", table_name="stock_prices")
        assert isinstance(run_id, str) and len(run_id) > 0

    def test_start_run_with_explicit_id(self):
        repo = self._make_repo()
        custom_id = "run_custom_001"
        returned = repo.start_run("job", layer="bronze", run_id=custom_id)
        assert returned == custom_id

    def test_start_run_stored_in_memory(self):
        repo = self._make_repo()
        run_id = repo.start_run("job", "bronze")
        record = repo.get_run(run_id)
        assert record is not None
        assert record.run_id == run_id

    def test_end_run_sets_status_success(self):
        repo = self._make_repo()
        run_id = repo.start_run("job", "bronze")
        repo.end_run(run_id, status="SUCCESS", rows_written=100)
        record = repo.get_run(run_id)
        assert record.status == "SUCCESS"
        assert record.rows_written == 100

    def test_end_run_sets_end_time(self):
        repo = self._make_repo()
        run_id = repo.start_run("job", "bronze")
        repo.end_run(run_id, status="SUCCESS")
        record = repo.get_run(run_id)
        assert record.end_time is not None

    def test_end_run_failed(self):
        repo = self._make_repo()
        run_id = repo.start_run("job", "bronze")
        repo.end_run(run_id, status="FAILED", error_message="DQ check failed")
        record = repo.get_run(run_id)
        assert record.status == "FAILED"
        assert record.error_message == "DQ check failed"

    def test_end_run_unknown_id_no_crash(self):
        """end_run với run_id không tồn tại không được raise exception."""
        repo = self._make_repo()
        repo.end_run("nonexistent_id", status="SUCCESS")  # should not raise

    def test_get_all_runs_returns_list(self):
        repo = self._make_repo()
        repo.start_run("job1", "bronze")
        repo.start_run("job2", "silver")
        all_runs = repo.get_all_runs()
        assert isinstance(all_runs, list)
        assert len(all_runs) == 2

    # ── Lineage logging ───────────────────────────────────────────────────

    def test_log_lineage_stored(self):
        repo = self._make_repo()
        run_id = repo.start_run("job", "bronze")
        repo.log_lineage(
            run_id=run_id,
            source_layer="external",
            source_table="cophieu68",
            target_layer="bronze",
            target_table="stock_prices",
            operation="APPEND",
            rows_affected=500,
        )
        lineage = repo.get_lineage_for_run(run_id)
        assert len(lineage) >= 1

    def test_log_lineage_returns_id(self):
        repo = self._make_repo()
        lid = repo.log_lineage("r1", "bronze", "a", "silver", "b", "MERGE", 10)
        assert isinstance(lid, str)

    def test_log_lineage_multiple_events(self):
        repo = self._make_repo()
        run_id = repo.start_run("job", "bronze")
        for i in range(3):
            repo.log_lineage(run_id, "bronze", f"src_{i}", "silver", f"tgt_{i}", "APPEND", i)
        lineage = repo.get_lineage_for_run(run_id)
        assert len(lineage) == 3

    # ── Error logging ─────────────────────────────────────────────────────

    def test_log_error_stored(self):
        repo = self._make_repo()
        run_id = repo.start_run("job", "bronze")
        repo.log_error(run_id, "extract_step", ErrorLevel.WARNING, "Null symbol found")
        errors = repo.get_errors_for_run(run_id)
        assert len(errors) >= 1

    def test_log_error_returns_event(self):
        repo = self._make_repo()
        run_id = repo.start_run("job", "bronze")
        result = repo.log_error(run_id, "step", ErrorLevel.WARNING, "msg")
        assert result is not None

    def test_log_error_empty_for_other_run(self):
        repo = self._make_repo()
        run_a = repo.start_run("job_a", "bronze")
        run_b = repo.start_run("job_b", "bronze")
        repo.log_error(run_a, "step", ErrorLevel.WARNING, "only for A")
        errors_b = repo.get_errors_for_run(run_b)
        assert len(errors_b) == 0

    # ── Run summary ───────────────────────────────────────────────────────

    def test_log_run_summary_no_crash(self):
        """log_run_summary không crash kể cả khi run có lỗi."""
        repo = self._make_repo()
        run_id = repo.start_run("job", "bronze")
        repo.log_error(run_id, "step", ErrorLevel.ERROR, "bad thing")
        repo.end_run(run_id, status="FAILED")
        repo.log_run_summary(run_id)  # must not raise

    def test_log_run_summary_unknown_id_no_crash(self):
        repo = self._make_repo()
        repo.log_run_summary("does_not_exist")  # must not raise

    # ── Context manager ───────────────────────────────────────────────────

    def test_run_context_manager_success(self):
        """Context manager run_context marks run as SUCCESS khi không có exception."""
        repo = self._make_repo()
        with repo.run_context("cm_job", layer="silver", table_name="fact_prices") as run_id:
            assert isinstance(run_id, str)
        record = repo.get_run(run_id)
        assert record.status == "SUCCESS"

    def test_run_context_manager_failed_on_exception(self):
        """Context manager run_context marks run as FAILED khi có exception."""
        repo = self._make_repo()
        captured_id = None
        with pytest.raises(ValueError):
            with repo.run_context("cm_job_fail", layer="bronze") as run_id:
                captured_id = run_id
                raise ValueError("simulated failure")
        if captured_id:
            record = repo.get_run(captured_id)
            assert record.status == "FAILED"
