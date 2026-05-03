"""
Metadata Repository Manager
=============================
Subsystem 34: Metadata Repository Manager
Subsystem 22: Job Scheduler (run tracking)
Subsystem 27: Workflow Monitor (status + duration)
Subsystem 29: Lineage and Dependency Analyzer
Subsystem 5: Error Event Schema

Tracks ETL pipeline runs, errors, data quality checks, and data lineage
in Delta Lake silver/meta tables.

Usage:
    repo = MetadataRepository(delta_backend)
    run_id = repo.start_run("cophieu68.extract_trading_data", layer="bronze")
    repo.end_run(run_id, status="SUCCESS", rows_written=1500)
    repo.log_error(run_id, "cophieu68.extract", ErrorLevel.WARNING, "Null symbol", record)
    repo.log_quality_check(run_id, "bronze_trading_data", "null_check:symbol", "PASS", 0, 1500)
"""

from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional

from shared.logger.python_main_logger import logger_manager
from platforms.processing.base_processing_subsystem.subsystem22_job_schedule import generate_run_id, _now_utc
from platforms.processing.base_processing_subsystem.subsystem1_data_profiling import DataQualityRecord
from platforms.processing.base_processing_subsystem.subsystem27_workflow_monitoring import ETLRunRecord
from platforms.processing.base_processing_subsystem.subsystem5_error_event_schema import ErrorEvent, ErrorEventLog, ErrorLevel


class MetadataRepository:
    """
    Subsystem 34: Metadata Repository Manager.

    Persists ETL run metadata, errors, data quality results, and lineage
    to Delta Lake silver/meta tables via DeltaLakeStorageBackend.

    Also supports in-memory mode (no Delta writes) for lightweight usage.
    """

    def __init__(
        self,
        delta_backend=None,
        logger: Optional[logging.Logger] = None,
        in_memory: bool = False,
    ):
        """
        Args:
            delta_backend: DeltaLakeStorageBackend instance (optional)
            logger: Python logger
            in_memory: If True, only store in memory (no Delta writes)
        """
        self.backend = delta_backend
        self.logger = logger or logger_manager.get_logger(__name__)
        self.in_memory = in_memory or (delta_backend is None)

        # In-memory stores
        self._runs: Dict[str, ETLRunRecord] = {}
        self._errors: List[ErrorEvent] = []
        self._quality: List[DataQualityRecord] = []
        self._lineage: List[Dict[str, Any]] = []
        self._escalations: List[Dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Run tracking (Subsystem 22 + 27)
    # ------------------------------------------------------------------

    def start_run(
        self,
        job_name: str,
        layer: str = "bronze",
        table_name: str = "",
        run_id: Optional[str] = None,
    ) -> str:
        """
        Register the start of an ETL pipeline run.
        Returns the run_id for use in subsequent calls.

        Args:
            job_name: Descriptive job name (e.g. "cophieu68.extract_trading_data")
            layer: Target layer (bronze | silver | gold)
            table_name: Target table name
            run_id: Optional pre-generated run_id
        """
        run_id = run_id or generate_run_id()
        start_time = _now_utc()

        record = ETLRunRecord(
            run_id=run_id,
            job_name=job_name,
            layer=layer,
            table_name=table_name,
            start_time=start_time,
            status="RUNNING",
        )
        self._runs[run_id] = record

        self.logger.info(
            "[RUN START] run_id=%s | job=%s | layer=%s | table=%s",
            run_id, job_name, layer, table_name,
        )

        if not self.in_memory and self.backend:
            self.backend.write_to_delta(
                table="silver_meta_etl_run",
                records=[record.to_dict()],
                mode="append",
            )

        return run_id

    def end_run(
        self,
        run_id: str,
        status: str = "SUCCESS",
        rows_read: Optional[int] = None,
        rows_written: Optional[int] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """
        Mark an ETL run as complete.

        Args:
            run_id: Run ID from start_run()
            status: SUCCESS | FAILED | PARTIAL | SKIPPED
            rows_read: Rows read from source
            rows_written: Rows written to target
            error_message: Error details if status=FAILED
        """
        if run_id not in self._runs:
            self.logger.error(f"[RUN END] Run {run_id} not found")
            return

        record = self._runs[run_id]
        record.end_time = _now_utc()
        record.status = status
        record.rows_read = rows_read
        record.rows_written = rows_written
        record.error_message = error_message

        duration = record.duration_seconds
        self.logger.info(
            "[RUN END] run_id=%s | status=%s | duration=%.1fs | rows_read=%s | rows_written=%s",
            run_id, status, duration or 0, rows_read, rows_written,
        )

        if not self.in_memory and self.backend:
            self.backend.write_to_delta(
                table="silver_meta_etl_run",
                records=[record.to_dict()],
                mode="merge",
                merge_keys=["run_id"],
            )

    # ------------------------------------------------------------------
    # Error tracking (Subsystem 5)
    # ------------------------------------------------------------------

    def log_error(
        self,
        run_id: str,
        job_name: str,
        level: ErrorLevel,
        message: str,
        record: Optional[Dict[str, Any]] = None,
        record_id_fields: Optional[List[str]] = None,
    ) -> str:
        """
        Log an error event for the pipeline run.

        Args:
            run_id: ETL run ID
            job_name: Job/process name
            level: ErrorLevel.WARNING | ERROR | FATAL
            message: Error message
            record: The data record that caused the error
            record_id_fields: Fields to use as record identifier
        Returns: error ID
        """
        error_event = ErrorEvent.create(
            run_id=run_id,
            job_name=job_name,
            level=level,
            message=message,
            record=record,
            record_id_fields=record_id_fields,
        )
        self._errors.append(error_event)

        log_fn = {
            ErrorLevel.WARNING: self.logger.warning,
            ErrorLevel.ERROR: self.logger.error,
            ErrorLevel.FATAL: self.logger.critical,
        }.get(level, self.logger.error)

        log_fn(
            "[ERROR] %s | run_id=%s | job=%s | record_id=%s",
            message, run_id, job_name, error_event.record_id,
        )

        # Handle FATAL escalation
        if level == ErrorLevel.FATAL:
            self._escalate_fatal(error_event)

        if not self.in_memory and self.backend:
            self.backend.write_to_delta(
                table="silver_meta_etl_error",
                records=[error_event.to_dict()],
                mode="append",
            )

        return error_event.err_id

    def _escalate_fatal(self, error_event: ErrorEvent) -> None:
        """Subsystem 30: Problem Escalation for FATAL errors."""
        escalation = {
            "escalation_id": error_event.err_id[:16] + "_esc",
            "error_id": error_event.err_id,
            "run_id": error_event.run_id,
            "severity": "CRITICAL",
            "action_taken": "escalated_to_monitoring",
            "escalated_at": _now_utc().isoformat(),
            "response_status": "PENDING",
        }
        self._escalations.append(escalation)

        self.logger.critical(
            "FATAL ERROR ESCALATION | run_id=%s | job=%s | %s",
            error_event.run_id, error_event.job_name, error_event.error_message,
        )

        # TODO: integrate with alerting system (Slack, PagerDuty, email)

    # ------------------------------------------------------------------
    # Data quality tracking (Subsystem 1)
    # ------------------------------------------------------------------

    def log_quality_check(
        self,
        run_id: str,
        table_name: str,
        check_name: str,
        status: str,
        failed_count: int,
        total_count: int,
    ) -> None:
        """
        Log a data quality check result.

        Args:
            run_id: ETL run ID
            table_name: Table being validated
            check_name: Quality check identifier (e.g., "null_check:symbol")
            status: PASS | FAIL | WARN | SKIPPED
            failed_count: Number of records failing the check
            total_count: Total records checked
        """
        pass_rate = 1.0 - (failed_count / total_count) if total_count > 0 else 1.0
        quality_record = {
            "quality_id": f"{run_id}_{check_name}".replace(":", "_"),
            "run_id": run_id,
            "table_name": table_name,
            "check_name": check_name,
            "status": status,
            "total_records": total_count,
            "failed_records": failed_count,
            "pass_rate": round(pass_rate, 4),
            "checked_at": _now_utc().isoformat(),
        }
        self._quality.append(quality_record)

        self.logger.info(
            "[QUALITY] %s.%s: %s (%.1f%% pass rate)",
            table_name, check_name, status, pass_rate * 100,
        )

        if not self.in_memory and self.backend:
            self.backend.write_to_delta(
                table="silver_meta_data_quality",
                records=[quality_record],
                mode="append",
            )

    # ------------------------------------------------------------------
    # Data lineage tracking (Subsystem 29)
    # ------------------------------------------------------------------

    def log_lineage(
        self,
        run_id: str,
        source_layer: str,
        source_table: str,
        target_layer: str,
        target_table: str,
        operation: str,
        rows_affected: int = 0,
    ) -> str:
        """
        Log a data lineage event (data flow between layers/tables).

        Args:
            run_id: ETL run ID
            source_layer: Source layer (bronze | silver | gold | external)
            source_table: Source table name
            target_layer: Target layer (bronze | silver | gold)
            target_table: Target table name
            operation: APPEND | MERGE | SCD2 | OVERWRITE | BACKFILL | CDC
            rows_affected: Number of rows processed
        Returns: lineage event ID
        """
        lineage_id = f"{run_id}_{source_table}_{target_table}_{operation}".replace(".", "_")
        lineage_record = {
            "lineage_id": lineage_id,
            "run_id": run_id,
            "source_layer": source_layer,
            "source_table": source_table,
            "target_layer": target_layer,
            "target_table": target_table,
            "operation": operation,
            "rows_affected": rows_affected,
            "recorded_at": _now_utc().isoformat(),
        }
        self._lineage.append(lineage_record)

        self.logger.info(
            "[LINEAGE] %s.%s → %s.%s | op=%s | rows=%d",
            source_layer, source_table, target_layer, target_table, operation, rows_affected,
        )

        if not self.in_memory and self.backend:
            self.backend.write_to_delta(
                table="silver_meta_data_lineage",
                records=[lineage_record],
                mode="append",
            )

        return lineage_id

    # ------------------------------------------------------------------
    # Query / Reporting
    # ------------------------------------------------------------------

    def get_run(self, run_id: str) -> Optional[ETLRunRecord]:
        """Retrieve a run record by run_id."""
        return self._runs.get(run_id)

    def get_all_runs(self) -> List[Dict[str, Any]]:
        """Get all run records as dicts."""
        return [r.to_dict() for r in self._runs.values()]

    def get_errors_for_run(self, run_id: str) -> List[Dict[str, Any]]:
        """Get all error events for a specific run."""
        return [e.to_dict() for e in self._errors if e.run_id == run_id]

    def get_quality_for_run(self, run_id: str) -> List[Dict[str, Any]]:
        """Get all quality checks for a specific run."""
        return [q for q in self._quality if q.get("run_id") == run_id]

    def get_lineage_for_run(self, run_id: str) -> List[Dict[str, Any]]:
        """Get all lineage events for a specific run."""
        return [l for l in self._lineage if l.get("run_id") == run_id]

    def log_run_summary(self, run_id: str) -> None:
        """Log a human-readable summary of a pipeline run."""
        run = self.get_run(run_id)
        if not run:
            self.logger.warning(f"Run {run_id} not found")
            return

        errors = self.get_errors_for_run(run_id)
        quality = self.get_quality_for_run(run_id)
        lineage = self.get_lineage_for_run(run_id)

        summary_lines = [
            f"\n{'='*70}",
            f"  ETL Run Summary: {run_id}",
            f"{'='*70}",
            f"  Job:           {run.job_name}",
            f"  Layer:         {run.layer} → {run.table_name}",
            f"  Status:        {run.status}",
            f"  Start:         {run.start_time}",
            f"  End:           {run.end_time}",
            f"  Duration:      {run.duration_seconds:.1f}s" if run.duration_seconds else "  Duration:      N/A",
            f"  Rows Read:     {run.rows_read or 0}",
            f"  Rows Written:  {run.rows_written or 0}",
        ]

        if errors:
            critical_errors = [e for e in errors if e.get("error_level") == "FATAL"]
            summary_lines.append(f"\n  Errors ({len(errors)} total, {len(critical_errors)} FATAL):")
            for e in errors:
                summary_lines.append(f"    [{e.get('error_level')}] {e.get('error_message')} (record_id={e.get('record_id')})")

        if quality:
            failed_checks = [q for q in quality if q.get("status") in ("FAIL", "WARN")]
            summary_lines.append(f"\n  Quality Checks ({len(quality)} total, {len(failed_checks)} FAIL/WARN):")
            for q in quality:
                summary_lines.append(f"    [{q.get('status')}] {q.get('check_name')} on {q.get('table_name')} ({q.get('pass_rate')*100:.1f}% pass)")

        if lineage:
            summary_lines.append(f"\n  Data Lineage ({len(lineage)} transformations):")
            for l in lineage:
                summary_lines.append(f"    {l.get('source_layer')}.{l.get('source_table')} --[{l.get('operation')}]--> {l.get('target_layer')}.{l.get('target_table')} ({l.get('rows_affected')} rows)")

        summary_lines.append(f"{'='*70}\n")
        summary_msg = "\n".join(summary_lines)
        self.logger.info(summary_msg)

    @contextmanager
    def run_context(
        self,
        job_name: str,
        layer: str = "bronze",
        table_name: str = "",
    ) -> Generator[str, None, None]:
        """
        Context manager for ETL run tracking.
        Automatically handles start_run() and end_run().

        Usage:
            with repo.run_context("cophieu68.extract", layer="bronze", table_name="bronze_trading") as run_id:
                # do work
                # if exception: run will be marked FAILED
                # if success: run will be marked SUCCESS
        """
        run_id = self.start_run(job_name, layer, table_name)
        try:
            yield run_id
            self.end_run(run_id, status="SUCCESS")
        except Exception as exc:
            self.end_run(run_id, status="FAILED", error_message=str(exc))
            self.log_error(
                run_id, job_name, ErrorLevel.FATAL,
                f"Unhandled exception: {str(exc)}",
            )
            raise

    def persist_to_delta(self) -> None:
        """Write all in-memory metadata to Delta Lake."""
        if self.in_memory or not self.backend:
            self.logger.warning("Cannot persist: in_memory mode or no backend configured")
            return

        self.logger.info("[PERSIST] Writing metadata to Delta Lake...")

        if self._runs:
            self.backend.write_to_delta(
                table="silver_meta_etl_run",
                records=[r.to_dict() for r in self._runs.values()],
                mode="merge",
                merge_keys=["run_id"],
            )

        if self._errors:
            self.backend.write_to_delta(
                table="silver_meta_etl_error",
                records=[e.to_dict() for e in self._errors],
                mode="append",
            )

        if self._quality:
            self.backend.write_to_delta(
                table="silver_meta_data_quality",
                records=self._quality,
                mode="append",
            )

        if self._lineage:
            self.backend.write_to_delta(
                table="silver_meta_data_lineage",
                records=self._lineage,
                mode="append",
            )

        if self._escalations:
            self.backend.write_to_delta(
                table="silver_meta_problem_escalation",
                records=self._escalations,
                mode="append",
            )

        self.logger.info("[PERSIST] Metadata persisted successfully")

    