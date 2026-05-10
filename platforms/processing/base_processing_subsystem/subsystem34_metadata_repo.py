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
import os
import yaml
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional

from shared.logger.python_main_logger import logger_manager
from platforms.processing.base_processing_subsystem.subsystem22_job_schedule import generate_run_id, _now_utc
from platforms.processing.base_processing_subsystem.subsystem1_data_profiling import DataQualityRecord
from platforms.processing.base_processing_subsystem.subsystem27_workflow_monitoring import ETLRunRecord
from platforms.processing.base_processing_subsystem.subsystem5_and_30_error_event_schema_and_escalate import ErrorEvent, ErrorEventLog, ErrorLevel

def load_metadata_config() -> Dict[str, Any]:
    config_path = os.path.join(os.path.dirname(__file__), "config", "metadata_repo.yaml")
    if os.path.exists(config_path):
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
                return config.get("metadata_repo", {}) if config else {}
        except Exception:
            pass
    return {}

META_CONFIG = load_metadata_config()
MSG_TEMPLATES = META_CONFIG.get("msg_templates", {})
SUMMARY_TEMPLATES = META_CONFIG.get("summary_templates", {})
SCHEMAS = META_CONFIG.get("schemas", {})

def _map_schema(context: Dict[str, Any], schema_name: str) -> Dict[str, Any]:
    schema = SCHEMAS.get(schema_name)
    if not schema:
        return context
    record = {}
    for k, v in schema.items():
        if isinstance(v, str):
            if "{" in v and "}" in v:
                record[k] = v.format(**context)
            elif v in context:
                record[k] = context[v]
            else:
                record[k] = v
        else:
            record[k] = v
    return record

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

        template = MSG_TEMPLATES.get("run_start", "[RUN START] run_id={run_id} | job={job_name} | layer={layer} | table={table_name}")
        self.logger.info(template.format(
            run_id=run_id, job_name=job_name, layer=layer, table_name=table_name
        ))

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
            template = MSG_TEMPLATES.get("run_not_found", "[RUN END] Run {run_id} not found")
            self.logger.error(template.format(run_id=run_id))
            return

        record = self._runs[run_id]
        record.end_time = _now_utc()
        record.status = status
        record.rows_read = rows_read
        record.rows_written = rows_written
        record.error_message = error_message

        duration = record.duration_seconds
        template = MSG_TEMPLATES.get("run_end", "[RUN END] run_id={run_id} | status={status} | duration={duration:.1f}s | rows_read={rows_read} | rows_written={rows_written}")
        self.logger.info(template.format(
            run_id=run_id, status=status, duration=duration or 0, 
            rows_read=rows_read, rows_written=rows_written
        ))

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

        template = MSG_TEMPLATES.get("error_logged", "[ERROR] {message} | run_id={run_id} | job={job_name} | record_id={record_id}")
        log_fn(template.format(
            message=message, run_id=run_id, 
            job_name=job_name, record_id=error_event.record_id
        ))

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
        context = {
            "escalation_id": error_event.err_id[:16] + "_esc",
            "error_id": error_event.err_id,
            "run_id": error_event.run_id,
            "severity": "CRITICAL",
            "action_taken": "escalated_to_monitoring",
            "escalated_at": _now_utc().isoformat(),
            "response_status": "PENDING",
        }
        escalation = _map_schema(context, "escalation")
        self._escalations.append(escalation)

        template = MSG_TEMPLATES.get("fatal_escalation", "FATAL ERROR ESCALATION | run_id={run_id} | job={job_name} | {message}")
        self.logger.critical(template.format(
            run_id=error_event.run_id, job_name=error_event.job_name, 
            message=error_event.error_message
        ))

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
        context = {
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
        quality_record = _map_schema(context, "quality")
        self._quality.append(quality_record)

        template = MSG_TEMPLATES.get("quality_logged", "[QUALITY] {table_name}.{check_name}: {status} ({pass_rate_pct:.1f}% pass rate)")
        self.logger.info(template.format(
            table_name=table_name, check_name=check_name, status=status, pass_rate_pct=pass_rate * 100
        ))

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
        context = {
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
        lineage_record = _map_schema(context, "lineage")
        self._lineage.append(lineage_record)

        template = MSG_TEMPLATES.get("lineage_logged", "[LINEAGE] {source_layer}.{source_table} -> {target_layer}.{target_table} | op={operation} | rows={rows_affected}")
        self.logger.info(template.format(
            source_layer=source_layer, source_table=source_table,
            target_layer=target_layer, target_table=target_table,
            operation=operation, rows_affected=rows_affected
        ))

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
            template = MSG_TEMPLATES.get("run_not_found", "[RUN END] Run {run_id} not found")
            self.logger.warning(template.format(run_id=run_id))
            return

        errors = self.get_errors_for_run(run_id)
        quality = self.get_quality_for_run(run_id)
        lineage = self.get_lineage_for_run(run_id)

        hdr = SUMMARY_TEMPLATES.get("header", "\n" + "="*70 + "\n  ETL Run Summary: {run_id}\n" + "="*70)
        body = SUMMARY_TEMPLATES.get("body", "  Job:           {job_name}\n  Layer:         {layer} -> {table_name}\n  Status:        {status}\n  Start:         {start_time}\n  End:           {end_time}\n  Duration:      {duration}\n  Rows Read:     {rows_read}\n  Rows Written:  {rows_written}")
        duration_str = f"{run.duration_seconds:.1f}s" if run.duration_seconds else "N/A"

        summary_lines = [
            hdr.format(run_id=run_id),
            body.format(
                job_name=run.job_name, layer=run.layer, table_name=run.table_name,
                status=run.status, start_time=run.start_time, end_time=run.end_time,
                duration=duration_str, rows_read=run.rows_read or 0, rows_written=run.rows_written or 0
            )
        ]

        if errors:
            critical_errors = [e for e in errors if e.get("error_level") == "FATAL"]
            err_hdr = SUMMARY_TEMPLATES.get("errors_header", "\n  Errors ({total_errors} total, {fatal_errors} FATAL):")
            summary_lines.append(err_hdr.format(total_errors=len(errors), fatal_errors=len(critical_errors)))
            err_item = SUMMARY_TEMPLATES.get("error_item", "    [{level}] {message} (record_id={record_id})")
            for e in errors:
                summary_lines.append(err_item.format(level=e.get('error_level'), message=e.get('error_message'), record_id=e.get('record_id')))

        if quality:
            failed_checks = [q for q in quality if q.get("status") in ("FAIL", "WARN")]
            q_hdr = SUMMARY_TEMPLATES.get("quality_header", "\n  Quality Checks ({total_checks} total, {failed_checks} FAIL/WARN):")
            summary_lines.append(q_hdr.format(total_checks=len(quality), failed_checks=len(failed_checks)))
            q_item = SUMMARY_TEMPLATES.get("quality_item", "    [{status}] {check_name} on {table_name} ({pass_rate_pct:.1f}% pass)")
            for q in quality:
                summary_lines.append(q_item.format(status=q.get('status'), check_name=q.get('check_name'), table_name=q.get('table_name'), pass_rate_pct=q.get('pass_rate')*100))

        if lineage:
            lin_hdr = SUMMARY_TEMPLATES.get("lineage_header", "\n  Data Lineage ({total_lineage} transformations):")
            summary_lines.append(lin_hdr.format(total_lineage=len(lineage)))
            lin_item = SUMMARY_TEMPLATES.get("lineage_item", "    {source_layer}.{source_table} --[{operation}]--> {target_layer}.{target_table} ({rows_affected} rows)")
            for l in lineage:
                summary_lines.append(lin_item.format(source_layer=l.get('source_layer'), source_table=l.get('source_table'), operation=l.get('operation'), target_layer=l.get('target_layer'), target_table=l.get('target_table'), rows_affected=l.get('rows_affected')))

        ftr = SUMMARY_TEMPLATES.get("footer", "="*70 + "\n")
        summary_lines.append(ftr)
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
            msg = MSG_TEMPLATES.get("persist_skip", "Cannot persist: in_memory mode or no backend configured")
            self.logger.warning(msg)
            return

        self.logger.info(MSG_TEMPLATES.get("persist_start", "[PERSIST] Writing metadata to Delta Lake..."))

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

        self.logger.info(MSG_TEMPLATES.get("persist_end", "[PERSIST] Metadata persisted successfully"))

    