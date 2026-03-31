"""
Metadata Repository Manager
=============================
Subsystem 34: Metadata Repository Manager
Subsystem 22: Job Scheduler (run tracking)
Subsystem 27: Workflow Monitor (status + duration)
Subsystem 29: Lineage and Dependency Analyzer

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

import hashlib
import logging
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional

import pandas as pd


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _make_id(*parts: str) -> str:
    raw = "|".join(str(p) for p in parts)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Run record dataclass
# ---------------------------------------------------------------------------

@dataclass
class ETLRunRecord:
    """Maps to silver_meta_etl_run Delta table. Subsystem 22 + 27."""
    run_id:        str
    job_name:      str
    layer:         str
    table_name:    str
    start_time:    datetime
    end_time:      Optional[datetime] = None
    status:        str = "RUNNING"   # RUNNING | SUCCESS | FAILED | PARTIAL
    rows_read:     Optional[int] = None
    rows_written:  Optional[int] = None
    error_message: Optional[str] = None

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.end_time and self.start_time:
            return (self.end_time - self.start_time).total_seconds()
        return None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["start_time"] = self.start_time.isoformat() if self.start_time else None
        d["end_time"] = self.end_time.isoformat() if self.end_time else None
        d["duration_seconds"] = self.duration_seconds
        return d


@dataclass
class ETLErrorRecord:
    """Maps to silver_meta_etl_error Delta table. Subsystem 5 + 30."""
    err_id:        str
    run_id:        str
    job_name:      str
    error_level:   str
    error_message: str
    record_id:     Optional[str] = None
    raw_json:      Optional[str] = None
    err_time:      datetime = field(default_factory=_now_utc)
    retry_count:   int = 0

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["err_time"] = self.err_time.isoformat()
        return d


@dataclass
class DataQualityRecord:
    """Maps to silver_meta_data_quality Delta table. Subsystem 1."""
    check_id:    str
    run_id:      str
    table_name:  str
    check_name:  str
    status:      str   # PASS | FAIL | WARN
    failed_rows: int
    total_rows:  int
    checked_at:  datetime = field(default_factory=_now_utc)
    details:     Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["checked_at"] = self.checked_at.isoformat()
        return d


@dataclass
class LineageRecord:
    """Data lineage record. Subsystem 29: Lineage and Dependency Analyzer."""
    lineage_id:   str
    run_id:       str
    source_layer: str
    source_table: str
    target_layer: str
    target_table: str
    operation:    str   # APPEND | MERGE | SCD2 | OVERWRITE | BACKFILL
    rows_affected: int
    recorded_at:  datetime = field(default_factory=_now_utc)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["recorded_at"] = self.recorded_at.isoformat()
        return d


# ---------------------------------------------------------------------------
# Metadata Repository
# ---------------------------------------------------------------------------

class MetadataRepository:
    """
    Subsystem 34: Metadata Repository Manager.

    Persists ETL run metadata, errors, data quality results, and lineage
    to Delta Lake silver/meta tables via DeltaLakeStorageBackend.

    Also supports in-memory mode (no Delta backend) for lightweight usage.
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
        self.logger = logger or logging.getLogger(__name__)
        self.in_memory = in_memory or (delta_backend is None)

        # In-memory stores
        self._runs:    Dict[str, ETLRunRecord] = {}
        self._errors:  List[ETLErrorRecord] = []
        self._quality: List[DataQualityRecord] = []
        self._lineage: List[LineageRecord] = []

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
        if not run_id:
            ts = _now_utc().strftime("%Y%m%d_%H%M%S")
            uid = str(uuid.uuid4())[:8]
            run_id = f"run_{ts}_{uid}"

        record = ETLRunRecord(
            run_id=run_id,
            job_name=job_name,
            layer=layer,
            table_name=table_name,
            start_time=_now_utc(),
            status="RUNNING",
        )
        self._runs[run_id] = record
        self.logger.info("[META] Run started: %s | job=%s | layer=%s", run_id, job_name, layer)
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
            status: SUCCESS | FAILED | PARTIAL
            rows_read: Number of rows read from source
            rows_written: Number of rows written to target
            error_message: Error message if status=FAILED
        """
        record = self._runs.get(run_id)
        if not record:
            self.logger.warning("[META] end_run called for unknown run_id: %s", run_id)
            return

        record.end_time = _now_utc()
        record.status = status
        record.rows_read = rows_read
        record.rows_written = rows_written
        record.error_message = error_message

        duration = record.duration_seconds
        self.logger.info(
            "[META] Run ended: %s | status=%s | rows_written=%s | duration=%.1fs",
            run_id, status, rows_written, duration or 0,
        )

        # Persist to Delta
        if not self.in_memory and self.backend:
            try:
                self.backend.write_silver_merge(
                    "meta_etl_run",
                    [record.to_dict()],
                    merge_keys=["run_id"],
                )
            except Exception as e:
                self.logger.warning("[META] Failed to persist run record: %s", e)

    @contextmanager
    def run_context(
        self,
        job_name: str,
        layer: str = "bronze",
        table_name: str = "",
    ) -> Generator[str, None, None]:
        """
        Context manager for automatic run tracking.

        Usage:
            with repo.run_context("cophieu68.load_trading", "bronze", "trading_data") as run_id:
                backend.write_bronze("trading_data", records)
        """
        run_id = self.start_run(job_name, layer, table_name)
        try:
            yield run_id
            self.end_run(run_id, status="SUCCESS")
        except Exception as exc:
            self.end_run(run_id, status="FAILED", error_message=str(exc))
            raise

    # ------------------------------------------------------------------
    # Error logging (Subsystem 5 + 30)
    # ------------------------------------------------------------------

    def log_error(
        self,
        run_id: str,
        job_name: str,
        error_level: str,
        error_message: str,
        record: Optional[Dict[str, Any]] = None,
        record_id_fields: Optional[List[str]] = None,
        retry_count: int = 0,
    ) -> str:
        """
        Log an error event.
        Subsystem 5 (Error Event Schema) + Subsystem 30 (Problem Escalation).

        Returns: err_id
        """
        import json
        record = record or {}
        record_id = "|".join(str(record.get(f, "")) for f in (record_id_fields or []))
        err_id = _make_id(run_id, record_id, error_message)
        raw_json = json.dumps(record, default=str, ensure_ascii=False) if record else None

        err_record = ETLErrorRecord(
            err_id=err_id,
            run_id=run_id,
            job_name=job_name,
            error_level=error_level,
            error_message=error_message,
            record_id=record_id or None,
            raw_json=raw_json,
            retry_count=retry_count,
        )
        self._errors.append(err_record)

        log_fn = {
            "WARNING": self.logger.warning,
            "ERROR":   self.logger.error,
            "FATAL":   self.logger.critical,
        }.get(error_level.upper(), self.logger.error)
        log_fn("[META ERROR] %s | %s | record_id=%s", error_level, error_message, record_id)

        # Persist to Delta
        if not self.in_memory and self.backend:
            try:
                self.backend.write_silver_merge(
                    "meta_etl_error",
                    [err_record.to_dict()],
                    merge_keys=["err_id"],
                )
            except Exception as e:
                self.logger.warning("[META] Failed to persist error record: %s", e)

        return err_id

    def log_errors_bulk(self, error_events: List[Any]) -> None:
        """
        Bulk log ErrorEvent objects (from DataCleansingEngine).
        Accepts ErrorEvent dataclass instances or dicts.
        """
        records = []
        for evt in error_events:
            if hasattr(evt, "to_dict"):
                d = evt.to_dict()
            elif isinstance(evt, dict):
                d = evt
            else:
                continue
            records.append(d)
            self._errors.append(evt)

        if records and not self.in_memory and self.backend:
            try:
                self.backend.write_silver_merge(
                    "meta_etl_error",
                    records,
                    merge_keys=["err_id"],
                )
            except Exception as e:
                self.logger.warning("[META] Failed to bulk persist error records: %s", e)

    # ------------------------------------------------------------------
    # Data quality (Subsystem 1)
    # ------------------------------------------------------------------

    def log_quality_check(
        self,
        run_id: str,
        table_name: str,
        check_name: str,
        status: str,
        failed_rows: int,
        total_rows: int,
        details: Optional[str] = None,
    ) -> str:
        """
        Log a data quality check result.
        Subsystem 1 (Data Profiling).

        Args:
            status: PASS | FAIL | WARN
        Returns: check_id
        """
        check_id = _make_id(run_id, table_name, check_name)
        record = DataQualityRecord(
            check_id=check_id,
            run_id=run_id,
            table_name=table_name,
            check_name=check_name,
            status=status,
            failed_rows=failed_rows,
            total_rows=total_rows,
            details=details,
        )
        self._quality.append(record)

        log_fn = self.logger.warning if status in ("FAIL", "WARN") else self.logger.info
        log_fn(
            "[META QC] %s | table=%s | check=%s | failed=%d/%d",
            status, table_name, check_name, failed_rows, total_rows,
        )

        if not self.in_memory and self.backend:
            try:
                self.backend.write_silver_merge(
                    "meta_data_quality",
                    [record.to_dict()],
                    merge_keys=["check_id"],
                )
            except Exception as e:
                self.logger.warning("[META] Failed to persist quality record: %s", e)

        return check_id

    def log_quality_from_profile(self, run_id: str, profile) -> None:
        """
        Bulk log quality checks from a DataProfile object.
        Subsystem 1 (Data Profiling).
        """
        if not hasattr(profile, "to_quality_records"):
            return
        for qr in profile.to_quality_records():
            self.log_quality_check(
                run_id=run_id,
                table_name=qr.get("table_name", ""),
                check_name=qr.get("check_name", ""),
                status=qr.get("status", "PASS"),
                failed_rows=qr.get("failed_rows", 0),
                total_rows=qr.get("total_rows", 0),
                details=qr.get("details"),
            )

    # ------------------------------------------------------------------
    # Lineage tracking (Subsystem 29)
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
        Log a data lineage event.
        Subsystem 29 (Lineage and Dependency Analyzer).

        Args:
            operation: APPEND | MERGE | SCD2 | OVERWRITE | BACKFILL | CDC
        Returns: lineage_id
        """
        lineage_id = _make_id(run_id, source_table, target_table, operation)
        record = LineageRecord(
            lineage_id=lineage_id,
            run_id=run_id,
            source_layer=source_layer,
            source_table=source_table,
            target_layer=target_layer,
            target_table=target_table,
            operation=operation,
            rows_affected=rows_affected,
        )
        self._lineage.append(record)
        self.logger.info(
            "[META LINEAGE] %s.%s → %s.%s | op=%s | rows=%d",
            source_layer, source_table, target_layer, target_table, operation, rows_affected,
        )
        return lineage_id

    # ------------------------------------------------------------------
    # Query / reporting
    # ------------------------------------------------------------------

    def get_run(self, run_id: str) -> Optional[ETLRunRecord]:
        return self._runs.get(run_id)

    def get_all_runs(self) -> pd.DataFrame:
        if not self._runs:
            return pd.DataFrame()
        return pd.DataFrame([r.to_dict() for r in self._runs.values()])

    def get_errors_for_run(self, run_id: str) -> pd.DataFrame:
        errors = [e.to_dict() if hasattr(e, "to_dict") else e for e in self._errors
                  if (e.run_id if hasattr(e, "run_id") else e.get("run_id")) == run_id]
        return pd.DataFrame(errors) if errors else pd.DataFrame()

    def get_quality_summary(self) -> pd.DataFrame:
        if not self._quality:
            return pd.DataFrame()
        return pd.DataFrame([r.to_dict() for r in self._quality])

    def get_lineage_graph(self) -> pd.DataFrame:
        if not self._lineage:
            return pd.DataFrame()
        return pd.DataFrame([r.to_dict() for r in self._lineage])

    def print_run_summary(self, run_id: str) -> None:
        """Print a human-readable summary of a pipeline run."""
        run = self.get_run(run_id)
        if not run:
            print(f"Run {run_id} not found")
            return

        errors = self.get_errors_for_run(run_id)
        quality = self.get_quality_summary()

        print(f"\n{'='*60}")
        print(f"  ETL Run Summary: {run_id}")
        print(f"{'='*60}")
        print(f"  Job:        {run.job_name}")
        print(f"  Layer:      {run.layer} → {run.table_name}")
        print(f"  Status:     {run.status}")
        print(f"  Start:      {run.start_time}")
        print(f"  End:        {run.end_time}")
        print(f"  Duration:   {run.duration_seconds:.1f}s" if run.duration_seconds else "  Duration:   N/A")
        print(f"  Rows Read:  {run.rows_read}")
        print(f"  Rows Written: {run.rows_written}")
        if not errors.empty:
            print(f"\n  Errors ({len(errors)}):")
            for _, e in errors.iterrows():
                print(f"    [{e.get('error_level','?')}] {e.get('error_message','')}")
        if not quality.empty:
            run_quality = quality[quality["run_id"] == run_id] if "run_id" in quality.columns else quality
            if not run_quality.empty:
                fails = run_quality[run_quality["status"].isin(["FAIL", "WARN"])]
                print(f"\n  Quality Checks: {len(run_quality)} total, {len(fails)} FAIL/WARN")
        print(f"{'='*60}\n")
