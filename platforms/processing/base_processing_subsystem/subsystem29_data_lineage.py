from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from shared.logger.python_main_logger import logger_manager
from platforms.processing.base_processing_subsystem.subsystem22_job_schedule import _make_id, _now_utc
from platforms.processing.base_processing_subsystem.subsystem27_workflow_monitoring import ETLRunRecord
import pandas as pd



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


class LineageTracker:
    """Tracks data lineage across pipeline execution."""
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logger_manager.get_logger(__name__)
        self._lineage: List[LineageRecord] = []
        self._runs: Dict[str, ETLRunRecord] = {}
        self._errors: List[Dict[str, Any]] = []
        self._quality: List[Dict[str, Any]] = []

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

    def get_all_runs(self) -> pd.DataFrame:
        if not self._runs:
            return pd.DataFrame()
        return pd.DataFrame([r.to_dict() for r in self._runs.values()])

    def get_errors_for_run(self, run_id: str) -> pd.DataFrame:
        errors = [e for e in self._errors if e.get("run_id") == run_id]
        return pd.DataFrame(errors) if errors else pd.DataFrame()

    def get_quality_summary(self) -> pd.DataFrame:
        if not self._quality:
            return pd.DataFrame()
        return pd.DataFrame(self._quality)

    def get_lineage_graph(self) -> pd.DataFrame:
        if not self._lineage:
            return pd.DataFrame()
        return pd.DataFrame([r.to_dict() for r in self._lineage])

    def log_run_summary(self, run_id: str) -> None:
        """Log a human-readable summary of a pipeline run to logger."""
        run = self._runs.get(run_id)
        if not run:
            self.logger.warning(f"Run {run_id} not found")
            return

        summary_lines = [
            f"\n{'='*60}",
            f"  ETL Run Summary: {run_id}",
            f"{'='*60}",
            f"  Job:        {run.job_name}",
            f"  Layer:      {run.layer} → {run.table_name}",
            f"  Status:     {run.status}",
            f"  Start:      {run.start_time}",
            f"  End:        {run.end_time}",
            f"  Duration:   {run.duration_seconds:.1f}s" if run.duration_seconds else "  Duration:   N/A",
            f"  Rows Read:  {run.rows_read}",
            f"  Rows Written: {run.rows_written}",
        ]

        error_items = [e for e in self._errors if e.get("run_id") == run_id]
        if error_items:
            summary_lines.append(f"\n  Errors ({len(error_items)}):")
            for e in error_items:
                summary_lines.append(f"    [{e.get('error_level','?')}] {e.get('error_message','')}")

        quality_items = [q for q in self._quality if q.get("run_id") == run_id]
        if quality_items:
            fails = [q for q in quality_items if q.get("status") in ("FAIL", "WARN")]
            summary_lines.append(f"\n  Quality Checks: {len(quality_items)} total, {len(fails)} FAIL/WARN")

        summary_lines.append(f"{'='*60}\n")
        summary_msg = "\n".join(summary_lines)
        self.logger.info(summary_msg)