from __future__ import annotations

import logging
import os
import yaml
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from shared.logger.python_main_logger import logger_manager
from platforms.processing.base_processing_subsystem.subsystem22_job_schedule import _make_id, _now_utc
from platforms.processing.base_processing_subsystem.subsystem27_workflow_monitoring import ETLRunRecord
import pandas as pd

def load_lineage_config() -> Dict[str, Any]:
    config_path = os.path.join(os.path.dirname(__file__), "config", "data_lineage.yaml")
    if os.path.exists(config_path):
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
                return config.get("data_lineage", {}) if config else {}
        except Exception:
            pass
    return {}

LINEAGE_CONFIG = load_lineage_config()
MSG_TEMPLATES = LINEAGE_CONFIG.get("msg_templates", {})


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
        context = asdict(self)
        context["recorded_at"] = self.recorded_at.isoformat()
        
        schema = LINEAGE_CONFIG.get("lineage_schema")
        if not schema:
            return context
            
        record = {}
        for field_name, template in schema.items():
            if isinstance(template, str):
                if "{" in template and "}" in template:
                    record[field_name] = template.format(**context)
                elif template in context:
                    record[field_name] = context[template]
                else:
                    record[field_name] = template
            else:
                record[field_name] = template
        return record


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
        
        template = MSG_TEMPLATES.get("lineage_logged", "[META LINEAGE] {source_layer}.{source_table} -> {target_layer}.{target_table} | op={operation} | rows={rows_affected}")
        self.logger.info(template.format(
            source_layer=source_layer,
            source_table=source_table,
            target_layer=target_layer,
            target_table=target_table,
            operation=operation,
            rows_affected=rows_affected
        ))
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
            template = MSG_TEMPLATES.get("run_not_found", "Run {run_id} not found")
            self.logger.warning(template.format(run_id=run_id))
            return

        header_tmpl = MSG_TEMPLATES.get("summary_header", "\n" + "="*60 + "\n  ETL Run Summary: {run_id}\n" + "="*60)
        body_tmpl = MSG_TEMPLATES.get("summary_body", "  Job:        {job_name}\n  Layer:      {layer} -> {table_name}\n  Status:     {status}\n  Start:      {start_time}\n  End:        {end_time}\n  Duration:   {duration}\n  Rows Read:  {rows_read}\n  Rows Written: {rows_written}")
        duration_str = f"{run.duration_seconds:.1f}s" if run.duration_seconds else "N/A"

        summary_lines = [
            header_tmpl.format(run_id=run_id),
            body_tmpl.format(
                job_name=run.job_name,
                layer=run.layer,
                table_name=run.table_name,
                status=run.status,
                start_time=run.start_time,
                end_time=run.end_time,
                duration=duration_str,
                rows_read=run.rows_read,
                rows_written=run.rows_written
            )
        ]

        error_items = [e for e in self._errors if e.get("run_id") == run_id]
        if error_items:
            err_hdr = MSG_TEMPLATES.get("summary_errors_header", "\n  Errors ({error_count}):")
            summary_lines.append(err_hdr.format(error_count=len(error_items)))
            
            err_item_tmpl = MSG_TEMPLATES.get("summary_error_item", "    [{error_level}] {error_message}")
            for e in error_items:
                summary_lines.append(err_item_tmpl.format(
                    error_level=e.get('error_level', '?'),
                    error_message=e.get('error_message', '')
                ))

        quality_items = [q for q in self._quality if q.get("run_id") == run_id]
        if quality_items:
            fails = [q for q in quality_items if q.get("status") in ("FAIL", "WARN")]
            qual_tmpl = MSG_TEMPLATES.get("summary_quality", "\n  Quality Checks: {total_checks} total, {fail_checks} FAIL/WARN")
            summary_lines.append(qual_tmpl.format(total_checks=len(quality_items), fail_checks=len(fails)))

        footer_tmpl = MSG_TEMPLATES.get("summary_footer", "="*60 + "\n")
        summary_lines.append(footer_tmpl)
        summary_msg = "\n".join(summary_lines)
        self.logger.info(summary_msg)