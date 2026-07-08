from __future__ import annotations

import hashlib
import re
import os
import yaml
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
import pandas as pd

# ---------------------------------------------------------------------------
# Subsystem 1: Data Profiling
# ---------------------------------------------------------------------------

@dataclass
class ColumnProfile:
    column_name:    str
    total_count:    int
    null_count:     int
    null_pct:       float
    unique_count:   int
    min_value:      Any = None
    max_value:      Any = None
    sample_values:  List[Any] = field(default_factory=list)

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
    checked_at:  datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    details:     Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["checked_at"] = self.checked_at.isoformat()
        return d

@dataclass
class DataProfile:
    """
    Subsystem 1: Data Profiling result.
    Maps to silver_meta_data_quality Delta table.
    """
    table_name:    str
    run_id:        str
    profiled_at:   datetime
    total_rows:    int
    columns:       List[ColumnProfile] = field(default_factory=list)
    issues:        List[str] = field(default_factory=list)

    def to_quality_records(self, profile_config) -> List[Dict[str, Any]]:
        """Convert to rows for silver_meta_data_quality."""
        records = []
        fail_threshold = profile_config.get("null_pct_fail_threshold", 0.5)
        warn_threshold = profile_config.get("null_pct_warn_threshold", 0.1)
        hash_length = profile_config.get("hash_id_length", 16)
        for col in self.columns:
            status = "FAIL" if col.null_pct > fail_threshold else ("WARN" if col.null_pct > warn_threshold else "PASS")
            
            # Tự động gộp toàn bộ thuộc tính của DataProfile và ColumnProfile vào context
            context = asdict(self).copy()
            context.pop("columns", None)
            context.pop("issues", None)
            context.update(asdict(col))
            context["status"] = status
            context["checked_at"] = self.profiled_at.isoformat()
            
            record = {}
            QUALITY_RECORD_SCHEMA = profile_config.get("quality_record_schema")
            for field, template in QUALITY_RECORD_SCHEMA.items():
                if template in context:
                    record[field] = context[template]
                elif isinstance(template, str):
                    formatted_val = template.format(**context)
                    if field == "check_id" and profile_config.get("hash_check_id", True):
                        formatted_val = hashlib.sha256(formatted_val.encode()).hexdigest()[:hash_length]
                    record[field] = formatted_val
                else:
                    record[field] = template
                    
            records.append(record)
        return records



class DataProfiler:
    """Subsystem 1: Data Profiling – generates statistics on incoming records."""

    @staticmethod
    def profile(
        profile_config,
        records: List[Dict[str, Any]],
        table_name: str,
        run_id: str = "unknown",
    ) -> DataProfile:
        if not records:
            return DataProfile(
                table_name=table_name,
                run_id=run_id,
                profiled_at=datetime.now(timezone.utc),
                total_rows=0,
            )

        df = pd.DataFrame(records)
        total = len(df)
        col_profiles = []
        
        sample_size = profile_config.get("sample_size", 3)
        null_pct_precision = profile_config.get("null_pct_precision", 4)
        fail_threshold = profile_config.get("null_pct_fail_threshold", 0.5)
        const_unique_count = profile_config.get("constant_column_unique_count", 1)
        const_min_rows = profile_config.get("constant_column_min_total_rows", 10)
        high_null_msg_template = profile_config.get("high_null_msg_template", "HIGH NULL RATE: {column_name} = {null_pct:.1%}")
        const_col_msg_template = profile_config.get("const_col_msg_template", "CONSTANT COLUMN: {column_name}")

        for col in df.columns:
            series = df[col]
            null_count = int(series.isna().sum())
            unique_count = int(series.nunique(dropna=True))
            non_null = series.dropna()

            try:
                min_val = non_null.min() if len(non_null) > 0 else None
                max_val = non_null.max() if len(non_null) > 0 else None
            except Exception:
                min_val = max_val = None

            sample = non_null.head(sample_size).tolist()

            col_profiles.append(ColumnProfile(
                column_name=col,
                total_count=total,
                null_count=null_count,
                null_pct=round(null_count / total, null_pct_precision) if total > 0 else 0.0,
                unique_count=unique_count,
                min_value=min_val,
                max_value=max_val,
                sample_values=sample,
            ))

        issues = []
        for cp in col_profiles:
            if cp.null_pct > fail_threshold:
                issues.append(high_null_msg_template.format(column_name=cp.column_name, null_pct=cp.null_pct))
            if cp.unique_count == const_unique_count and total > const_min_rows:
                issues.append(const_col_msg_template.format(column_name=cp.column_name))

        return DataProfile(
            table_name=table_name,
            run_id=run_id,
            profiled_at=datetime.now(timezone.utc),
            total_rows=total,
            columns=col_profiles,
            issues=issues,
        )

    @staticmethod
    def profile_dataframe(df: pd.DataFrame, table_name: str, run_id: str = "unknown") -> DataProfile:
        return DataProfiler.profile(
            df.where(pd.notnull(df), None).to_dict(orient="records"),
            table_name=table_name,
            run_id=run_id,
        )


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
def load_profiling_config() -> Dict[str, Any]:
    config_path = os.path.join(os.path.dirname(__file__), "config", "data_profiling_config.yaml")
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f).get("data_profiling", {})
    return {}


# if __name__ == "__main__":
#     PROFILING_CONFIG = load_profiling_config()
    # Example usage
