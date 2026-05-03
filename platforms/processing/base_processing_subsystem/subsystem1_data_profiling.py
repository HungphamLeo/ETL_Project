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
# Configuration
# ---------------------------------------------------------------------------
def load_profiling_config() -> Dict[str, Any]:
    config_path = os.path.join(os.path.dirname(__file__), "config", "data_profiling_config.yaml")
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f).get("data_profiling", {})
    return {}

PROFILING_CONFIG = load_profiling_config()

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

    def to_quality_records(self) -> List[Dict[str, Any]]:
        """Convert to rows for silver_meta_data_quality."""
        records = []
        fail_threshold = PROFILING_CONFIG.get("null_pct_fail_threshold", 0.5)
        warn_threshold = PROFILING_CONFIG.get("null_pct_warn_threshold", 0.1)
        hash_length = PROFILING_CONFIG.get("hash_id_length", 16)
        for col in self.columns:
            status = "FAIL" if col.null_pct > fail_threshold else ("WARN" if col.null_pct > warn_threshold else "PASS")
            records.append({
                "check_id":   hashlib.sha256(f"{self.run_id}|{self.table_name}|{col.column_name}|null_check".encode()).hexdigest()[:hash_length],
                "run_id":     self.run_id,
                "table_name": self.table_name,
                "check_name": f"null_check:{col.column_name}",
                "status":     status,
                "failed_rows": col.null_count,
                "total_rows":  self.total_rows,
                "checked_at":  self.profiled_at.isoformat(),
                "details":     f"null_pct={col.null_pct:.2%}, unique={col.unique_count}",
            })
        return records


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


class DataProfiler:
    """Subsystem 1: Data Profiling – generates statistics on incoming records."""

    @staticmethod
    def profile(
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
        
        sample_size = PROFILING_CONFIG.get("sample_size", 3)
        null_pct_precision = PROFILING_CONFIG.get("null_pct_precision", 4)
        fail_threshold = PROFILING_CONFIG.get("null_pct_fail_threshold", 0.5)
        const_unique_count = PROFILING_CONFIG.get("constant_column_unique_count", 1)
        const_min_rows = PROFILING_CONFIG.get("constant_column_min_total_rows", 10)

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
                issues.append(f"HIGH NULL RATE: {cp.column_name} = {cp.null_pct:.1%}")
            if cp.unique_count == const_unique_count and total > const_min_rows:
                issues.append(f"CONSTANT COLUMN: {cp.column_name}")

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
