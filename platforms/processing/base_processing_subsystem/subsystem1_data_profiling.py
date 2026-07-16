from __future__ import annotations

import hashlib
import logging
import os
import warnings
import yaml
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import pandas as pd

# Suppress GE progress-bar noise in pipeline logs
warnings.filterwarnings("ignore", category=UserWarning, module="great_expectations")

# ---------------------------------------------------------------------------
# Subsystem 1: Data Profiling  (backed by Great Expectations for DQ logging)
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
    # GE validation results stored here for downstream reporting / Grafana
    ge_results:    List[Dict[str, Any]] = field(default_factory=list)

    def to_quality_records(self, profile_config) -> List[Dict[str, Any]]:
        """Convert to rows for silver_meta_data_quality."""
        records = []
        fail_threshold = profile_config.get("null_pct_fail_threshold", 0.5)
        warn_threshold = profile_config.get("null_pct_warn_threshold", 0.1)
        hash_length = profile_config.get("hash_id_length", 16)
        for col in self.columns:
            status = "FAIL" if col.null_pct > fail_threshold else ("WARN" if col.null_pct > warn_threshold else "PASS")

            context = asdict(self).copy()
            context.pop("columns", None)
            context.pop("issues", None)
            context.pop("ge_results", None)
            context.update(asdict(col))
            context["status"] = status
            context["checked_at"] = self.profiled_at.isoformat()

            record = {}
            QUALITY_RECORD_SCHEMA = profile_config.get("quality_record_schema")
            if not QUALITY_RECORD_SCHEMA:
                continue
            for fld, template in QUALITY_RECORD_SCHEMA.items():
                if template in context:
                    record[fld] = context[template]
                elif isinstance(template, str):
                    formatted_val = template.format(**context)
                    if fld == "check_id" and profile_config.get("hash_check_id", True):
                        formatted_val = hashlib.sha256(formatted_val.encode()).hexdigest()[:hash_length]
                    record[fld] = formatted_val
                else:
                    record[fld] = template

            records.append(record)
        return records


# ---------------------------------------------------------------------------
# GE helper – runs expectations on a pandas DataFrame, returns structured log
# Never raises; failures are captured as log entries only.
# ---------------------------------------------------------------------------

def _run_ge_profiling(
    df: pd.DataFrame,
    table_name: str,
    run_id: str,
    profile_config: Dict[str, Any],
    logger: Optional[logging.Logger] = None,
) -> List[Dict[str, Any]]:
    """
    Runs a standard set of Great Expectations checks on *df*.
    Returns a list of result dicts (one per expectation per column).
    The pipeline is NEVER blocked — all exceptions are swallowed.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    results: List[Dict[str, Any]] = []

    try:
        import great_expectations as gx

        fail_threshold = profile_config.get("null_pct_fail_threshold", 0.5)
        warn_threshold = profile_config.get("null_pct_warn_threshold", 0.1)
        ge_cfg = profile_config.get("great_expectations", {})
        mostly_not_null = ge_cfg.get("mostly_not_null", 1.0 - fail_threshold)
        check_unique_cols: List[str] = ge_cfg.get("check_unique_columns", [])
        numeric_ranges: Dict[str, Any] = ge_cfg.get("numeric_ranges", {})

        # Suppress GE's internal tqdm progress bars during batch validation
        import logging as _logging
        _logging.getLogger("great_expectations").setLevel(_logging.ERROR)

        context = gx.get_context(mode="ephemeral")
        ds = context.data_sources.add_pandas(name=f"ds_{table_name}_{run_id}")
        da = ds.add_dataframe_asset(name="asset")
        batch_def = da.add_batch_definition_whole_dataframe("batch")
        batch = batch_def.get_batch(batch_parameters={"dataframe": df})

        checked_at = datetime.now(timezone.utc).isoformat()

        def _run(exp, col_name: str, exp_type: str, extra: Optional[Dict] = None):
            """Run a single expectation and record result."""
            try:
                vr = batch.validate(exp)
                success = bool(vr.success)
                res_detail = vr.result or {}
                unexpected_count = res_detail.get("unexpected_count", 0)
                element_count = res_detail.get("element_count", len(df))
                entry = {
                    "table_name": table_name,
                    "run_id": run_id,
                    "column": col_name,
                    "expectation_type": exp_type,
                    "success": success,
                    "status": "PASS" if success else "FAIL",
                    "unexpected_count": unexpected_count,
                    "element_count": element_count,
                    "checked_at": checked_at,
                    **(extra or {}),
                }
                results.append(entry)

                if not success:
                    logger.warning(
                        "[GE|%s] FAIL  %-40s | unexpected=%s/%s | %s",
                        table_name,
                        f"{exp_type}({col_name})",
                        unexpected_count,
                        element_count,
                        extra or "",
                    )
                else:
                    logger.debug(
                        "[GE|%s] PASS  %s(%s)",
                        table_name, exp_type, col_name,
                    )
            except Exception as exc:  # noqa: BLE001
                logger.debug("[GE] expectation %s(%s) errored: %s", exp_type, col_name, exc)

        # ── Per-column expectations ──────────────────────────────────────────
        for col in df.columns:
            # 1. Null check
            _run(
                gx.expectations.ExpectColumnProportionOfNonNullValuesToBeBetween(
                    column=col,
                    min_value=mostly_not_null,
                    max_value=1.0,
                ),
                col,
                "not_null_proportion",
                {"min_proportion": mostly_not_null},
            )

            # 2. Unique check (only for flagged columns)
            if col in check_unique_cols:
                _run(
                    gx.expectations.ExpectColumnValuesToBeUnique(column=col),
                    col,
                    "unique_values",
                )

            # 3. Numeric range checks from config
            if col in numeric_ranges:
                rng = numeric_ranges[col]
                _run(
                    gx.expectations.ExpectColumnValuesToBeBetween(
                        column=col,
                        min_value=rng.get("min"),
                        max_value=rng.get("max"),
                        mostly=rng.get("mostly", 1.0),
                    ),
                    col,
                    "numeric_range",
                    {"range": rng},
                )

        logger.info(
            "[GE|%s] Profiling complete — %d checks, %d FAIL",
            table_name,
            len(results),
            sum(1 for r in results if r["status"] == "FAIL"),
        )

    except Exception as exc:  # noqa: BLE001
        logger.warning("[GE] Profiling skipped for %s: %s", table_name, exc)

    return results


class DataProfiler:
    """Subsystem 1: Data Profiling – generates statistics + GE checks on incoming records."""

    @staticmethod
    def profile(
        profile_config: Dict[str, Any],
        records: List[Dict[str, Any]],
        table_name: str,
        run_id: str = "unknown",
        logger: Optional[logging.Logger] = None,
    ) -> DataProfile:
        if not records:
            return DataProfile(
                table_name=table_name,
                run_id=run_id,
                profiled_at=datetime.now(timezone.utc),
                total_rows=0,
            )

        if logger is None:
            logger = logging.getLogger(__name__)

        df = pd.DataFrame(records)
        total = len(df)
        col_profiles = []

        sample_size          = profile_config.get("sample_size", 3)
        null_pct_precision   = profile_config.get("null_pct_precision", 4)
        fail_threshold       = profile_config.get("null_pct_fail_threshold", 0.5)
        const_unique_count   = profile_config.get("constant_column_unique_count", 1)
        const_min_rows       = profile_config.get("constant_column_min_total_rows", 10)
        high_null_msg_tmpl   = profile_config.get("high_null_msg_template", "HIGH NULL RATE: {column_name} = {null_pct:.1%}")
        const_col_msg_tmpl   = profile_config.get("const_col_msg_template", "CONSTANT COLUMN: {column_name}")

        for col in df.columns:
            series      = df[col]
            null_count  = int(series.isna().sum())
            unique_count = int(series.nunique(dropna=True))
            non_null    = series.dropna()

            try:
                min_val = non_null.min() if len(non_null) > 0 else None
                max_val = non_null.max() if len(non_null) > 0 else None
            except Exception:
                min_val = max_val = None

            sample = non_null.head(sample_size).tolist()

            col_profiles.append(ColumnProfile(
                column_name  = col,
                total_count  = total,
                null_count   = null_count,
                null_pct     = round(null_count / total, null_pct_precision) if total > 0 else 0.0,
                unique_count = unique_count,
                min_value    = min_val,
                max_value    = max_val,
                sample_values = sample,
            ))

        issues = []
        for cp in col_profiles:
            if cp.null_pct > fail_threshold:
                issues.append(high_null_msg_tmpl.format(column_name=cp.column_name, null_pct=cp.null_pct))
            if cp.unique_count == const_unique_count and total > const_min_rows:
                issues.append(const_col_msg_tmpl.format(column_name=cp.column_name))

        # ── Great Expectations run (observation only, never blocks) ──────────
        ge_results = _run_ge_profiling(df, table_name, run_id, profile_config, logger)

        return DataProfile(
            table_name  = table_name,
            run_id      = run_id,
            profiled_at = datetime.now(timezone.utc),
            total_rows  = total,
            columns     = col_profiles,
            issues      = issues,
            ge_results  = ge_results,
        )

    @staticmethod
    def profile_dataframe(
        profile_config: Dict[str, Any],
        df: pd.DataFrame,
        table_name: str,
        run_id: str = "unknown",
        logger: Optional[logging.Logger] = None,
    ) -> "DataProfile":
        return DataProfiler.profile(
            profile_config,
            df.where(pd.notnull(df), None).to_dict(orient="records"),
            table_name=table_name,
            run_id=run_id,
            logger=logger,
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


if __name__ == "__main__":
    PROFILING_CONFIG = load_profiling_config()
    # Example usage
