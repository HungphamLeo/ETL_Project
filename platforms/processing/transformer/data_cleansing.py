"""
Data Cleansing System + Error Event Schema
==========================================
Subsystem 1  : Data Profiling  – profile() generates statistics on incoming data
Subsystem 4  : Data Cleansing  – DataCleansingEngine applies configurable rules
Subsystem 5  : Error Event Schema – ErrorEvent dataclass + ErrorEventLog collector
Subsystem 30 : Problem Escalation – escalate() logs FATAL errors with full context

Usage:
    engine = DataCleansingEngine(rules=COPHIEU68_CLEANSING_RULES)
    cleaned, errors = engine.cleanse(records, source="cophieu68.trading_data", run_id="run_001")
    profile = DataProfiler.profile(records, table_name="bronze_trading_data")
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd


# ---------------------------------------------------------------------------
# Subsystem 5: Error Event Schema
# ---------------------------------------------------------------------------

class ErrorLevel(str, Enum):
    WARNING = "WARNING"
    ERROR   = "ERROR"
    FATAL   = "FATAL"


@dataclass
class ErrorEvent:
    """
    Canonical error event record.
    Subsystem 5 (Error Event Schema) – every cleansing failure produces one.
    Maps directly to silver_meta_etl_error Delta table.
    """
    err_id:        str
    run_id:        str
    job_name:      str
    error_level:   ErrorLevel
    error_message: str
    record_id:     Optional[str] = None   # natural key of the failed record
    raw_json:      Optional[str] = None   # serialized raw record
    err_time:      datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    retry_count:   int = 0

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["error_level"] = self.error_level.value
        d["err_time"] = self.err_time.isoformat()
        return d

    @staticmethod
    def _make_id(run_id: str, record_id: str, message: str) -> str:
        raw = f"{run_id}|{record_id}|{message}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    @classmethod
    def create(
        cls,
        run_id: str,
        job_name: str,
        level: ErrorLevel,
        message: str,
        record: Optional[Dict[str, Any]] = None,
        record_id_fields: Optional[List[str]] = None,
    ) -> "ErrorEvent":
        record = record or {}
        record_id = "|".join(str(record.get(f, "")) for f in (record_id_fields or []))
        err_id = cls._make_id(run_id, record_id, message)
        raw_json = json.dumps(record, default=str, ensure_ascii=False) if record else None
        return cls(
            err_id=err_id,
            run_id=run_id,
            job_name=job_name,
            error_level=level,
            error_message=message,
            record_id=record_id or None,
            raw_json=raw_json,
        )


class ErrorEventLog:
    """
    Collects ErrorEvents during a pipeline run.
    Subsystem 5 + 30 (Problem Escalation).
    """

    def __init__(self, run_id: str, job_name: str, logger: Optional[logging.Logger] = None):
        self.run_id = run_id
        self.job_name = job_name
        self.logger = logger or logging.getLogger(__name__)
        self._events: List[ErrorEvent] = []

    def add(
        self,
        level: ErrorLevel,
        message: str,
        record: Optional[Dict[str, Any]] = None,
        record_id_fields: Optional[List[str]] = None,
    ) -> ErrorEvent:
        evt = ErrorEvent.create(
            run_id=self.run_id,
            job_name=self.job_name,
            level=level,
            message=message,
            record=record,
            record_id_fields=record_id_fields,
        )
        self._events.append(evt)

        # Log to Python logger
        log_fn = {
            ErrorLevel.WARNING: self.logger.warning,
            ErrorLevel.ERROR:   self.logger.error,
            ErrorLevel.FATAL:   self.logger.critical,
        }.get(level, self.logger.error)
        log_fn("[%s] %s | record_id=%s", level.value, message, evt.record_id)

        # Subsystem 30: escalate FATAL immediately
        if level == ErrorLevel.FATAL:
            self._escalate(evt)

        return evt

    def _escalate(self, evt: ErrorEvent) -> None:
        """Subsystem 30: Problem Escalation – hook for alerting (Slack, PagerDuty, etc.)."""
        self.logger.critical(
            "🚨 FATAL ERROR ESCALATION | run_id=%s | job=%s | %s",
            evt.run_id, evt.job_name, evt.error_message,
        )
        # TODO: integrate with alerting system (Slack webhook, email, etc.)

    @property
    def events(self) -> List[ErrorEvent]:
        return list(self._events)

    @property
    def has_fatal(self) -> bool:
        return any(e.error_level == ErrorLevel.FATAL for e in self._events)

    @property
    def has_errors(self) -> bool:
        return any(e.error_level in (ErrorLevel.ERROR, ErrorLevel.FATAL) for e in self._events)

    def summary(self) -> Dict[str, int]:
        counts: Dict[str, int] = {level.value: 0 for level in ErrorLevel}
        for e in self._events:
            counts[e.error_level.value] += 1
        return counts

    def to_records(self) -> List[Dict[str, Any]]:
        return [e.to_dict() for e in self._events]

    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame(self.to_records()) if self._events else pd.DataFrame()


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
        for col in self.columns:
            status = "FAIL" if col.null_pct > 0.5 else ("WARN" if col.null_pct > 0.1 else "PASS")
            records.append({
                "check_id":   hashlib.sha256(f"{self.run_id}|{self.table_name}|{col.column_name}|null_check".encode()).hexdigest()[:16],
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

            sample = non_null.head(3).tolist()

            col_profiles.append(ColumnProfile(
                column_name=col,
                total_count=total,
                null_count=null_count,
                null_pct=round(null_count / total, 4) if total > 0 else 0.0,
                unique_count=unique_count,
                min_value=min_val,
                max_value=max_val,
                sample_values=sample,
            ))

        issues = []
        for cp in col_profiles:
            if cp.null_pct > 0.5:
                issues.append(f"HIGH NULL RATE: {cp.column_name} = {cp.null_pct:.1%}")
            if cp.unique_count == 1 and total > 10:
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


# ---------------------------------------------------------------------------
# Subsystem 4: Data Cleansing Rules
# ---------------------------------------------------------------------------

# Rule type: a callable that takes a record dict and returns (is_valid, message)
CleansingRule = Callable[[Dict[str, Any]], Tuple[bool, str]]


def rule_not_null(*fields: str) -> CleansingRule:
    """Reject records where any of the specified fields is None/empty."""
    def check(record: Dict[str, Any]) -> Tuple[bool, str]:
        for f in fields:
            val = record.get(f)
            if val is None or (isinstance(val, str) and val.strip() == ""):
                return False, f"Field '{f}' is null or empty"
        return True, ""
    return check


def rule_min_length(field: str, min_len: int) -> CleansingRule:
    def check(record: Dict[str, Any]) -> Tuple[bool, str]:
        val = record.get(field, "")
        if val and len(str(val)) < min_len:
            return False, f"Field '{field}' too short (min={min_len})"
        return True, ""
    return check


def rule_regex(field: str, pattern: str) -> CleansingRule:
    compiled = re.compile(pattern)
    def check(record: Dict[str, Any]) -> Tuple[bool, str]:
        val = str(record.get(field, ""))
        if val and not compiled.match(val):
            return False, f"Field '{field}' does not match pattern '{pattern}'"
        return True, ""
    return check


def rule_numeric_range(field: str, min_val: float = None, max_val: float = None) -> CleansingRule:
    def check(record: Dict[str, Any]) -> Tuple[bool, str]:
        val = record.get(field)
        if val is None:
            return True, ""
        try:
            num = float(val)
            if min_val is not None and num < min_val:
                return False, f"Field '{field}' = {num} below min {min_val}"
            if max_val is not None and num > max_val:
                return False, f"Field '{field}' = {num} above max {max_val}"
        except (ValueError, TypeError):
            return False, f"Field '{field}' = '{val}' is not numeric"
        return True, ""
    return check


def rule_date_format(field: str, fmt: str = "%d/%m/%Y") -> CleansingRule:
    def check(record: Dict[str, Any]) -> Tuple[bool, str]:
        val = record.get(field)
        if not val:
            return True, ""
        try:
            datetime.strptime(str(val), fmt)
        except ValueError:
            return False, f"Field '{field}' = '{val}' does not match date format '{fmt}'"
        return True, ""
    return check


def rule_allowed_values(field: str, allowed: List[Any]) -> CleansingRule:
    allowed_set = set(allowed)
    def check(record: Dict[str, Any]) -> Tuple[bool, str]:
        val = record.get(field)
        if val is not None and val not in allowed_set:
            return False, f"Field '{field}' = '{val}' not in allowed values {allowed}"
        return True, ""
    return check


# ---------------------------------------------------------------------------
# Pre-built rule sets for cophieu68 data
# ---------------------------------------------------------------------------

COPHIEU68_TRADING_DATA_RULES: List[CleansingRule] = [
    rule_not_null("symbol"),
    rule_regex("symbol", r"^[A-Z0-9]{2,10}$"),
    rule_not_null("date"),
    rule_numeric_range("close_price", min_val=0.0, max_val=1_000_000.0),
    rule_numeric_range("volume", min_val=0),
    rule_numeric_range("open_price", min_val=0.0),
    rule_numeric_range("high_price", min_val=0.0),
    rule_numeric_range("low_price", min_val=0.0),
]

COPHIEU68_COMPANY_INFO_RULES: List[CleansingRule] = [
    rule_not_null("symbol"),
    rule_regex("symbol", r"^[A-Z0-9]{2,10}$"),
    rule_min_length("company_name", 2),
]

COPHIEU68_FINANCIAL_REPORT_RULES: List[CleansingRule] = [
    rule_not_null("symbol"),
    rule_not_null("report_type"),
    rule_allowed_values("report_type", ["quarter", "year", "QUARTERLY", "ANNUALLY"]),
]

COPHIEU68_INDUSTRY_RULES: List[CleansingRule] = [
    rule_not_null("industry_code"),
    rule_not_null("symbol"),
]


# ---------------------------------------------------------------------------
# Subsystem 4: Data Cleansing Engine
# ---------------------------------------------------------------------------

@dataclass
class CleansingResult:
    cleaned:       List[Dict[str, Any]]
    rejected:      List[Dict[str, Any]]
    error_events:  List[ErrorEvent]
    total_input:   int
    total_cleaned: int
    total_rejected: int
    profile:       Optional[DataProfile] = None

    def summary(self) -> Dict[str, Any]:
        return {
            "total_input":    self.total_input,
            "total_cleaned":  self.total_cleaned,
            "total_rejected": self.total_rejected,
            "rejection_rate": round(self.total_rejected / self.total_input, 4) if self.total_input else 0,
            "error_counts":   {
                level.value: sum(1 for e in self.error_events if e.error_level == level)
                for level in ErrorLevel
            },
        }


class DataCleansingEngine:
    """
    Subsystem 4: Data Cleansing System.
    Applies a list of CleansingRules to each record.
    Records failing any rule are rejected and logged as ErrorEvents.
    """

    def __init__(
        self,
        rules: Optional[List[CleansingRule]] = None,
        rejection_level: ErrorLevel = ErrorLevel.WARNING,
        run_profiling: bool = True,
        logger: Optional[logging.Logger] = None,
    ):
        self.rules = rules or []
        self.rejection_level = rejection_level
        self.run_profiling = run_profiling
        self.logger = logger or logging.getLogger(__name__)

    def add_rule(self, rule: CleansingRule) -> "DataCleansingEngine":
        self.rules.append(rule)
        return self

    def cleanse(
        self,
        records: List[Dict[str, Any]],
        source: str = "unknown",
        run_id: str = "unknown",
        record_id_fields: Optional[List[str]] = None,
    ) -> CleansingResult:
        """
        Apply all rules to each record.
        Returns CleansingResult with cleaned records + error events.

        Args:
            records: Input records (list of dicts)
            source: Source identifier for error events (e.g. "cophieu68.trading_data")
            run_id: Pipeline run ID for lineage
            record_id_fields: Fields to use as record identifier in error events
        """
        error_log = ErrorEventLog(run_id=run_id, job_name=source, logger=self.logger)
        cleaned: List[Dict[str, Any]] = []
        rejected: List[Dict[str, Any]] = []

        # Subsystem 1: Profile before cleansing
        profile = None
        if self.run_profiling and records:
            profile = DataProfiler.profile(records, table_name=source, run_id=run_id)
            if profile.issues:
                for issue in profile.issues:
                    error_log.add(ErrorLevel.WARNING, f"[PROFILING] {issue}")

        for record in records:
            record_valid = True
            for rule in self.rules:
                try:
                    is_valid, message = rule(record)
                    if not is_valid:
                        error_log.add(
                            self.rejection_level,
                            message,
                            record=record,
                            record_id_fields=record_id_fields,
                        )
                        record_valid = False
                        break  # stop at first failing rule per record
                except Exception as exc:
                    error_log.add(
                        ErrorLevel.ERROR,
                        f"Rule evaluation error: {exc}",
                        record=record,
                        record_id_fields=record_id_fields,
                    )
                    record_valid = False
                    break

            if record_valid:
                cleaned.append(record)
            else:
                rejected.append(record)

        return CleansingResult(
            cleaned=cleaned,
            rejected=rejected,
            error_events=error_log.events,
            total_input=len(records),
            total_cleaned=len(cleaned),
            total_rejected=len(rejected),
            profile=profile,
        )

    def cleanse_dataframe(
        self,
        df: pd.DataFrame,
        source: str = "unknown",
        run_id: str = "unknown",
        record_id_fields: Optional[List[str]] = None,
    ) -> Tuple[pd.DataFrame, CleansingResult]:
        """Convenience wrapper for pandas DataFrames."""
        records = df.where(pd.notnull(df), None).to_dict(orient="records")
        result = self.cleanse(records, source=source, run_id=run_id, record_id_fields=record_id_fields)
        cleaned_df = pd.DataFrame(result.cleaned) if result.cleaned else pd.DataFrame(columns=df.columns)
        return cleaned_df, result


# ---------------------------------------------------------------------------
# Transformation helpers (field-level cleaning)
# ---------------------------------------------------------------------------

class FieldTransformer:
    """
    Subsystem 4: Field-level transformations applied after validation.
    Converts raw crawled strings to typed values.
    """

    @staticmethod
    def clean_numeric(value: Any, default: Optional[float] = None) -> Optional[float]:
        """Remove commas/spaces, convert to float."""
        if value is None:
            return default
        try:
            cleaned = re.sub(r"[,\s]", "", str(value))
            return float(cleaned)
        except (ValueError, TypeError):
            return default

    @staticmethod
    def clean_symbol(value: Any) -> Optional[str]:
        """Uppercase and strip stock symbol."""
        if not value:
            return None
        return str(value).strip().upper()

    @staticmethod
    def clean_date_vn(value: Any) -> Optional[str]:
        """Convert dd/mm/yyyy → yyyy-mm-dd ISO format."""
        if not value:
            return None
        try:
            dt = datetime.strptime(str(value).strip(), "%d/%m/%Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            return str(value)

    @staticmethod
    def clean_percentage(value: Any) -> Optional[float]:
        """Convert '12.5%' → 0.125."""
        if value is None:
            return None
        try:
            cleaned = str(value).replace("%", "").strip()
            return float(cleaned) / 100.0
        except (ValueError, TypeError):
            return None

    @staticmethod
    def normalize_report_type(value: Any) -> Optional[str]:
        """Normalize report_type to ANNUALLY|QUARTERLY."""
        if not value:
            return None
        mapping = {
            "year": "ANNUALLY", "annual": "ANNUALLY", "annually": "ANNUALLY",
            "quarter": "QUARTERLY", "quarterly": "QUARTERLY", "q": "QUARTERLY",
        }
        return mapping.get(str(value).lower().strip(), str(value).upper())

    @staticmethod
    def transform_trading_record(record: Dict[str, Any]) -> Dict[str, Any]:
        """Apply all field transformations to a raw trading data record."""
        return {
            "symbol":        FieldTransformer.clean_symbol(record.get("symbol")),
            "date":          FieldTransformer.clean_date_vn(record.get("date") or record.get("trade_date")),
            "close_price":   FieldTransformer.clean_numeric(record.get("close_price") or record.get("close")),
            "open_price":    FieldTransformer.clean_numeric(record.get("open_price") or record.get("open")),
            "high_price":    FieldTransformer.clean_numeric(record.get("high_price") or record.get("high")),
            "low_price":     FieldTransformer.clean_numeric(record.get("low_price") or record.get("low")),
            "volume":        FieldTransformer.clean_numeric(record.get("volume")),
            "foreign_buy":   FieldTransformer.clean_numeric(record.get("foreign_buy")),
            "foreign_sell":  FieldTransformer.clean_numeric(record.get("foreign_sell")),
            "foreign_value": FieldTransformer.clean_numeric(record.get("foreign_value") or record.get("foreign_net_value")),
        }

    @staticmethod
    def transform_financial_record(record: Dict[str, Any]) -> Dict[str, Any]:
        """Apply field transformations to a raw financial report record."""
        return {
            "symbol":               FieldTransformer.clean_symbol(record.get("symbol")),
            "report_type":          FieldTransformer.normalize_report_type(record.get("report_type")),
            "year":                 str(record.get("year", "")).strip() or None,
            "period":               str(record.get("period", "")).strip() or None,
            "metric_code":          str(record.get("metric_code", "")).strip() or None,
            "metric_value":         FieldTransformer.clean_numeric(record.get("metric_value")),
            "metric_name_en":       record.get("metric_name_en"),
            "metric_group":         record.get("metric_group"),
        }
