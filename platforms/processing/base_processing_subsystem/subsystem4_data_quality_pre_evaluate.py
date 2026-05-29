from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import yaml
from dataclasses import dataclass
from datetime import datetime 
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple
import pandas as pd
from shared.logger.python_main_logger import logger_manager
from platforms.processing.base_processing_subsystem.subsystem5_and_30_error_event_schema_and_escalate import ErrorEvent, ErrorLevel, ErrorEventLog
from platforms.processing.base_processing_subsystem.subsystem1_data_profiling import DataProfile, DataProfiler

# ---------------------------------------------------------------------------
# Subsystem 4: Data Cleansing Rules
# ---------------------------------------------------------------------------

def load_pre_eval_config() -> Dict[str, Any]:
    config_path = os.path.join(os.path.dirname(__file__), "config", "data_quality_pre_evaluation.yaml")
    if os.path.exists(config_path):
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
                return config.get("data_quality_pre_evaluation", {}) if config else {}
        except Exception:
            pass
    return {}

PRE_EVAL_CONFIG = load_pre_eval_config()
MSG_TEMPLATES = PRE_EVAL_CONFIG.get("msg_templates", {})

# Rule type: a callable that takes a record dict and returns (is_valid, message)
CleansingRule = Callable[[Dict[str, Any]], Tuple[bool, str]]
class CleansingRuleSet:
    """Defines a set of cleansing rules for a specific table."""
    def __init__(self, table_name: str, logger: Optional[logging.Logger] = None, 
                 rules: Optional[List[CleansingRule]] = None):
        
        self.table_name = table_name
        self.rules = rules or []
        self.logger = logger or logger_manager.get_logger(f"{__name__}.{table_name}")
        

    def add_rule(self, rule: CleansingRule):
        self.rules.append(rule)

    def apply(self, record: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Apply all rules to the record. Returns (is_valid, list_of_messages)."""
        is_valid = True
        messages = []
        for rule in self.rules:
            valid, msg = rule(record)
            if not valid:
                is_valid = False
                messages.append(msg)
        return is_valid, messages
    
    @staticmethod
    def rule_not_null(*fields: str) -> CleansingRule:
        """Reject records where any of the specified fields is None/empty."""
        template = MSG_TEMPLATES.get("rule_not_null", "Field '{field}' is null or empty")
        def check(record: Dict[str, Any]) -> Tuple[bool, str]:
            for f in fields:
                val = record.get(f)
                if val is None or (isinstance(val, str) and val.strip() == ""):
                    return False, template.format(field=f)
            return True, ""
        return check

    @staticmethod
    def rule_min_length(field: str, min_len: int) -> CleansingRule:
        template = MSG_TEMPLATES.get("rule_min_length", "Field '{field}' too short (min={min_len})")
        def check(record: Dict[str, Any]) -> Tuple[bool, str]:
            val = record.get(field, "")
            if val and len(str(val)) < min_len:
                return False, template.format(field=field, min_len=min_len)
            return True, ""
        return check

    @staticmethod
    def rule_regex(field: str, pattern: str) -> CleansingRule:
        compiled = re.compile(pattern)
        template = MSG_TEMPLATES.get("rule_regex", "Field '{field}' does not match pattern '{pattern}'")
        def check(record: Dict[str, Any]) -> Tuple[bool, str]:
            val = str(record.get(field, ""))
            if val and not compiled.match(val):
                return False, template.format(field=field, pattern=pattern)
            return True, ""
        return check

    @staticmethod
    def rule_numeric_range(field: str, min_val: float = None, max_val: float = None) -> CleansingRule:
        template_below = MSG_TEMPLATES.get("rule_numeric_range_below", "Field '{field}' = {val} below min {min_val}")
        template_above = MSG_TEMPLATES.get("rule_numeric_range_above", "Field '{field}' = {val} above max {max_val}")
        template_invalid = MSG_TEMPLATES.get("rule_numeric_range_invalid", "Field '{field}' = '{val}' is not numeric")
        def check(record: Dict[str, Any]) -> Tuple[bool, str]:
            val = record.get(field)
            if val is None:
                return True, ""
            try:
                num = float(val)
                if min_val is not None and num < min_val:
                    return False, template_below.format(field=field, val=num, min_val=min_val)
                if max_val is not None and num > max_val:
                    return False, template_above.format(field=field, val=num, max_val=max_val)
            except (ValueError, TypeError):
                return False, template_invalid.format(field=field, val=val)
            return True, ""
        return check

    @staticmethod
    def rule_date_format(field: str, fmt: str = "%d/%m/%Y") -> CleansingRule:
        template = MSG_TEMPLATES.get("rule_date_format", "Field '{field}' = '{val}' does not match date format '{fmt}'")
        def check(record: Dict[str, Any]) -> Tuple[bool, str]:
            val = record.get(field)
            if not val:
                return True, ""
            try:
                datetime.strptime(str(val), fmt)
            except ValueError:
                return False, template.format(field=field, val=val, fmt=fmt)
            return True, ""
        return check

    @staticmethod
    def rule_allowed_values(field: str, allowed: List[Any]) -> CleansingRule:
        allowed_set = set(allowed)
        template = MSG_TEMPLATES.get("rule_allowed_values", "Field '{field}' = '{val}' not in allowed values {allowed}")
        def check(record: Dict[str, Any]) -> Tuple[bool, str]:
            val = record.get(field)
            if val is not None and val not in allowed_set:
                return False, template.format(field=field, val=val, allowed=allowed)
            return True, ""
        return check


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
# Pre-built rule sets for cophieu68 data
# ---------------------------------------------------------------------------

# COPHIEU68_TRADING_DATA_RULES: List[CleansingRule] = [
#     rule_not_null("symbol"),
#     rule_regex("symbol", r"^[A-Z0-9]{2,10}$"),
#     rule_not_null("date"),
#     rule_numeric_range("close_price", min_val=0.0, max_val=1_000_000.0),
#     rule_numeric_range("volume", min_val=0),
#     rule_numeric_range("open_price", min_val=0.0),
#     rule_numeric_range("high_price", min_val=0.0),
#     rule_numeric_range("low_price", min_val=0.0),
# ]

# COPHIEU68_COMPANY_INFO_RULES: List[CleansingRule] = [
#     rule_not_null("symbol"),
#     rule_regex("symbol", r"^[A-Z0-9]{2,10}$"),
#     rule_min_length("company_name", 2),
# ]

# COPHIEU68_FINANCIAL_REPORT_RULES: List[CleansingRule] = [
#     rule_not_null("symbol"),
#     rule_not_null("report_type"),
#     rule_allowed_values("report_type", ["quarter", "year", "QUARTERLY", "ANNUALLY"]),
# ]

# COPHIEU68_INDUSTRY_RULES: List[CleansingRule] = [
#     rule_not_null("industry_code"),
#     rule_not_null("symbol"),
# ]




# ---------------------------------------------------------------------------
# Transformation helpers (field-level cleaning)
# ---------------------------------------------------------------------------

# class FieldTransformer:
#     """
#     Subsystem 4: Field-level transformations applied after validation.
#     Converts raw crawled strings to typed values.
#     """

#     @staticmethod
#     def clean_numeric(value: Any, default: Optional[float] = None) -> Optional[float]:
#         """Remove commas/spaces, convert to float."""
#         if value is None:
#             return default
#         try:
#             cleaned = re.sub(r"[,\s]", "", str(value))
#             return float(cleaned)
#         except (ValueError, TypeError):
#             return default

#     @staticmethod
#     def clean_symbol(value: Any) -> Optional[str]:
#         """Uppercase and strip stock symbol."""
#         if not value:
#             return None
#         return str(value).strip().upper()

#     @staticmethod
#     def clean_date_vn(value: Any) -> Optional[str]:
#         """Convert dd/mm/yyyy → yyyy-mm-dd ISO format."""
#         if not value:
#             return None
#         try:
#             dt = datetime.strptime(str(value).strip(), "%d/%m/%Y")
#             return dt.strftime("%Y-%m-%d")
#         except ValueError:
#             return str(value)

#     @staticmethod
#     def clean_percentage(value: Any) -> Optional[float]:
#         """Convert '12.5%' → 0.125."""
#         if value is None:
#             return None
#         try:
#             cleaned = str(value).replace("%", "").strip()
#             return float(cleaned) / 100.0
#         except (ValueError, TypeError):
#             return None

#     @staticmethod
#     def normalize_report_type(value: Any) -> Optional[str]:
#         """Normalize report_type to ANNUALLY|QUARTERLY."""
#         if not value:
#             return None
#         mapping = {
#             "year": "ANNUALLY", "annual": "ANNUALLY", "annually": "ANNUALLY",
#             "quarter": "QUARTERLY", "quarterly": "QUARTERLY", "q": "QUARTERLY",
#         }
#         return mapping.get(str(value).lower().strip(), str(value).upper())

#     @staticmethod
#     def transform_trading_record(record: Dict[str, Any]) -> Dict[str, Any]:
#         """Apply all field transformations to a raw trading data record."""
#         return {
#             "symbol":        FieldTransformer.clean_symbol(record.get("symbol")),
#             "date":          FieldTransformer.clean_date_vn(record.get("date") or record.get("trade_date")),
#             "close_price":   FieldTransformer.clean_numeric(record.get("close_price") or record.get("close")),
#             "open_price":    FieldTransformer.clean_numeric(record.get("open_price") or record.get("open")),
#             "high_price":    FieldTransformer.clean_numeric(record.get("high_price") or record.get("high")),
#             "low_price":     FieldTransformer.clean_numeric(record.get("low_price") or record.get("low")),
#             "volume":        FieldTransformer.clean_numeric(record.get("volume")),
#             "foreign_buy":   FieldTransformer.clean_numeric(record.get("foreign_buy")),
#             "foreign_sell":  FieldTransformer.clean_numeric(record.get("foreign_sell")),
#             "foreign_value": FieldTransformer.clean_numeric(record.get("foreign_value") or record.get("foreign_net_value")),
#         }

#     @staticmethod
#     def transform_financial_record(record: Dict[str, Any]) -> Dict[str, Any]:
#         """Apply field transformations to a raw financial report record."""
#         return {
#             "symbol":               FieldTransformer.clean_symbol(record.get("symbol")),
#             "report_type":          FieldTransformer.normalize_report_type(record.get("report_type")),
#             "year":                 str(record.get("year", "")).strip() or None,
#             "period":               str(record.get("period", "")).strip() or None,
#             "metric_code":          str(record.get("metric_code", "")).strip() or None,
#             "metric_value":         FieldTransformer.clean_numeric(record.get("metric_value")),
#             "metric_name_en":       record.get("metric_name_en"),
#             "metric_group":         record.get("metric_group"),
#         }