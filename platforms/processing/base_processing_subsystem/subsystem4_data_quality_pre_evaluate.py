from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import warnings
import yaml
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple
import pandas as pd

from shared.logger.python_main_logger import logger_manager
from platforms.processing.base_processing_subsystem.subsystem5_and_30_error_event_schema_and_escalate import ErrorEvent, ErrorLevel, ErrorEventLog
from platforms.processing.base_processing_subsystem.subsystem1_data_profiling import DataProfile, DataProfiler

# Suppress GE noise
warnings.filterwarnings("ignore", category=UserWarning, module="great_expectations")

# ---------------------------------------------------------------------------
# Subsystem 4: Data Quality Pre-Evaluation
#
# Strategy (non-blocking DQ):
#   • All rules are evaluated and their results are LOGGED via Great Expectations.
#   • Records are NEVER rejected — the pipeline always continues regardless of
#     DQ failures.  This allows the full ETL flow to run and data to land in the
#     database first; DQ reports are built separately (e.g. Grafana dashboards).
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
    """
    Defines a set of cleansing rules for a specific table.

    Rules are evaluated and their outcomes are LOGGED.
    They are no longer used as hard gates — records always pass through.
    """
    def __init__(self, table_name: str, logger: Optional[logging.Logger] = None,
                 rules: Optional[List[CleansingRule]] = None):
        self.table_name = table_name
        self.rules = rules or []
        self.logger = logger or logger_manager.get_logger(f"{__name__}.{table_name}")

    def add_rule(self, rule: CleansingRule):
        self.rules.append(rule)

    def apply(self, record: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Apply all rules to the record. Returns (is_valid, list_of_messages).
        NOTE: Even if is_valid=False, callers should NOT reject the record.
        The result is used for observation / logging only.
        """
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
        """Observe records where any of the specified fields is None/empty."""
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
        template_below   = MSG_TEMPLATES.get("rule_numeric_range_below",   "Field '{field}' = {val} below min {min_val}")
        template_above   = MSG_TEMPLATES.get("rule_numeric_range_above",   "Field '{field}' = {val} above max {max_val}")
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
    cleaned:        List[Dict[str, Any]]
    rejected:       List[Dict[str, Any]]
    error_events:   List[ErrorEvent]
    total_input:    int
    total_cleaned:  int
    total_rejected: int
    profile:        Optional[DataProfile] = None
    # GE-level DQ observations (rule violations that were logged but not rejected)
    dq_observations: List[Dict[str, Any]] = None

    def __post_init__(self):
        if self.dq_observations is None:
            self.dq_observations = []

    def summary(self) -> Dict[str, Any]:
        return {
            "total_input":       self.total_input,
            "total_cleaned":     self.total_cleaned,
            "total_rejected":    self.total_rejected,
            "rejection_rate":    round(self.total_rejected / self.total_input, 4) if self.total_input else 0,
            "dq_observations":   len(self.dq_observations),
            "dq_failures":       sum(1 for o in self.dq_observations if not o.get("passed", True)),
            "error_counts":      {
                level.value: sum(1 for e in self.error_events if e.error_level == level)
                for level in ErrorLevel
            },
        }


# ---------------------------------------------------------------------------
# GE helper – evaluate CleansingRules as GE expectations for logging
# ---------------------------------------------------------------------------

def _run_ge_rule_checks(
    df: pd.DataFrame,
    rules: List[CleansingRule],
    source: str,
    run_id: str,
    logger: logging.Logger,
) -> List[Dict[str, Any]]:
    """
    Translates CleansingRules to Great Expectations checks on *df*, runs them,
    and returns structured observation records for reporting.
    Never raises; never modifies the dataframe.
    """
    observations: List[Dict[str, Any]] = []
    checked_at = datetime.now(timezone.utc).isoformat()

    try:
        import great_expectations as gx
        import logging as _logging
        _logging.getLogger("great_expectations").setLevel(_logging.ERROR)

        context = gx.get_context(mode="ephemeral")
        ds = context.data_sources.add_pandas(name=f"ds_{source}_{run_id}")
        da = ds.add_dataframe_asset(name="asset")
        batch_def = da.add_batch_definition_whole_dataframe("batch")
        batch = batch_def.get_batch(batch_parameters={"dataframe": df})

        # Inspect each rule's closure to derive the right GE expectation
        for rule in rules:
            try:
                obs = _rule_to_ge_observation(rule, batch, df, source, run_id, checked_at, logger)
                if obs:
                    observations.append(obs)
            except Exception as exc:  # noqa: BLE001
                logger.debug("[GE|%s] rule inspection failed: %s", source, exc)

        pass_count = sum(1 for o in observations if o.get("passed"))
        fail_count = len(observations) - pass_count
        logger.info(
            "[GE|%s] Rule checks complete — %d rules, %d PASS, %d FAIL (pipeline continues)",
            source, len(observations), pass_count, fail_count,
        )

    except Exception as exc:  # noqa: BLE001
        logger.warning("[GE] Rule checks skipped for %s: %s", source, exc)

    return observations


def _rule_to_ge_observation(
    rule: CleansingRule,
    batch: Any,
    df: pd.DataFrame,
    source: str,
    run_id: str,
    checked_at: str,
    logger: logging.Logger,
) -> Optional[Dict[str, Any]]:
    """
    Introspects the rule's closure to map it to a GE expectation.
    Falls back to running the rule record-by-record if introspection fails.
    """
    import great_expectations as gx

    closure = getattr(rule, "__closure__", None) or []
    cell_contents = [c.cell_contents for c in closure if c.cell_contents is not None]

    def _try_validate(exp) -> Optional[Dict]:
        try:
            vr = batch.validate(exp)
            result = vr.result or {}
            passed = bool(vr.success)
            uc = result.get("unexpected_count", 0)
            ec = result.get("element_count", len(df))
            return {
                "source": source, "run_id": run_id,
                "expectation": type(exp).__name__,
                "column": exp.column if hasattr(exp, "column") else None,
                "passed": passed,
                "unexpected_count": uc,
                "element_count": ec,
                "checked_at": checked_at,
            }
        except Exception:
            return None

    # ── Map rule_not_null ────────────────────────────────────────────────────
    func_name = getattr(rule, "__name__", "") or getattr(getattr(rule, "__func__", None), "__name__", "")
    # Check by inspecting cell contents for known patterns
    # cell_contents for rule_not_null: (*fields,)
    # cell_contents for rule_regex:    (compiled, template)
    # cell_contents for rule_numeric_range: (min_val, max_val, ...)

    # Try rule_not_null: fields are strings
    field_strings = [c for c in cell_contents if isinstance(c, str) and not c.startswith("Field")]
    regex_objects = [c for c in cell_contents if hasattr(c, "pattern")]
    numeric_vals  = [c for c in cell_contents if isinstance(c, (int, float)) and not isinstance(c, bool)]

    if regex_objects and field_strings:
        # rule_regex
        col = field_strings[0]
        if col in df.columns:
            obs = _try_validate(gx.expectations.ExpectColumnValuesToMatchRegex(
                column=col, regex=regex_objects[0].pattern))
            if obs:
                obs["rule_type"] = "rule_regex"
                if not obs["passed"]:
                    logger.warning("[GE|%s] FAIL rule_regex(%s) unexpected=%s/%s",
                                   source, col, obs["unexpected_count"], obs["element_count"])
                return obs

    elif field_strings and numeric_vals:
        # rule_numeric_range or rule_min_length
        col = field_strings[0]
        if col in df.columns:
            if len(numeric_vals) >= 2:
                # numeric_range: min_val, max_val in closure
                min_v = min(numeric_vals)
                max_v = max(numeric_vals)
                obs = _try_validate(gx.expectations.ExpectColumnValuesToBeBetween(
                    column=col, min_value=min_v, max_value=max_v))
                if obs:
                    obs["rule_type"] = "rule_numeric_range"
                    if not obs["passed"]:
                        logger.warning("[GE|%s] FAIL rule_numeric_range(%s) min=%s max=%s unexpected=%s/%s",
                                       source, col, min_v, max_v, obs["unexpected_count"], obs["element_count"])
                    return obs
            elif len(numeric_vals) == 1:
                # min_length: check string length
                obs = _try_validate(gx.expectations.ExpectColumnValueLengthsToBeBetween(
                    column=col, min_value=int(numeric_vals[0])))
                if obs:
                    obs["rule_type"] = "rule_min_length"
                    if not obs["passed"]:
                        logger.warning("[GE|%s] FAIL rule_min_length(%s) min_len=%s unexpected=%s/%s",
                                       source, col, numeric_vals[0], obs["unexpected_count"], obs["element_count"])
                    return obs

    elif field_strings:
        # rule_not_null: one or more fields
        col = field_strings[0]
        if col in df.columns:
            obs = _try_validate(gx.expectations.ExpectColumnValuesToNotBeNull(column=col))
            if obs:
                obs["rule_type"] = "rule_not_null"
                if not obs["passed"]:
                    logger.warning("[GE|%s] FAIL rule_not_null(%s) unexpected=%s/%s",
                                   source, col, obs["unexpected_count"], obs["element_count"])
                return obs

    # Fallback: run rule record-by-record and report aggregate
    violations = 0
    for rec in df.to_dict(orient="records"):
        try:
            valid, _ = rule(rec)
            if not valid:
                violations += 1
        except Exception:
            pass
    total = len(df)
    passed = violations == 0
    obs = {
        "source": source, "run_id": run_id,
        "expectation": "custom_rule",
        "column": None,
        "passed": passed,
        "unexpected_count": violations,
        "element_count": total,
        "checked_at": checked_at,
        "rule_type": "custom",
    }
    if not passed:
        logger.warning("[GE|%s] FAIL custom_rule unexpected=%s/%s", source, violations, total)
    return obs


class DataCleansingEngine:
    """
    Subsystem 4: Data Quality Engine (non-blocking mode).

    Evaluates CleansingRules against records and logs all outcomes as
    Great Expectations observations.  Records are NEVER rejected —
    all input records are returned in `cleaned` regardless of DQ result.
    This allows the full ETL pipeline to complete while DQ results are
    persisted for later reporting (Grafana / delta meta tables).
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
        Evaluate all rules against the records and log DQ observations.

        ALL records are returned in `cleaned`; `rejected` is always empty.
        DQ violations are observable via CleansingResult.dq_observations
        and CleansingResult.error_events (logged at WARNING level only).

        Args:
            records:          Input records (list of dicts)
            source:           Source identifier (e.g. "cophieu68.trading_data")
            run_id:           Pipeline run ID for lineage
            record_id_fields: Fields to use as record identifier in error events
        """
        error_log = ErrorEventLog(run_id=run_id, job_name=source, logger=self.logger)

        # ── Subsystem 1: Profile before evaluation ───────────────────────────
        profile = None
        if self.run_profiling and records:
            profile = DataProfiler.profile({}, records, table_name=source, run_id=run_id,
                                           logger=self.logger)
            if profile.issues:
                for issue in profile.issues:
                    error_log.add(ErrorLevel.WARNING, f"[PROFILING] {issue}")

        # ── Evaluate rules for observation (no rejection) ────────────────────
        for record in records:
            for rule in self.rules:
                try:
                    is_valid, message = rule(record)
                    if not is_valid:
                        # Log as WARNING only — do not reject
                        error_log.add(
                            ErrorLevel.WARNING,
                            f"[DQ_OBS] {message}",
                            record=record,
                            record_id_fields=record_id_fields,
                        )
                except Exception as exc:
                    error_log.add(
                        ErrorLevel.WARNING,
                        f"[DQ_OBS] Rule evaluation error: {exc}",
                        record=record,
                        record_id_fields=record_id_fields,
                    )

        # ── GE-level batch observations ──────────────────────────────────────
        dq_observations: List[Dict[str, Any]] = []
        if self.rules and records:
            try:
                df = pd.DataFrame(records)
                dq_observations = _run_ge_rule_checks(
                    df, self.rules, source, run_id, self.logger)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("[GE] batch rule checks failed for %s: %s", source, exc)

        # All records pass through — no rejection
        return CleansingResult(
            cleaned         = list(records),
            rejected        = [],
            error_events    = error_log.events,
            total_input     = len(records),
            total_cleaned   = len(records),
            total_rejected  = 0,
            profile         = profile,
            dq_observations = dq_observations,
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
        # Always return the full original dataframe (no records were dropped)
        return df.copy(), result
