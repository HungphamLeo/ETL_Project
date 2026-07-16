"""
Unit tests – Subsystem 4: DataCleansingEngine & CleansingRuleSet
"""
from __future__ import annotations

import pytest
import pandas as pd

from platforms.processing.base_processing_subsystem.subsystem4_data_quality_pre_evaluate import (
    CleansingResult,
    CleansingRuleSet,
    DataCleansingEngine,
)
from platforms.processing.base_processing_subsystem.subsystem5_and_30_error_event_schema_and_escalate import (
    ErrorLevel,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

VALID_RECORDS = [
    {"symbol": "FPT",  "close_price": 115.5, "volume": 2_500_000},
    {"symbol": "VNM",  "close_price": 68.2,  "volume": 3_100_000},
    {"symbol": "HPG",  "close_price": 29.8,  "volume": 15_000_000},
]

INVALID_RECORDS = [
    {"symbol": "",     "close_price": 115.5, "volume": 2_500_000},  # null symbol
    {"symbol": "VNM",  "close_price": -5.0,  "volume": 3_100_000},  # negative price
    {"symbol": "HPG",  "close_price": 29.8,  "volume": 15_000_000},  # valid
]


# ---------------------------------------------------------------------------
# CleansingRuleSet – individual rules
# ---------------------------------------------------------------------------

class TestCleansingRuleNotNull:
    def test_passes_on_non_null_field(self):
        rule = CleansingRuleSet.rule_not_null("symbol")
        valid, msg = rule({"symbol": "FPT"})
        assert valid is True
        assert msg == ""

    def test_fails_on_none(self):
        rule = CleansingRuleSet.rule_not_null("symbol")
        valid, msg = rule({"symbol": None})
        assert valid is False
        assert "symbol" in msg

    def test_fails_on_empty_string(self):
        rule = CleansingRuleSet.rule_not_null("symbol")
        valid, msg = rule({"symbol": "   "})
        assert valid is False

    def test_checks_multiple_fields(self):
        rule = CleansingRuleSet.rule_not_null("symbol", "date")
        valid, _ = rule({"symbol": "FPT", "date": None})
        assert valid is False


class TestCleansingRuleNumericRange:
    def test_passes_in_range(self):
        rule = CleansingRuleSet.rule_numeric_range("price", min_val=0.0, max_val=1_000_000.0)
        valid, _ = rule({"price": 500.0})
        assert valid is True

    def test_fails_below_min(self):
        rule = CleansingRuleSet.rule_numeric_range("price", min_val=0.0)
        valid, msg = rule({"price": -1.0})
        assert valid is False
        assert "price" in msg

    def test_fails_above_max(self):
        rule = CleansingRuleSet.rule_numeric_range("price", max_val=100.0)
        valid, msg = rule({"price": 200.0})
        assert valid is False

    def test_skips_when_field_is_none(self):
        rule = CleansingRuleSet.rule_numeric_range("price", min_val=0.0)
        valid, _ = rule({"price": None})
        assert valid is True

    def test_fails_on_non_numeric(self):
        rule = CleansingRuleSet.rule_numeric_range("price", min_val=0.0)
        valid, msg = rule({"price": "abc"})
        assert valid is False


class TestCleansingRuleRegex:
    def test_passes_matching_pattern(self):
        rule = CleansingRuleSet.rule_regex("symbol", r"^[A-Z0-9]{2,10}$")
        valid, _ = rule({"symbol": "FPT"})
        assert valid is True

    def test_fails_non_matching(self):
        rule = CleansingRuleSet.rule_regex("symbol", r"^[A-Z0-9]{2,10}$")
        valid, msg = rule({"symbol": "fpt lower"})
        assert valid is False

    def test_skips_empty_value(self):
        rule = CleansingRuleSet.rule_regex("symbol", r"^[A-Z0-9]+$")
        valid, _ = rule({"symbol": ""})
        assert valid is True


class TestCleansingRuleMinLength:
    def test_passes_sufficient_length(self):
        rule = CleansingRuleSet.rule_min_length("name", 3)
        valid, _ = rule({"name": "FPT"})
        assert valid is True

    def test_fails_too_short(self):
        rule = CleansingRuleSet.rule_min_length("name", 5)
        valid, msg = rule({"name": "ab"})
        assert valid is False


class TestCleansingRuleDateFormat:
    def test_passes_valid_date(self):
        rule = CleansingRuleSet.rule_date_format("date", fmt="%d/%m/%Y")
        valid, _ = rule({"date": "15/01/2024"})
        assert valid is True

    def test_fails_wrong_format(self):
        rule = CleansingRuleSet.rule_date_format("date", fmt="%d/%m/%Y")
        valid, _ = rule({"date": "2024-01-15"})
        assert valid is False

    def test_skips_empty_date(self):
        rule = CleansingRuleSet.rule_date_format("date")
        valid, _ = rule({"date": None})
        assert valid is True


class TestCleansingRuleAllowedValues:
    def test_passes_allowed_value(self):
        rule = CleansingRuleSet.rule_allowed_values("type", ["quarter", "year"])
        valid, _ = rule({"type": "quarter"})
        assert valid is True

    def test_fails_not_in_allowed(self):
        rule = CleansingRuleSet.rule_allowed_values("type", ["quarter", "year"])
        valid, msg = rule({"type": "monthly"})
        assert valid is False

    def test_skips_none_value(self):
        rule = CleansingRuleSet.rule_allowed_values("type", ["quarter"])
        valid, _ = rule({"type": None})
        assert valid is True


# ---------------------------------------------------------------------------
# DataCleansingEngine.cleanse
# ---------------------------------------------------------------------------

class TestDataCleansingEngineCleanse:
    def _build_engine(self, run_profiling=False):
        engine = DataCleansingEngine(run_profiling=run_profiling)
        engine.add_rule(CleansingRuleSet.rule_not_null("symbol"))
        engine.add_rule(CleansingRuleSet.rule_numeric_range("close_price", min_val=0.0))
        return engine

    def test_all_valid_records_pass(self):
        engine = self._build_engine()
        result = engine.cleanse(VALID_RECORDS)
        assert isinstance(result, CleansingResult)
        assert result.total_cleaned == 3
        assert result.total_rejected == 0

    def test_invalid_records_not_rejected_dq_non_blocking(self):
        # DQ is now non-blocking: all records pass through, violations are logged
        engine = self._build_engine()
        result = engine.cleanse(INVALID_RECORDS)
        assert result.total_rejected == 0
        assert result.total_cleaned == 3  # all records pass through
        # Violations are captured as error_events (WARNING level) and dq_observations
        assert len(result.error_events) >= 2

    def test_empty_input_returns_empty_result(self):
        engine = self._build_engine()
        result = engine.cleanse([])
        assert result.total_input == 0
        assert result.total_cleaned == 0
        assert result.total_rejected == 0
        assert result.error_events == []

    def test_error_events_created_for_dq_violations(self):
        # DQ violations are now WARNING-level error events (not hard rejects)
        engine = self._build_engine()
        result = engine.cleanse(INVALID_RECORDS)
        assert len(result.error_events) >= 2

    def test_summary_rejection_rate_is_zero_non_blocking(self):
        # Non-blocking mode: rejection_rate is always 0; DQ failures go to dq_observations
        engine = self._build_engine()
        result = engine.cleanse(INVALID_RECORDS)
        summary = result.summary()
        assert summary["total_input"] == 3
        assert summary["total_rejected"] == 0
        assert summary["rejection_rate"] == 0
        # DQ failures are exposed through dq_failures in summary
        assert "dq_failures" in summary
        assert "dq_observations" in summary

    def test_no_rules_accepts_all(self):
        engine = DataCleansingEngine(run_profiling=False)
        result = engine.cleanse(INVALID_RECORDS)
        assert result.total_cleaned == 3
        assert result.total_rejected == 0

    def test_run_profiling_populates_profile(self):
        engine = DataCleansingEngine(run_profiling=True)
        engine.add_rule(CleansingRuleSet.rule_not_null("symbol"))
        result = engine.cleanse(VALID_RECORDS, source="test", run_id="r1")
        assert result.profile is not None

    def test_run_profiling_false_leaves_profile_none(self):
        engine = self._build_engine(run_profiling=False)
        result = engine.cleanse(VALID_RECORDS)
        assert result.profile is None


# ---------------------------------------------------------------------------
# DataCleansingEngine.cleanse_dataframe
# ---------------------------------------------------------------------------

class TestDataCleansingEngineDataFrame:
    def test_cleanse_dataframe_returns_dataframe(self):
        engine = DataCleansingEngine(run_profiling=False)
        engine.add_rule(CleansingRuleSet.rule_not_null("symbol"))
        df = pd.DataFrame(VALID_RECORDS)
        cleaned_df, result = engine.cleanse_dataframe(df, source="test")
        assert isinstance(cleaned_df, pd.DataFrame)
        assert len(cleaned_df) == 3

    def test_cleanse_dataframe_non_blocking_passes_all(self):
        # Non-blocking: dataframe always returns all rows, violations logged only
        engine = DataCleansingEngine(run_profiling=False)
        engine.add_rule(CleansingRuleSet.rule_numeric_range("close_price", min_val=0.0))
        df = pd.DataFrame(INVALID_RECORDS)
        cleaned_df, result = engine.cleanse_dataframe(df)
        assert result.total_rejected == 0
        assert len(cleaned_df) == len(df)  # full dataframe returned
        # Violation captured in error_events
        assert len(result.error_events) >= 1
