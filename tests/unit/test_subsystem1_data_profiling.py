"""
Unit tests – Subsystem 1: Data Profiling
"""
from __future__ import annotations

import pytest
import pandas as pd
from datetime import datetime, timezone

from platforms.processing.base_processing_subsystem.subsystem1_data_profiling import (
    ColumnProfile,
    DataProfile,
    DataProfiler,
    DataQualityRecord,
)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

BASE_CONFIG = {
    "sample_size": 3,
    "null_pct_precision": 4,
    "null_pct_fail_threshold": 0.5,
    "null_pct_warn_threshold": 0.1,
    "constant_column_unique_count": 1,
    "constant_column_min_total_rows": 2,
    "high_null_msg_template": "HIGH NULL RATE: {column_name} = {null_pct:.1%}",
    "const_col_msg_template": "CONSTANT COLUMN: {column_name}",
    "hash_id_length": 16,
    "hash_check_id": True,
    "quality_record_schema": {
        "check_id": "{table_name}|{column_name}|null_check",
        "run_id": "run_id",
        "table_name": "table_name",
        "check_name": "{column_name}_null_check",
        "status": "status",
        "failed_rows": "null_count",
        "total_rows": "total_count",
    },
}

SAMPLE_RECORDS = [
    {"symbol": "FPT", "close_price": 115.5, "volume": 2_500_000},
    {"symbol": "VNM", "close_price": 68.2, "volume": 3_100_000},
    {"symbol": "HPG", "close_price": None, "volume": 15_000_000},
    {"symbol": "VCB", "close_price": 92.0, "volume": None},
]


# ---------------------------------------------------------------------------
# DataProfiler.profile
# ---------------------------------------------------------------------------

class TestDataProfilerProfile:
    def test_returns_dataprofile_instance(self):
        result = DataProfiler.profile(BASE_CONFIG, SAMPLE_RECORDS, table_name="test")
        assert isinstance(result, DataProfile)

    def test_total_rows(self):
        result = DataProfiler.profile(BASE_CONFIG, SAMPLE_RECORDS, table_name="test")
        assert result.total_rows == 4

    def test_column_count(self):
        result = DataProfiler.profile(BASE_CONFIG, SAMPLE_RECORDS, table_name="test")
        assert len(result.columns) == 3  # symbol, close_price, volume

    def test_null_counts_per_column(self):
        result = DataProfiler.profile(BASE_CONFIG, SAMPLE_RECORDS, table_name="test")
        col_map = {c.column_name: c for c in result.columns}

        assert col_map["symbol"].null_count == 0
        assert col_map["close_price"].null_count == 1
        assert col_map["volume"].null_count == 1

    def test_null_pct_calculation(self):
        result = DataProfiler.profile(BASE_CONFIG, SAMPLE_RECORDS, table_name="test")
        col_map = {c.column_name: c for c in result.columns}
        assert col_map["close_price"].null_pct == pytest.approx(0.25, abs=1e-4)

    def test_empty_records_returns_empty_profile(self):
        result = DataProfiler.profile(BASE_CONFIG, [], table_name="empty_table")
        assert result.total_rows == 0
        assert result.columns == []
        assert result.issues == []

    def test_issues_flagged_for_high_null_rate(self):
        # Force > 50% nulls on one column
        records = [
            {"x": 1, "y": None},
            {"x": 2, "y": None},
            {"x": 3, "y": None},
        ]
        result = DataProfiler.profile(BASE_CONFIG, records, table_name="t")
        assert any("HIGH NULL RATE" in i for i in result.issues)

    def test_constant_column_flagged(self):
        records = [
            {"symbol": "FPT", "const": "same"},
            {"symbol": "VNM", "const": "same"},
            {"symbol": "HPG", "const": "same"},
        ]
        result = DataProfiler.profile(BASE_CONFIG, records, table_name="t")
        assert any("CONSTANT COLUMN" in i for i in result.issues)

    def test_run_id_propagated(self):
        result = DataProfiler.profile(BASE_CONFIG, SAMPLE_RECORDS, table_name="t", run_id="rid_123")
        assert result.run_id == "rid_123"


# ---------------------------------------------------------------------------
# DataProfile.to_quality_records
# ---------------------------------------------------------------------------

class TestDataProfileToQualityRecords:
    def test_returns_list_of_dicts(self):
        profile = DataProfiler.profile(BASE_CONFIG, SAMPLE_RECORDS, table_name="stock_prices")
        records = profile.to_quality_records(BASE_CONFIG)
        assert isinstance(records, list)
        assert len(records) == 3  # one per column

    def test_status_pass_for_zero_nulls(self):
        records = [{"symbol": "FPT"}, {"symbol": "VNM"}]
        profile = DataProfiler.profile(BASE_CONFIG, records, table_name="t")
        qr = profile.to_quality_records(BASE_CONFIG)
        symbol_rec = next(r for r in qr if "symbol" in r.get("table_name", "") or True)
        # all should be PASS since null_pct == 0
        assert all(r["status"] == "PASS" for r in qr)

    def test_check_id_is_hashed_hex(self):
        profile = DataProfiler.profile(BASE_CONFIG, SAMPLE_RECORDS, table_name="t")
        qr = profile.to_quality_records(BASE_CONFIG)
        for r in qr:
            # sha256 truncated to 16 hex chars
            assert len(r["check_id"]) == 16
            assert all(c in "0123456789abcdef" for c in r["check_id"])


# ---------------------------------------------------------------------------
# DataProfiler.profile_dataframe
# ---------------------------------------------------------------------------

class TestDataProfilerProfileDataframe:
    def test_accepts_dataframe(self):
        df = pd.DataFrame(SAMPLE_RECORDS)
        result = DataProfiler.profile_dataframe(BASE_CONFIG, df, table_name="df_test")
        assert result.total_rows == 4

    def test_nan_treated_as_null(self):
        df = pd.DataFrame({"a": [1.0, float("nan"), 3.0]})
        result = DataProfiler.profile_dataframe(BASE_CONFIG, df, table_name="t")
        assert result.columns[0].null_count == 1


# ---------------------------------------------------------------------------
# DataQualityRecord
# ---------------------------------------------------------------------------

class TestDataQualityRecord:
    def test_to_dict_serialises_datetime(self):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        rec = DataQualityRecord(
            check_id="abc123",
            run_id="run_01",
            table_name="stock_prices",
            check_name="null_check",
            status="PASS",
            failed_rows=0,
            total_rows=100,
            checked_at=now,
        )
        d = rec.to_dict()
        assert d["checked_at"] == now.isoformat()
        assert d["status"] == "PASS"
