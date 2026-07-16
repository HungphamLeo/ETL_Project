"""
Unit tests – Subsystem 9: SCDManager (SCD1, SCD2, SCD3)
"""
from __future__ import annotations

import pytest
import pandas as pd
from datetime import date

from platforms.processing.base_processing_subsystem.subsystem9_and_25_scd_manage_and_version import (
    SCD1Result,
    SCD2Result,
    SCDManager,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_manager():
    return SCDManager(
        natural_keys=["symbol"],
        tracked_cols=["company_name", "sector"],
        effective_date_col="effective_date",
        end_date_col="end_date",
        is_current_col="is_current",
    )


EXISTING_DF = pd.DataFrame([
    {"symbol": "FPT", "company_name": "FPT Corp", "sector": "Technology",
     "effective_date": date(2023, 1, 1), "end_date": None, "is_current": True},
    {"symbol": "VNM", "company_name": "Vinamilk", "sector": "Food",
     "effective_date": date(2023, 1, 1), "end_date": None, "is_current": True},
])

INCOMING_CHANGED = pd.DataFrame([
    {"symbol": "FPT",  "company_name": "FPT Corporation", "sector": "IT"},  # changed
    {"symbol": "VNM",  "company_name": "Vinamilk",        "sector": "Food"},  # unchanged
    {"symbol": "HPG",  "company_name": "Hoa Phat Group",  "sector": "Steel"},  # new
])

INCOMING_NO_CHANGE = pd.DataFrame([
    {"symbol": "FPT", "company_name": "FPT Corp", "sector": "Technology"},
    {"symbol": "VNM", "company_name": "Vinamilk", "sector": "Food"},
])


# ---------------------------------------------------------------------------
# compute_row_hash
# ---------------------------------------------------------------------------

class TestComputeRowHash:
    def test_same_inputs_same_hash(self):
        mgr = _make_manager()
        record = {"symbol": "FPT", "company_name": "FPT Corp", "sector": "Technology"}
        h1 = mgr.compute_row_hash(record)
        h2 = mgr.compute_row_hash(record)
        assert h1 == h2

    def test_different_inputs_different_hash(self):
        mgr = _make_manager()
        r1 = {"symbol": "FPT", "company_name": "FPT Corp",        "sector": "Technology"}
        r2 = {"symbol": "FPT", "company_name": "FPT Corporation", "sector": "IT"}
        assert mgr.compute_row_hash(r1) != mgr.compute_row_hash(r2)

    def test_hash_is_64_hex_chars(self):
        mgr = _make_manager()
        h = mgr.compute_row_hash({"company_name": "X", "sector": "Y"})
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)


# ---------------------------------------------------------------------------
# SCD Type 1
# ---------------------------------------------------------------------------

class TestSCDManagerSCD1:
    def test_first_load_all_go_to_insert(self):
        mgr = _make_manager()
        result = mgr.apply_scd1(pd.DataFrame(), INCOMING_CHANGED)
        assert isinstance(result, SCD1Result)
        assert len(result.to_insert) == 3
        assert len(result.to_update) == 0

    def test_changed_record_goes_to_update(self):
        mgr = _make_manager()
        result = mgr.apply_scd1(EXISTING_DF, INCOMING_CHANGED)
        # FPT changed, so it goes to to_update
        assert any(row["symbol"] == "FPT" for _, row in result.to_update.iterrows())

    def test_new_record_goes_to_insert(self):
        mgr = _make_manager()
        result = mgr.apply_scd1(EXISTING_DF, INCOMING_CHANGED)
        assert any(row["symbol"] == "HPG" for _, row in result.to_insert.iterrows())

    def test_unchanged_record_goes_to_unchanged(self):
        mgr = _make_manager()
        result = mgr.apply_scd1(EXISTING_DF, INCOMING_CHANGED)
        assert any(row["symbol"] == "VNM" for _, row in result.unchanged.iterrows())

    def test_no_changes_all_unchanged(self):
        mgr = _make_manager()
        result = mgr.apply_scd1(EXISTING_DF, INCOMING_NO_CHANGE)
        assert len(result.to_update) == 0
        assert len(result.to_insert) == 0
        assert len(result.unchanged) == 2

    def test_update_time_set_on_inserts(self):
        mgr = _make_manager()
        result = mgr.apply_scd1(pd.DataFrame(), INCOMING_CHANGED)
        assert "update_time" in result.to_insert.columns

    def test_stats_populated(self):
        mgr = _make_manager()
        result = mgr.apply_scd1(EXISTING_DF, INCOMING_CHANGED)
        assert len(result.stats) > 0

    def test_summary_string_returned(self):
        mgr = _make_manager()
        result = mgr.apply_scd1(EXISTING_DF, INCOMING_CHANGED)
        summary = result.summary()
        assert isinstance(summary, str)


# ---------------------------------------------------------------------------
# SCD Type 2
# ---------------------------------------------------------------------------

class TestSCDManagerSCD2:
    def test_initial_load_inserts_all(self):
        mgr = _make_manager()
        result = mgr.apply_scd2(pd.DataFrame(), INCOMING_CHANGED)
        assert isinstance(result, SCD2Result)
        assert len(result.to_insert) == 3
        assert len(result.to_close) == 0

    def test_changed_record_closes_old_and_inserts_new(self):
        mgr = _make_manager()
        result = mgr.apply_scd2(EXISTING_DF, INCOMING_CHANGED)
        # FPT changed → should be in to_close and to_insert
        assert any(row["symbol"] == "FPT" for _, row in result.to_close.iterrows())
        assert any(row["symbol"] == "FPT" for _, row in result.to_insert.iterrows())

    def test_new_record_goes_to_insert(self):
        mgr = _make_manager()
        result = mgr.apply_scd2(EXISTING_DF, INCOMING_CHANGED)
        assert any(row["symbol"] == "HPG" for _, row in result.to_insert.iterrows())

    def test_unchanged_record_goes_to_unchanged(self):
        mgr = _make_manager()
        result = mgr.apply_scd2(EXISTING_DF, INCOMING_CHANGED)
        assert any(row["symbol"] == "VNM" for _, row in result.unchanged.iterrows())

    def test_no_changes_produces_no_closes_or_inserts(self):
        mgr = _make_manager()
        result = mgr.apply_scd2(EXISTING_DF, INCOMING_NO_CHANGE)
        assert len(result.to_close) == 0
        assert len(result.to_insert) == 0

    def test_closed_record_has_end_date_set(self):
        mgr = _make_manager()
        result = mgr.apply_scd2(EXISTING_DF, INCOMING_CHANGED, as_of=date(2024, 6, 1))
        if len(result.to_close) > 0:
            assert result.to_close.iloc[0]["end_date"] == date(2024, 6, 1)
            assert result.to_close.iloc[0]["is_current"] == False  # noqa: E712 – np.False_ equality

    def test_new_insert_has_is_current_true(self):
        mgr = _make_manager()
        result = mgr.apply_scd2(pd.DataFrame(), INCOMING_CHANGED)
        assert all(result.to_insert["is_current"] == True)

    def test_has_changes_true_when_changes(self):
        mgr = _make_manager()
        result = mgr.apply_scd2(EXISTING_DF, INCOMING_CHANGED)
        assert result.has_changes() is True

    def test_has_changes_false_when_no_changes(self):
        mgr = _make_manager()
        result = mgr.apply_scd2(EXISTING_DF, INCOMING_NO_CHANGE)
        assert result.has_changes() is False

    def test_summary_string(self):
        mgr = _make_manager()
        result = mgr.apply_scd2(EXISTING_DF, INCOMING_CHANGED)
        summary = result.summary()
        assert isinstance(summary, str)


# ---------------------------------------------------------------------------
# SCD Type 3
# ---------------------------------------------------------------------------

class TestSCDManagerSCD3:
    def test_initial_load_adds_prev_col_with_none(self):
        mgr = _make_manager()
        result = mgr.apply_scd3(pd.DataFrame(), INCOMING_CHANGED, col_to_track="sector")
        assert "prev_sector" in result.columns
        assert result["prev_sector"].isna().all()

    def test_returns_merged_with_prev_col(self):
        mgr = _make_manager()
        existing = EXISTING_DF[["symbol", "company_name", "sector", "is_current"]].copy()
        result = mgr.apply_scd3(existing, INCOMING_CHANGED, col_to_track="sector")
        assert "prev_sector" in result.columns

    def test_custom_prev_col_name(self):
        mgr = _make_manager()
        result = mgr.apply_scd3(pd.DataFrame(), INCOMING_CHANGED,
                                  col_to_track="sector", prev_col_name="old_sector")
        assert "old_sector" in result.columns
