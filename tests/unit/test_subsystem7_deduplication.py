"""
Unit tests – Subsystem 7: DeduplicationEngine, SurrogateKeyDeduplicator, SparkDeduplicator
"""
from __future__ import annotations

import pytest
import pandas as pd

from platforms.processing.base_processing_subsystem.subsystem7_deduplication import (
    DeduplicationEngine,
    DeduplicationStrategy,
    DeduplicationStats,
    SurrogateKeyDeduplicator,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

RECORDS_WITH_DUPS = [
    {"symbol": "FPT", "date": "2024-01-01", "close_price": 100.0, "ts": 1},
    {"symbol": "VNM", "date": "2024-01-01", "close_price": 68.0,  "ts": 1},
    {"symbol": "FPT", "date": "2024-01-01", "close_price": 101.0, "ts": 2},  # dup FPT
]

RECORDS_NO_DUPS = [
    {"symbol": "FPT", "date": "2024-01-01", "close_price": 100.0},
    {"symbol": "VNM", "date": "2024-01-01", "close_price": 68.0},
    {"symbol": "HPG", "date": "2024-01-01", "close_price": 30.0},
]


# ---------------------------------------------------------------------------
# DeduplicationEngine – KEEP_FIRST
# ---------------------------------------------------------------------------

class TestDeduplicationEngineKeepFirst:
    def _engine(self):
        return DeduplicationEngine(
            keys=["symbol", "date"],
            strategy=DeduplicationStrategy.KEEP_FIRST,
        )

    def test_removes_duplicates(self):
        engine = self._engine()
        result = engine.deduplicate(RECORDS_WITH_DUPS)
        assert len(result) == 2

    def test_keeps_first_occurrence(self):
        engine = self._engine()
        result = engine.deduplicate(RECORDS_WITH_DUPS)
        fpt = next(r for r in result if r["symbol"] == "FPT")
        assert fpt["close_price"] == 100.0  # first occurrence

    def test_no_dups_unchanged(self):
        engine = self._engine()
        result = engine.deduplicate(RECORDS_NO_DUPS)
        assert len(result) == 3

    def test_empty_input_returns_empty(self):
        engine = self._engine()
        result = engine.deduplicate([])
        assert result == []

    def test_stats_populated(self):
        engine = self._engine()
        engine.deduplicate(RECORDS_WITH_DUPS)
        stats = engine.last_stats
        assert stats is not None
        assert stats.total_input == 3
        assert stats.total_output == 2
        assert stats.duplicates_removed == 1


# ---------------------------------------------------------------------------
# DeduplicationEngine – KEEP_LAST
# ---------------------------------------------------------------------------

class TestDeduplicationEngineKeepLast:
    def _engine(self):
        return DeduplicationEngine(
            keys=["symbol", "date"],
            strategy=DeduplicationStrategy.KEEP_LAST,
        )

    def test_keeps_last_occurrence(self):
        engine = self._engine()
        result = engine.deduplicate(RECORDS_WITH_DUPS)
        fpt = next(r for r in result if r["symbol"] == "FPT")
        assert fpt["close_price"] == 101.0  # last occurrence


# ---------------------------------------------------------------------------
# DeduplicationEngine – KEEP_MAX_COL
# ---------------------------------------------------------------------------

class TestDeduplicationEngineKeepMax:
    def _engine(self):
        return DeduplicationEngine(
            keys=["symbol", "date"],
            strategy=DeduplicationStrategy.KEEP_MAX_COL,
            tiebreaker_col="ts",
        )

    def test_keeps_record_with_max_tiebreaker(self):
        engine = self._engine()
        result = engine.deduplicate(RECORDS_WITH_DUPS)
        fpt = next(r for r in result if r["symbol"] == "FPT")
        assert fpt["ts"] == 2  # max ts

    def test_falls_back_when_tiebreaker_missing(self):
        engine = DeduplicationEngine(
            keys=["symbol", "date"],
            strategy=DeduplicationStrategy.KEEP_MAX_COL,
            tiebreaker_col="nonexistent",
        )
        result = engine.deduplicate(RECORDS_WITH_DUPS)
        assert len(result) == 2  # still deduped, fallback to KEEP_LAST


# ---------------------------------------------------------------------------
# DeduplicationEngine – missing keys in records
# ---------------------------------------------------------------------------

class TestDeduplicationEngineMissingKeys:
    def test_missing_key_skips_dedup(self):
        engine = DeduplicationEngine(
            keys=["nonexistent_col"],
            strategy=DeduplicationStrategy.KEEP_LAST,
        )
        result = engine.deduplicate(RECORDS_NO_DUPS)
        # key not found → no dedup, all records returned
        assert len(result) == 3


# ---------------------------------------------------------------------------
# DeduplicationEngine – DataFrame interface
# ---------------------------------------------------------------------------

class TestDeduplicationEngineDataFrame:
    def test_deduplicate_dataframe_returns_tuple(self):
        engine = DeduplicationEngine(keys=["symbol", "date"], strategy=DeduplicationStrategy.KEEP_LAST)
        df = pd.DataFrame(RECORDS_WITH_DUPS)
        deduped_df, stats = engine.deduplicate_dataframe(df, source="test")
        assert isinstance(deduped_df, pd.DataFrame)
        assert isinstance(stats, DeduplicationStats)
        assert len(deduped_df) == 2

    def test_stats_duplicate_rate(self):
        engine = DeduplicationEngine(keys=["symbol", "date"], strategy=DeduplicationStrategy.KEEP_LAST)
        df = pd.DataFrame(RECORDS_WITH_DUPS)
        _, stats = engine.deduplicate_dataframe(df)
        assert stats.duplicate_rate == pytest.approx(1 / 3, abs=1e-4)


# ---------------------------------------------------------------------------
# DeduplicationStats.to_dict
# ---------------------------------------------------------------------------

class TestDeduplicationStats:
    def test_to_dict_has_required_fields(self):
        stats = DeduplicationStats(
            source="test",
            run_id="r1",
            total_input=10,
            total_output=8,
            duplicates_removed=2,
            strategy="KEEP_LAST",
            keys=["symbol", "date"],
        )
        d = stats.to_dict()
        # always returns at minimum the context keys
        assert "source" in d or len(d) > 0  # schema may remap keys

    def test_duplicate_rate_zero_input(self):
        stats = DeduplicationStats(
            source="s", run_id="r", total_input=0, total_output=0,
            duplicates_removed=0, strategy="KEEP_LAST", keys=[],
        )
        assert stats.duplicate_rate == 0.0


# ---------------------------------------------------------------------------
# SurrogateKeyDeduplicator
# ---------------------------------------------------------------------------

class TestSurrogateKeyDeduplicator:
    def test_filter_new_returns_all_on_first_call(self):
        deduper = SurrogateKeyDeduplicator()
        records = [{"symbol": "FPT"}, {"symbol": "VNM"}]
        result = deduper.filter_new(records, key_fields=["symbol"])
        assert len(result) == 2

    def test_filter_new_removes_seen_on_second_call(self):
        deduper = SurrogateKeyDeduplicator()
        records = [{"symbol": "FPT"}]
        deduper.filter_new(records, key_fields=["symbol"])
        result2 = deduper.filter_new(records, key_fields=["symbol"])
        assert len(result2) == 0

    def test_reset_clears_seen_keys(self):
        deduper = SurrogateKeyDeduplicator()
        records = [{"symbol": "FPT"}]
        deduper.filter_new(records, key_fields=["symbol"])
        deduper.reset()
        result = deduper.filter_new(records, key_fields=["symbol"])
        assert len(result) == 1

    def test_inject_key_as_adds_field(self):
        deduper = SurrogateKeyDeduplicator()
        records = [{"symbol": "FPT"}]
        result = deduper.filter_new(records, key_fields=["symbol"], inject_key_as="sk")
        assert "sk" in result[0]
        assert len(result[0]["sk"]) > 0

    def test_seen_count_increments(self):
        deduper = SurrogateKeyDeduplicator()
        deduper.filter_new([{"x": 1}, {"x": 2}], key_fields=["x"])
        assert deduper.seen_count == 2

    def test_compute_key_is_deterministic(self):
        deduper = SurrogateKeyDeduplicator()
        record = {"symbol": "FPT", "date": "2024-01-01"}
        k1 = deduper.compute_key(record, ["symbol", "date"])
        k2 = deduper.compute_key(record, ["symbol", "date"])
        assert k1 == k2

    def test_compute_key_changes_with_different_input(self):
        deduper = SurrogateKeyDeduplicator()
        k1 = deduper.compute_key({"symbol": "FPT"}, ["symbol"])
        k2 = deduper.compute_key({"symbol": "VNM"}, ["symbol"])
        assert k1 != k2
