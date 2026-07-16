"""
Unit tests – Subsystem 10: SurrogateKeyGenerator
"""
from __future__ import annotations

import pytest
import pandas as pd

from platforms.processing.base_processing_subsystem.subsystem10_surrogate_key_generator import (
    SurrogateKeyGenerator,
)


# ---------------------------------------------------------------------------
# hash_key
# ---------------------------------------------------------------------------

class TestSurrogateKeyGeneratorHashKey:
    def test_single_part_key(self):
        gen = SurrogateKeyGenerator()
        key = gen.hash_key("VCB")
        assert isinstance(key, str)
        assert len(key) == 32

    def test_multi_part_key(self):
        gen = SurrogateKeyGenerator()
        key = gen.hash_key("VCB", "2024-01-15")
        assert len(key) == 32

    def test_deterministic(self):
        gen = SurrogateKeyGenerator()
        k1 = gen.hash_key("FPT", "2024-01-01")
        k2 = gen.hash_key("FPT", "2024-01-01")
        assert k1 == k2

    def test_different_inputs_different_keys(self):
        gen = SurrogateKeyGenerator()
        k1 = gen.hash_key("FPT")
        k2 = gen.hash_key("VNM")
        assert k1 != k2

    def test_none_parts_handled(self):
        gen = SurrogateKeyGenerator()
        # None parts should be filtered out
        key = gen.hash_key("FPT", None, "2024-01-01")
        assert isinstance(key, str)

    def test_prefix_applied(self):
        gen = SurrogateKeyGenerator(prefix="CO_")
        key = gen.hash_key("VCB")
        assert key.startswith("CO_")

    def test_key_length_respected(self):
        gen = SurrogateKeyGenerator(key_length=16)
        key = gen.hash_key("FPT")
        assert len(key) == 16

    def test_key_length_max_64(self):
        # SHA-256 produces max 64 hex chars
        gen = SurrogateKeyGenerator(key_length=100)
        key = gen.hash_key("FPT")
        assert len(key) == 64

    def test_key_is_hex_string(self):
        gen = SurrogateKeyGenerator()
        key = gen.hash_key("FPT")
        assert all(c in "0123456789abcdef" for c in key)


# ---------------------------------------------------------------------------
# hash_key_from_record
# ---------------------------------------------------------------------------

class TestSurrogateKeyGeneratorHashKeyFromRecord:
    def test_extracts_fields_from_dict(self):
        gen = SurrogateKeyGenerator()
        record = {"symbol": "FPT", "date": "2024-01-01", "extra": "ignored"}
        key = gen.hash_key_from_record(record, key_fields=["symbol", "date"])
        # Should equal direct call
        expected = gen.hash_key("FPT", "2024-01-01")
        assert key == expected

    def test_missing_field_treated_as_none(self):
        gen = SurrogateKeyGenerator()
        record = {"symbol": "FPT"}
        key = gen.hash_key_from_record(record, key_fields=["symbol", "missing_col"])
        assert isinstance(key, str)


# ---------------------------------------------------------------------------
# add_hash_key_column
# ---------------------------------------------------------------------------

class TestSurrogateKeyGeneratorAddHashKeyColumn:
    def _sample_df(self):
        return pd.DataFrame([
            {"symbol": "FPT", "date": "2024-01-01"},
            {"symbol": "VNM", "date": "2024-01-01"},
            {"symbol": "HPG", "date": "2024-01-02"},
        ])

    def test_column_added(self):
        gen = SurrogateKeyGenerator()
        df = self._sample_df()
        result = gen.add_hash_key_column(df, key_fields=["symbol", "date"])
        assert "surrogate_key" in result.columns

    def test_custom_output_col(self):
        gen = SurrogateKeyGenerator()
        df = self._sample_df()
        result = gen.add_hash_key_column(df, key_fields=["symbol"], output_col="company_key")
        assert "company_key" in result.columns

    def test_key_is_unique_per_row(self):
        gen = SurrogateKeyGenerator()
        df = self._sample_df()
        result = gen.add_hash_key_column(df, key_fields=["symbol", "date"])
        assert result["surrogate_key"].nunique() == 3

    def test_same_key_for_same_natural_key(self):
        gen = SurrogateKeyGenerator()
        df = pd.DataFrame([
            {"symbol": "FPT", "date": "2024-01-01"},
            {"symbol": "FPT", "date": "2024-01-01"},
        ])
        result = gen.add_hash_key_column(df, key_fields=["symbol", "date"])
        assert result["surrogate_key"].iloc[0] == result["surrogate_key"].iloc[1]

    def test_does_not_overwrite_when_overwrite_false(self):
        gen = SurrogateKeyGenerator()
        df = self._sample_df()
        df["surrogate_key"] = "existing_value"
        result = gen.add_hash_key_column(df, key_fields=["symbol"], overwrite=False)
        assert all(result["surrogate_key"] == "existing_value")

    def test_overwrites_when_overwrite_true(self):
        gen = SurrogateKeyGenerator()
        df = self._sample_df()
        df["surrogate_key"] = "old"
        result = gen.add_hash_key_column(df, key_fields=["symbol"], overwrite=True)
        assert not any(result["surrogate_key"] == "old")

    def test_missing_key_field_adds_none_column(self):
        gen = SurrogateKeyGenerator()
        df = self._sample_df()
        result = gen.add_hash_key_column(df, key_fields=["nonexistent"])
        assert result["surrogate_key"].isna().all()

    def test_original_df_not_mutated(self):
        gen = SurrogateKeyGenerator()
        df = self._sample_df()
        _ = gen.add_hash_key_column(df, key_fields=["symbol"])
        assert "surrogate_key" not in df.columns


# ---------------------------------------------------------------------------
# uuid_key and run_id
# ---------------------------------------------------------------------------

class TestSurrogateKeyGeneratorUUID:
    def test_uuid_key_is_32_hex_chars(self):
        key = SurrogateKeyGenerator.uuid_key()
        assert len(key) == 32
        assert all(c in "0123456789abcdef" for c in key)

    def test_uuid_key_is_unique(self):
        k1 = SurrogateKeyGenerator.uuid_key()
        k2 = SurrogateKeyGenerator.uuid_key()
        assert k1 != k2

    def test_run_id_starts_with_run(self):
        rid = SurrogateKeyGenerator.run_id()
        assert rid.startswith("run_")

    def test_run_id_format(self):
        rid = SurrogateKeyGenerator.run_id()
        parts = rid.split("_")
        assert len(parts) >= 3
        # second part should be date: YYYYMMDD
        assert len(parts[1]) == 8
        assert parts[1].isdigit()
