"""
Surrogate Key Generator
========================
Subsystem 10: Surrogate Key Generator

Generates deterministic or sequential surrogate keys for dimension tables.

Two strategies:
  1. SHA-256 Hash Key (deterministic): SHA256(natural_key_parts) → 32-char hex
     - Idempotent: same input always produces same key
     - No central state needed
     - Used for: dim_company (symbol), dim_industry (code+metric), fact rows

  2. Sequential Integer Key (stateful): auto-increment via Delta meta table
     - Used when integer keys are required for legacy BI tools

Usage:
    gen = SurrogateKeyGenerator()
    key = gen.hash_key("VCB")                          # company_key
    key = gen.hash_key("VCB", "2024-01-01")            # trade_key
    key = gen.hash_key("^nh", "summary_info")          # industry_sk

    # Batch: add surrogate key column to DataFrame
    df = gen.add_hash_key_column(df, key_fields=["symbol"], output_col="company_key")
"""

from __future__ import annotations

import hashlib
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd


class SurrogateKeyGenerator:
    """
    Subsystem 10: Surrogate Key Generator.

    Generates deterministic SHA-256 based surrogate keys.
    Keys are 32-character hex strings (first 32 chars of SHA-256 digest).
    """

    def __init__(
        self,
        prefix: str = "",
        key_length: int = 32,
        logger: Optional[logging.Logger] = None,
    ):
        """
        Args:
            prefix: Optional prefix for all generated keys (e.g. "CO_" for company)
            key_length: Length of hex digest to use (max 64 for SHA-256)
        """
        self.prefix = prefix
        self.key_length = min(key_length, 64)
        self.logger = logger or logging.getLogger(__name__)

    # ------------------------------------------------------------------
    # Hash-based (deterministic) key generation
    # ------------------------------------------------------------------

    def hash_key(self, *parts: Any) -> str:
        """
        Generate a deterministic surrogate key from one or more parts.
        Parts are joined with '|' separator before hashing.

        Examples:
            gen.hash_key("VCB")                    → company_key for VCB
            gen.hash_key("VCB", "2024-01-15")      → trade_key for VCB on 2024-01-15
            gen.hash_key("^nh", "summary_info")    → industry_sk for banking sector
        """
        raw = "|".join(str(p) for p in parts if p is not None)
        digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:self.key_length]
        return f"{self.prefix}{digest}" if self.prefix else digest

    def hash_key_from_record(self, record: Dict[str, Any], key_fields: List[str]) -> str:
        """Generate surrogate key from specific fields of a record dict."""
        parts = [record.get(f) for f in key_fields]
        return self.hash_key(*parts)

    # ------------------------------------------------------------------
    # DataFrame batch operations
    # ------------------------------------------------------------------

    def add_hash_key_column(
        self,
        df: pd.DataFrame,
        key_fields: List[str],
        output_col: str = "surrogate_key",
        overwrite: bool = False,
    ) -> pd.DataFrame:
        """
        Add a surrogate key column to a DataFrame.

        Args:
            df: Input DataFrame
            key_fields: Columns to hash for key generation
            output_col: Name of the new surrogate key column
            overwrite: If True, overwrite existing column; if False, skip if exists
        Returns:
            DataFrame with surrogate key column added
        """
        if output_col in df.columns and not overwrite:
            self.logger.debug("[SKG] Column '%s' already exists, skipping", output_col)
            return df

        missing = [f for f in key_fields if f not in df.columns]
        if missing:
            self.logger.warning("[SKG] Key fields not found in DataFrame: %s", missing)
            df = df.copy()
            df[output_col] = None
            return df

        df = df.copy()
        df[output_col] = df.apply(
            lambda row: self.hash_key(*[row[f] for f in key_fields]),
            axis=1,
        )
        return df

    def add_hash_key_spark(self, df, key_fields: List[str], output_col: str = "surrogate_key"):
        """
        Add surrogate key column to a Spark DataFrame using SHA-256.
        Subsystem 10 + Subsystem 31 (Parallelizing).

        Args:
            df: Spark DataFrame
            key_fields: Columns to hash
            output_col: Output column name
        Returns:
            Spark DataFrame with surrogate key column
        """
        from pyspark.sql import functions as F
        hash_expr = F.sha2(
            F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in key_fields]),
            256,
        )
        # Truncate to key_length characters
        return df.withColumn(output_col, F.substring(hash_expr, 1, self.key_length))

    # ------------------------------------------------------------------
    # UUID-based key generation (for non-deterministic cases)
    # ------------------------------------------------------------------

    @staticmethod
    def uuid_key() -> str:
        """Generate a random UUID-based surrogate key (non-deterministic)."""
        return str(uuid.uuid4()).replace("-", "")

    @staticmethod
    def run_id() -> str:
        """Generate a unique pipeline run ID (timestamp + UUID)."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        uid = str(uuid.uuid4())[:8]
        return f"run_{ts}_{uid}"


# ---------------------------------------------------------------------------
# Pre-built generators for each dimension/fact table
# ---------------------------------------------------------------------------

COMPANY_KEY_GEN = SurrogateKeyGenerator(prefix="", key_length=32)
INDUSTRY_KEY_GEN = SurrogateKeyGenerator(prefix="", key_length=32)
TRADE_KEY_GEN = SurrogateKeyGenerator(prefix="", key_length=32)
INCOME_KEY_GEN = SurrogateKeyGenerator(prefix="", key_length=32)
BALANCE_KEY_GEN = SurrogateKeyGenerator(prefix="", key_length=32)
PLAN_KEY_GEN = SurrogateKeyGenerator(prefix="", key_length=32)
FINANCIAL_RATIO_KEY_GEN = SurrogateKeyGenerator(prefix="", key_length=32)
INDUSTRY_SUMMARY_KEY_GEN = SurrogateKeyGenerator(prefix="", key_length=32)


def enrich_with_surrogate_keys(records: List[Dict[str, Any]], table_name: str) -> List[Dict[str, Any]]:
    """
    Convenience function: add the correct surrogate key to records based on table_name.
    Maps table names to their key fields and generator.

    Args:
        records: List of record dicts
        table_name: Target table name (e.g. "fact_trading_history", "dim_company")
    Returns:
        Records with surrogate key field injected
    """
    KEY_CONFIG: Dict[str, Dict] = {
        "dim_company": {
            "key_field": "company_key",
            "key_parts": ["symbol"],
            "gen": COMPANY_KEY_GEN,
        },
        "dim_industry": {
            "key_field": "industry_sk",
            "key_parts": ["industry_code", "industry_metric"],
            "gen": INDUSTRY_KEY_GEN,
        },
        "fact_trading_history": {
            "key_field": "trade_key",
            "key_parts": ["symbol", "date"],
            "gen": TRADE_KEY_GEN,
        },
        "fact_income_statement": {
            "key_field": "income_key",
            "key_parts": ["symbol", "report_type", "year", "period", "metric_code"],
            "gen": INCOME_KEY_GEN,
        },
        "fact_balance_sheet": {
            "key_field": "balance_key",
            "key_parts": ["symbol", "report_type", "year", "period", "metric_code"],
            "gen": BALANCE_KEY_GEN,
        },
        "fact_business_plan": {
            "key_field": "plan_key",
            "key_parts": ["symbol", "year"],
            "gen": PLAN_KEY_GEN,
        },
        "fact_financial_metrics": {
            "key_field": "financial_ratio_key",
            "key_parts": ["symbol"],
            "gen": FINANCIAL_RATIO_KEY_GEN,
        },
        "fact_industry_summary": {
            "key_field": "industry_summary_key",
            "key_parts": ["industry_code", "industry_metric_type"],
            "gen": INDUSTRY_SUMMARY_KEY_GEN,
        },
    }

    cfg = KEY_CONFIG.get(table_name)
    if not cfg:
        return records

    key_field = cfg["key_field"]
    key_parts = cfg["key_parts"]
    gen: SurrogateKeyGenerator = cfg["gen"]

    enriched = []
    for record in records:
        record = dict(record)
        record[key_field] = gen.hash_key_from_record(record, key_parts)
        enriched.append(record)

    return enriched
