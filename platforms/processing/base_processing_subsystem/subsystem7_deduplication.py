
"""
Deduplication System
====================
Subsystem 7: Deduplication System

Handles deduplication at two levels:
  1. In-memory (pandas): for small batches before writing to Delta
  2. Delta MERGE-based: idempotent upsert ensures no duplicates in Delta tables

Strategies:
  - KEEP_FIRST  : keep first occurrence by insertion order
  - KEEP_LAST   : keep last occurrence (latest update wins)
  - KEEP_BY_COL : keep record with max/min value in a tiebreaker column

Usage:
    deduper = DeduplicationEngine(
        keys=["symbol", "date"],
        strategy=DeduplicationStrategy.KEEP_LAST,
        tiebreaker_col="update_time",
    )
    deduped = deduper.deduplicate(records)
    stats = deduper.last_stats
"""

from __future__ import annotations

import hashlib
import logging
import os
import yaml
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd


def load_dedup_config() -> Dict[str, Any]:
    config_path = os.path.join(os.path.dirname(__file__), "config", "deduplication.yaml")
    if os.path.exists(config_path):
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
                return config.get("deduplication", {}) if config else {}
        except Exception:
            pass
    return {}

DEDUP_CONFIG = load_dedup_config()
MSG_TEMPLATES = DEDUP_CONFIG.get("msg_templates", {})
DEFAULTS = DEDUP_CONFIG.get("defaults", {})

# ---------------------------------------------------------------------------
# Strategy enum
# ---------------------------------------------------------------------------

class DeduplicationStrategy(str, Enum):
    KEEP_FIRST      = "KEEP_FIRST"       # keep first occurrence in input order
    KEEP_LAST       = "KEEP_LAST"        # keep last occurrence in input order
    KEEP_MAX_COL    = "KEEP_MAX_COL"     # keep record with max value in tiebreaker_col
    KEEP_MIN_COL    = "KEEP_MIN_COL"     # keep record with min value in tiebreaker_col


# ---------------------------------------------------------------------------
# Stats dataclass
# ---------------------------------------------------------------------------

@dataclass
class DeduplicationStats:
    source:          str
    run_id:          str
    total_input:     int
    total_output:    int
    duplicates_removed: int
    strategy:        str
    keys:            List[str]
    deduped_at:      datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def duplicate_rate(self) -> float:
        return round(self.duplicates_removed / self.total_input, 4) if self.total_input else 0.0

    def to_dict(self) -> Dict[str, Any]:
        context = asdict(self)
        context["duplicate_rate"] = self.duplicate_rate
        context["deduped_at"] = self.deduped_at.isoformat()
        
        schema = DEDUP_CONFIG.get("stats_schema")
        if not schema:
            return context
            
        record = {}
        for field_name, template in schema.items():
            if isinstance(template, str):
                if "{" in template and "}" in template:
                    record[field_name] = template.format(**context)
                elif template in context:
                    record[field_name] = context[template]
                else:
                    record[field_name] = template
            else:
                record[field_name] = template
        return record


# ---------------------------------------------------------------------------
# Core Deduplication Engine
# ---------------------------------------------------------------------------

class DeduplicationEngine:
    """
    Subsystem 7: Deduplication System.

    Performs in-memory deduplication on list[dict] or pd.DataFrame.
    For Delta-level deduplication, use DeltaLakeStorageBackend.write_silver_merge()
    which performs MERGE ON primary_keys (idempotent upsert).
    """

    def __init__(
        self,
        keys: List[str],
        strategy: Optional[DeduplicationStrategy] = None,
        tiebreaker_col: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
    ):
        """
        Args:
            keys: Columns that form the composite natural key
            strategy: Which record to keep when duplicates found
            tiebreaker_col: Column used for KEEP_MAX_COL / KEEP_MIN_COL strategies
        """
        self.keys = keys
        self.strategy = strategy if strategy is not None else DeduplicationStrategy(DEFAULTS.get("strategy", "KEEP_LAST"))
        self.tiebreaker_col = tiebreaker_col
        self.logger = logger or logging.getLogger(__name__)
        self._last_stats: Optional[DeduplicationStats] = None

    @property
    def last_stats(self) -> Optional[DeduplicationStats]:
        return self._last_stats

    # ------------------------------------------------------------------
    # Main deduplication entry points
    # ------------------------------------------------------------------

    def deduplicate(
        self,
        records: List[Dict[str, Any]],
        source: str = "unknown",
        run_id: str = "unknown",
    ) -> List[Dict[str, Any]]:
        """
        Deduplicate a list of dicts.
        Returns deduplicated list preserving dict structure.
        """
        if not records:
            self._last_stats = DeduplicationStats(
                source=source, run_id=run_id,
                total_input=0, total_output=0, duplicates_removed=0,
                strategy=self.strategy.value, keys=self.keys,
            )
            return []

        df = pd.DataFrame(records)
        deduped_df = self._deduplicate_df(df)
        result = deduped_df.where(pd.notnull(deduped_df), None).to_dict(orient="records")

        removed = len(records) - len(result)
        self._last_stats = DeduplicationStats(
            source=source, run_id=run_id,
            total_input=len(records), total_output=len(result),
            duplicates_removed=removed,
            strategy=self.strategy.value, keys=self.keys,
        )

        if removed > 0:
            template = MSG_TEMPLATES.get("removed_duplicates_dict", "[DEDUP] {source}: removed {removed} duplicates ({total_input} -> {total_output}) by keys={keys} strategy={strategy}")
            self.logger.info(template.format(
                source=source, removed=removed, 
                total_input=len(records), total_output=len(result), 
                keys=self.keys, strategy=self.strategy.value
            ))

        return result

    def deduplicate_dataframe(
        self,
        df: pd.DataFrame,
        source: str = "unknown",
        run_id: str = "unknown",
    ) -> Tuple[pd.DataFrame, DeduplicationStats]:
        """
        Deduplicate a pandas DataFrame.
        Returns (deduped_df, stats).
        """
        total_input = len(df)
        deduped = self._deduplicate_df(df)
        removed = total_input - len(deduped)

        stats = DeduplicationStats(
            source=source, run_id=run_id,
            total_input=total_input, total_output=len(deduped),
            duplicates_removed=removed,
            strategy=self.strategy.value, keys=self.keys,
        )
        self._last_stats = stats

        if removed > 0:
            template = MSG_TEMPLATES.get("removed_duplicates_df", "[DEDUP] {source}: removed {removed} duplicates ({total_input} -> {total_output})")
            self.logger.info(template.format(
                source=source, removed=removed, 
                total_input=total_input, total_output=len(deduped)
            ))

        return deduped, stats

    # ------------------------------------------------------------------
    # Internal strategy implementations
    # ------------------------------------------------------------------

    def _deduplicate_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply deduplication strategy to a DataFrame."""
        # Validate keys exist
        missing_keys = [k for k in self.keys if k not in df.columns]
        if missing_keys:
            template = MSG_TEMPLATES.get("keys_not_found", "[DEDUP] Keys not found in DataFrame: {missing_keys} - skipping dedup")
            self.logger.warning(template.format(missing_keys=missing_keys))
            return df

        if self.strategy == DeduplicationStrategy.KEEP_FIRST:
            return df.drop_duplicates(subset=self.keys, keep="first").reset_index(drop=True)

        elif self.strategy == DeduplicationStrategy.KEEP_LAST:
            return df.drop_duplicates(subset=self.keys, keep="last").reset_index(drop=True)

        elif self.strategy in (DeduplicationStrategy.KEEP_MAX_COL, DeduplicationStrategy.KEEP_MIN_COL):
            if not self.tiebreaker_col or self.tiebreaker_col not in df.columns:
                template = MSG_TEMPLATES.get("tiebreaker_not_found", "[DEDUP] tiebreaker_col '{tiebreaker_col}' not found, falling back to KEEP_LAST")
                self.logger.warning(template.format(tiebreaker_col=self.tiebreaker_col))
                return df.drop_duplicates(subset=self.keys, keep="last").reset_index(drop=True)

            ascending = self.strategy == DeduplicationStrategy.KEEP_MIN_COL
            return (
                df.sort_values(self.tiebreaker_col, ascending=ascending)
                  .drop_duplicates(subset=self.keys, keep="last")
                  .reset_index(drop=True)
            )

        return df


# ---------------------------------------------------------------------------
# Surrogate Key Deduplication (cross-batch)
# ---------------------------------------------------------------------------

class SurrogateKeyDeduplicator:
    """
    Tracks seen surrogate keys across batches to prevent cross-batch duplicates.
    Useful for streaming/micro-batch scenarios where the same record
    may arrive in multiple batches.

    Subsystem 7 (Deduplication) + Subsystem 10 (Surrogate Key Generator).
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        self._seen: set = set()
        self.logger = logger or logging.getLogger(__name__)

    def compute_key(self, record: Dict[str, Any], key_fields: List[str]) -> str:
        """Compute a deterministic surrogate key from key_fields."""
        hash_length = DEFAULTS.get("hash_length", 32)
        raw = "|".join(str(record.get(f, "")) for f in key_fields)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:hash_length]

    def filter_new(
        self,
        records: List[Dict[str, Any]],
        key_fields: List[str],
        inject_key_as: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Return only records whose surrogate key has NOT been seen before.
        Optionally injects the computed key into each record.

        Args:
            records: Input records
            key_fields: Fields to hash for surrogate key
            inject_key_as: If set, injects computed key into record under this field name
        """
        new_records = []
        for record in records:
            key = self.compute_key(record, key_fields)
            if key not in self._seen:
                self._seen.add(key)
                if inject_key_as:
                    record = {**record, inject_key_as: key}
                new_records.append(record)

        removed = len(records) - len(new_records)
        if removed > 0:
            template = MSG_TEMPLATES.get("cross_batch_filtered", "[CROSS-BATCH DEDUP] Filtered {removed} already-seen records")
            self.logger.info(template.format(removed=removed))

        return new_records

    def reset(self) -> None:
        """Clear seen keys (call between full-refresh runs)."""
        self._seen.clear()

    @property
    def seen_count(self) -> int:
        return len(self._seen)


# ---------------------------------------------------------------------------
# Delta-level deduplication helpers (Spark-based)
# ---------------------------------------------------------------------------

class SparkDeduplicator:
    """
    Spark-based deduplication for large datasets.
    Used in Silver layer processing before writing to Delta.
    Subsystem 7 (Deduplication) + Subsystem 31 (Parallelizing/Pipelining).
    """

    @staticmethod
    def deduplicate_spark_df(
        df,
        keys: List[str],
        tiebreaker_col: Optional[str] = None,
        ascending: bool = False,
    ):
        """
        Deduplicate a Spark DataFrame.

        Args:
            df: Spark DataFrame
            keys: Partition keys for deduplication
            tiebreaker_col: Column to sort by before dedup (keep last by default)
            ascending: Sort order for tiebreaker (False = keep max/latest)
        Returns:
            Deduplicated Spark DataFrame
        """
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        if tiebreaker_col and tiebreaker_col in df.columns:
            sort_col = F.col(tiebreaker_col).asc() if ascending else F.col(tiebreaker_col).desc()
            window = Window.partitionBy(*keys).orderBy(sort_col)
            return (
                df.withColumn("_dedup_rank", F.row_number().over(window))
                  .filter(F.col("_dedup_rank") == 1)
                  .drop("_dedup_rank")
            )
        else:
            return df.dropDuplicates(keys)

    @staticmethod
    def add_row_hash(df, tracked_cols: List[str], hash_col: str = "_row_hash"):
        """
        Add a SHA-256 hash column over tracked_cols.
        Used for change detection in SCD2 (Subsystem 9).
        """
        from pyspark.sql import functions as F
        hash_expr = F.sha2(
            F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in tracked_cols]),
            256,
        )
        return df.withColumn(hash_col, hash_expr)

    @staticmethod
    def find_duplicates_spark(df, keys: List[str]):
        """
        Return a Spark DataFrame of duplicate records (count > 1 per key).
        Useful for data quality reporting (Subsystem 1).
        """
        from pyspark.sql import functions as F
        return (
            df.groupBy(*keys)
              .agg(F.count("*").alias("_dup_count"))
              .filter(F.col("_dup_count") > 1)
        )


# ---------------------------------------------------------------------------
# Pre-built deduplication configs for cophieu68 tables
# ---------------------------------------------------------------------------

# TRADING_DATA_DEDUPER = DeduplicationEngine(
#     keys=["symbol", "date"],
#     strategy=DeduplicationStrategy.KEEP_LAST,
#     tiebreaker_col="update_time",
# )

# COMPANY_INFO_DEDUPER = DeduplicationEngine(
#     keys=["symbol"],
#     strategy=DeduplicationStrategy.KEEP_LAST,
#     tiebreaker_col="update_time",
# )

# INCOME_STATEMENT_DEDUPER = DeduplicationEngine(
#     keys=["symbol", "report_type", "year", "period", "metric_code"],
#     strategy=DeduplicationStrategy.KEEP_LAST,
#     tiebreaker_col="update_time",
# )

# BALANCE_SHEET_DEDUPER = DeduplicationEngine(
#     keys=["symbol", "report_type", "year", "period", "metric_code"],
#     strategy=DeduplicationStrategy.KEEP_LAST,
#     tiebreaker_col="update_time",
# )

# INDUSTRY_SECTORS_DEDUPER = DeduplicationEngine(
#     keys=["industry_code", "symbol"],
#     strategy=DeduplicationStrategy.KEEP_LAST,
# )