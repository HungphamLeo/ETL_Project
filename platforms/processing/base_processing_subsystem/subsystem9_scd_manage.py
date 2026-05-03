"""
Slowly Changing Dimension (SCD) Manager
========================================
Subsystem 9: Slowly Changing Dimension Manager

SCD Type 1 - Overwrite: No history. Latest value wins. Used for: dim_market_type.
SCD Type 2 - Add new row: Full history via effective_date/end_date/is_current. Used for: dim_company, dim_industry.
SCD Type 3 - Add column: Keeps one previous value in a separate column.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


@dataclass
class SCD2Result:
    to_close:   pd.DataFrame
    to_insert:  pd.DataFrame
    unchanged:  pd.DataFrame
    stats:      Dict[str, int] = field(default_factory=dict)

    def has_changes(self) -> bool:
        return len(self.to_close) > 0 or len(self.to_insert) > 0

    def summary(self) -> str:
        return (
            f"SCD2 result: close={len(self.to_close)}, "
            f"insert={len(self.to_insert)}, unchanged={len(self.unchanged)}"
        )


@dataclass
class SCD1Result:
    to_update:  pd.DataFrame
    to_insert:  pd.DataFrame
    unchanged:  pd.DataFrame
    stats:      Dict[str, int] = field(default_factory=dict)


class SCDManager:
    """
    Subsystem 9: Slowly Changing Dimension Manager.
    Handles SCD Type 1, 2, and 3 logic at the pandas level.
    For Delta-level execution, use DeltaLakeStorageBackend.write_silver_scd2().
    """

    def __init__(
        self,
        natural_keys: List[str],
        tracked_cols: List[str],
        effective_date_col: str = "effective_date",
        end_date_col: str = "end_date",
        is_current_col: str = "is_current",
        row_hash_col: str = "_row_hash",
        surrogate_key_col: str = "surrogate_key",
        logger: Optional[logging.Logger] = None,
    ):
        self.natural_keys = natural_keys
        self.tracked_cols = tracked_cols
        self.effective_date_col = effective_date_col
        self.end_date_col = end_date_col
        self.is_current_col = is_current_col
        self.row_hash_col = row_hash_col
        self.surrogate_key_col = surrogate_key_col
        self.logger = logger or logging.getLogger(__name__)

    # ------------------------------------------------------------------
    # Hash computation
    # ------------------------------------------------------------------

    def compute_row_hash(self, record: Dict[str, Any]) -> str:
        raw = "|".join(str(record.get(col, "")) for col in sorted(self.tracked_cols))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def add_row_hash_column(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df[self.row_hash_col] = df.apply(
            lambda row: self.compute_row_hash(row.to_dict()), axis=1
        )
        return df

    # ------------------------------------------------------------------
    # SCD Type 1
    # ------------------------------------------------------------------

    def apply_scd1(
        self,
        existing_df: pd.DataFrame,
        incoming_df: pd.DataFrame,
        as_of: Optional[date] = None,
    ) -> SCD1Result:
        """SCD Type 1: Overwrite changed records, insert new records. No history."""
        as_of = as_of or date.today()

        if existing_df.empty:
            incoming_df = incoming_df.copy()
            incoming_df["update_time"] = pd.Timestamp(as_of)
            return SCD1Result(
                to_update=pd.DataFrame(),
                to_insert=incoming_df,
                unchanged=pd.DataFrame(),
                stats={"new": len(incoming_df), "updated": 0, "unchanged": 0},
            )

        existing_hashed = self.add_row_hash_column(existing_df)
        incoming_hashed = self.add_row_hash_column(incoming_df)

        existing_key_hash = existing_hashed.set_index(self.natural_keys)[self.row_hash_col]
        incoming_key_hash = incoming_hashed.set_index(self.natural_keys)[self.row_hash_col]

        # New records (key not in existing)
        existing_keys = set(map(tuple, existing_df[self.natural_keys].values.tolist()))
        incoming_keys = set(map(tuple, incoming_df[self.natural_keys].values.tolist()))

        new_keys = incoming_keys - existing_keys
        common_keys = incoming_keys & existing_keys

        to_insert_mask = incoming_df.apply(
            lambda r: tuple(r[k] for k in self.natural_keys) in new_keys, axis=1
        )
        to_insert = incoming_df[to_insert_mask].copy()
        to_insert["update_time"] = pd.Timestamp(as_of)

        # Changed records (key exists but hash differs)
        changed_keys = set()
        for key_tuple in common_keys:
            key_idx = tuple(key_tuple) if not isinstance(key_tuple, tuple) else key_tuple
            try:
                old_hash = existing_key_hash.loc[key_idx]
                new_hash = incoming_key_hash.loc[key_idx]
                if old_hash != new_hash:
                    changed_keys.add(key_tuple)
            except KeyError:
                pass

        to_update_mask = incoming_df.apply(
            lambda r: tuple(r[k] for k in self.natural_keys) in changed_keys, axis=1
        )
        to_update = incoming_df[to_update_mask].copy()
        to_update["update_time"] = pd.Timestamp(as_of)

        unchanged_mask = incoming_df.apply(
            lambda r: tuple(r[k] for k in self.natural_keys) in (common_keys - changed_keys), axis=1
        )
        unchanged = incoming_df[unchanged_mask].copy()

        self.logger.info(
            "[SCD1] new=%d, updated=%d, unchanged=%d",
            len(to_insert), len(to_update), len(unchanged),
        )

        return SCD1Result(
            to_update=to_update,
            to_insert=to_insert,
            unchanged=unchanged,
            stats={"new": len(to_insert), "updated": len(to_update), "unchanged": len(unchanged)},
        )

    # ------------------------------------------------------------------
    # SCD Type 2
    # ------------------------------------------------------------------

    def apply_scd2(
        self,
        existing_df: pd.DataFrame,
        incoming_df: pd.DataFrame,
        as_of: Optional[date] = None,
    ) -> SCD2Result:
        """
        SCD Type 2: Preserve full history.
        - Detects changes via row hash of tracked_cols.
        - Returns rows to close (set end_date, is_current=False).
        - Returns new rows to insert (effective_date=today, is_current=True).
        """
        as_of = as_of or date.today()

        incoming_hashed = self.add_row_hash_column(incoming_df)

        # First load
        if existing_df.empty:
            new_rows = incoming_hashed.copy()
            new_rows[self.effective_date_col] = as_of
            new_rows[self.end_date_col] = None
            new_rows[self.is_current_col] = True
            self.logger.info("[SCD2] Initial load: inserting %d rows", len(new_rows))
            return SCD2Result(
                to_close=pd.DataFrame(),
                to_insert=new_rows,
                unchanged=pd.DataFrame(),
                stats={"new": len(new_rows), "changed": 0, "unchanged": 0},
            )

        # Filter only current records from existing
        current_mask = existing_df.get(self.is_current_col, pd.Series([True] * len(existing_df)))
        current_df = existing_df[current_mask].copy()
        current_hashed = self.add_row_hash_column(current_df)

        # Build lookup: natural_key_tuple -> row_hash
        current_lookup: Dict[tuple, str] = {}
        for _, row in current_hashed.iterrows():
            key = tuple(row[k] for k in self.natural_keys)
            current_lookup[key] = row[self.row_hash_col]

        to_close_indices = []
        to_insert_rows = []
        unchanged_rows = []

        for _, inc_row in incoming_hashed.iterrows():
            key = tuple(inc_row[k] for k in self.natural_keys)
            inc_hash = inc_row[self.row_hash_col]

            if key not in current_lookup:
                # Brand new record
                new_row = inc_row.to_dict()
                new_row[self.effective_date_col] = as_of
                new_row[self.end_date_col] = None
                new_row[self.is_current_col] = True
                to_insert_rows.append(new_row)

            elif current_lookup[key] != inc_hash:
                # Changed record: close old, insert new version
                # Find the existing row index to close
                close_mask = current_hashed.apply(
                    lambda r: tuple(r[k] for k in self.natural_keys) == key, axis=1
                )
                to_close_indices.extend(current_hashed[close_mask].index.tolist())

                new_row = inc_row.to_dict()
                new_row[self.effective_date_col] = as_of
                new_row[self.end_date_col] = None
                new_row[self.is_current_col] = True
                to_insert_rows.append(new_row)

            else:
                # Unchanged
                unchanged_rows.append(inc_row.to_dict())

        # Build to_close DataFrame
        if to_close_indices:
            to_close = current_df.loc[to_close_indices].copy()
            to_close[self.end_date_col] = as_of
            to_close[self.is_current_col] = False
        else:
            to_close = pd.DataFrame()

        to_insert = pd.DataFrame(to_insert_rows) if to_insert_rows else pd.DataFrame()
        unchanged = pd.DataFrame(unchanged_rows) if unchanged_rows else pd.DataFrame()

        changed_count = len([r for r in to_insert_rows if r.get(self.effective_date_col) == as_of]) - len(
            [r for r in to_insert_rows if r not in unchanged_rows]
        ) if to_insert_rows else 0

        self.logger.info(
            "[SCD2] to_close=%d, to_insert=%d, unchanged=%d",
            len(to_close), len(to_insert), len(unchanged),
        )

        return SCD2Result(
            to_close=to_close,
            to_insert=to_insert,
            unchanged=unchanged,
            stats={
                "new": len([r for r in to_insert_rows]),
                "closed": len(to_close),
                "unchanged": len(unchanged),
            },
        )

    # ------------------------------------------------------------------
    # SCD Type 3
    # ------------------------------------------------------------------

    def apply_scd3(
        self,
        existing_df: pd.DataFrame,
        incoming_df: pd.DataFrame,
        col_to_track: str,
        prev_col_name: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        SCD Type 3: Keep current + one previous value.
        Adds a 'prev_{col_to_track}' column to store the old value.

        Args:
            existing_df: Current dimension table
            incoming_df: New records
            col_to_track: Column to track previous value for
            prev_col_name: Name for the previous value column (default: prev_{col_to_track})
        Returns:
            Merged DataFrame with prev_ column added
        """
        prev_col = prev_col_name or f"prev_{col_to_track}"

        if existing_df.empty:
            result = incoming_df.copy()
            result[prev_col] = None
            return result

        merged = incoming_df.merge(
            existing_df[self.natural_keys + [col_to_track]].rename(
                columns={col_to_track: prev_col}
            ),
            on=self.natural_keys,
            how="left",
        )
        return merged


# ---------------------------------------------------------------------------
# Delta-level SCD2 SQL helpers (for use with Spark SQL)
# ---------------------------------------------------------------------------

class DeltaSCD2SQLBuilder:
    """
    Generates Delta SQL for SCD2 MERGE operations.
    Subsystem 9 (SCD Manager) + Subsystem 25 (Version Control via Delta log).
    """

    @staticmethod
    def build_close_sql(
        target_path: str,
        source_alias: str,
        natural_keys: List[str],
        effective_date_col: str = "effective_date",
        end_date_col: str = "end_date",
        is_current_col: str = "is_current",
        row_hash_col: str = "_row_hash",
    ) -> str:
        """Generate MERGE SQL to close changed records (Step 1 of SCD2)."""
        match_cond = " AND ".join(f"t.{k} = s.{k}" for k in natural_keys)
        return f"""
            MERGE INTO delta.`{target_path}` AS t
            USING {source_alias} AS s
            ON ({match_cond}) AND t.{is_current_col} = true AND t.{row_hash_col} <> s.{row_hash_col}
            WHEN MATCHED THEN UPDATE SET
                t.{is_current_col} = false,
                t.{end_date_col} = current_date()
            """

    @staticmethod
    def build_insert_new_sql(
        target_path: str,
        source_alias: str,
        natural_keys: List[str],
        effective_date_col: str = "effective_date",
        end_date_col: str = "end_date",
        is_current_col: str = "is_current",
    ) -> str:
        """Generate INSERT SQL for new/changed records (Step 2 of SCD2)."""
        match_cond = " AND ".join(f"t.{k} = s.{k}" for k in natural_keys)
        return f"""
                MERGE INTO delta.`{target_path}` AS t
                USING (
                    SELECT s.*, current_date() AS {effective_date_col}, NULL AS {end_date_col}, true AS {is_current_col}
                    FROM {source_alias} s
                    LEFT ANTI JOIN delta.`{target_path}` t
                    ON ({match_cond}) AND t.{is_current_col} = true AND t._row_hash = s._row_hash
                ) AS new_records
                ON false
                WHEN NOT MATCHED THEN INSERT *
                """


# ---------------------------------------------------------------------------
# Pre-built SCD managers for cophieu68 dimensions
# ---------------------------------------------------------------------------

# DIM_COMPANY_SCD_MANAGER = SCDManager(
#     natural_keys=["symbol"],
#     tracked_cols=[
#         "company_name", "full_name", "english_name", "short_name",
#         "address", "phone", "fax", "website", "email_address",
#         "established_date", "listed_date", "listed_volume",
#         "circulating_volume", "market_capitalization",
#     ],
# )

# DIM_INDUSTRY_SCD_MANAGER = SCDManager(
#     natural_keys=["industry_code", "industry_metric"],
#     tracked_cols=[
#         "industry_name", "industry_craw_url",
#     ],
# )