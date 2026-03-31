"""
Delta Lake Storage Backend
==========================
Implements StorageBackend interface using PySpark + delta-spark.

Supports:
  - Bronze layer: append-only raw ingestion with audit columns
  - Silver layer: MERGE upsert (idempotent) + SCD2 via SCDManager
  - Gold layer: overwrite with OPTIMIZE + ZORDER for Power BI

Subsystems covered:
  - Subsystem 3  : Extract System (storage target)
  - Subsystem 6  : Audit Dimension (auto-inject _ingested_at, _pipeline_run_id)
  - Subsystem 9  : SCD Manager (via merge_scd2)
  - Subsystem 13 : Fact Table Builders (write_silver_fact)
  - Subsystem 22 : Job Scheduler integration (run_id propagation)
  - Subsystem 25 : Version Control (Delta transaction log)
  - Subsystem 27 : Workflow Monitor (Delta history)
  - Subsystem 29 : Lineage (run_id in every row)
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd

from platforms.storage.base_storage import StorageBackend, to_primitive
from platforms.storage.deltalake.delta_schema_registry import (
    DeltaTableDef,
    get_table_def,
    ALL_BRONZE_TABLES,
    ALL_SILVER_TABLES,
    ALL_GOLD_TABLES,
)


# ---------------------------------------------------------------------------
# Lazy Spark session factory (avoids import errors when PySpark not installed)
# ---------------------------------------------------------------------------

def _get_spark(app_name: str = "ETL_Lakehouse"):
    """
    Build or retrieve a SparkSession with Delta Lake extensions.
    Falls back gracefully if PySpark is not installed.
    """
    try:
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip  # type: ignore

        builder = (
            SparkSession.builder.appName(app_name)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        )
        return configure_spark_with_delta_pip(builder).getOrCreate()
    except ImportError as exc:
        raise RuntimeError(
            "PySpark + delta-spark are required for DeltaLakeStorageBackend. "
            "Install via: pip install delta-spark pyspark"
        ) from exc


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _make_surrogate_key(*parts: Any) -> str:
    """SHA-256 based surrogate key from concatenated string parts."""
    import hashlib
    raw = "|".join(str(p) for p in parts if p is not None)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def _to_spark_df(spark, data: Any, schema_cols: Optional[List] = None):
    """
    Convert various Python data types to a Spark DataFrame.
    Accepts: list[dict], pd.DataFrame, dict, or JSON string.
    """
    from pyspark.sql import functions as F

    if isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError:
            data = [{"value": data}]

    if isinstance(data, pd.DataFrame):
        data = data.where(pd.notnull(data), None).to_dict(orient="records")

    if isinstance(data, dict):
        data = [data]

    if not isinstance(data, list):
        data = [{"value": str(data)}]

    if not data:
        return None

    # Sanitize: convert non-serializable values
    clean = []
    for row in data:
        if isinstance(row, dict):
            clean.append({k: (None if isinstance(v, float) and v != v else v)
                          for k, v in row.items()})
        else:
            clean.append({"value": str(row)})

    return spark.createDataFrame(clean)


def _add_audit_columns(df, ingested_at: datetime, source: str, run_id: str):
    """Inject standard audit columns (Subsystem 6)."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import TimestampType

    return (
        df.withColumn("_ingested_at", F.lit(ingested_at).cast(TimestampType()))
          .withColumn("_source", F.lit(source))
          .withColumn("_pipeline_run_id", F.lit(run_id))
    )


# ---------------------------------------------------------------------------
# Core Delta Lake Storage Backend
# ---------------------------------------------------------------------------

class DeltaLakeStorageBackend(StorageBackend):
    """
    StorageBackend implementation backed by Delta Lake (PySpark).

    Usage:
        backend = DeltaLakeStorageBackend(
            base_path="/data/lakehouse",
            source_name="cophieu68",
            pipeline_run_id="run_20240101_001",
        )
        backend.write_bronze("trading_data", records)
        backend.write_silver_merge("fact_trading_history", df, ["trade_key"])
    """

    def __init__(
        self,
        base_path: str,
        source_name: str = "cophieu68",
        pipeline_run_id: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
        spark=None,
    ):
        self.base_path = base_path.rstrip("/")
        self.source_name = source_name
        self.pipeline_run_id = pipeline_run_id or _make_surrogate_key(
            source_name, _now_utc().isoformat()
        )
        self.logger = logger or logging.getLogger(__name__)
        self._spark = spark  # allow injection for testing

    # ------------------------------------------------------------------
    # Spark session (lazy)
    # ------------------------------------------------------------------

    @property
    def spark(self):
        if self._spark is None:
            self._spark = _get_spark()
        return self._spark

    def _table_path(self, relative_path: str) -> str:
        return f"{self.base_path}/{relative_path}"

    # ------------------------------------------------------------------
    # StorageBackend interface (required abstract methods)
    # ------------------------------------------------------------------

    def save(self, dataset_name: str, data: Any, fmt: Optional[str] = None) -> Dict[str, Any]:
        """
        Generic save: auto-routes to bronze layer by dataset_name.
        Implements StorageBackend.save().
        """
        return self.write_bronze(dataset_name, data)

    def create_database(self, name: str) -> Dict[str, Any]:
        """Create a Delta Lake 'database' as a directory."""
        path = os.path.join(self.base_path, name)
        os.makedirs(path, exist_ok=True)
        return {"ok": True, "path": path}

    def delete_database(self, name: str) -> Dict[str, Any]:
        import shutil
        path = os.path.join(self.base_path, name)
        if os.path.exists(path):
            shutil.rmtree(path)
        return {"ok": True, "path": path}

    def create_schema(self, name: str, schema: Optional[Dict] = None) -> Dict[str, Any]:
        """Create Delta table from schema registry definition."""
        table_def = get_table_def(*name.split(".", 1)) if "." in name else None
        if table_def:
            return self.ensure_table(table_def)
        path = os.path.join(self.base_path, name)
        os.makedirs(path, exist_ok=True)
        return {"ok": True, "path": path}

    def rename_schema(self, old_name: str, new_name: str) -> Dict[str, Any]:
        import shutil
        old_path = os.path.join(self.base_path, old_name)
        new_path = os.path.join(self.base_path, new_name)
        if os.path.exists(old_path):
            shutil.move(old_path, new_path)
        return {"ok": True, "from": old_path, "to": new_path}

    def create_table(self, name: str, schema: Optional[Dict] = None) -> Dict[str, Any]:
        return self.create_schema(name, schema)

    def truncate_table(self, name: str) -> Dict[str, Any]:
        """Delete all rows from a Delta table (DELETE FROM)."""
        try:
            table_def = get_table_def(*name.split(".", 1)) if "." in name else None
            path = self._table_path(table_def.relative_path) if table_def else os.path.join(self.base_path, name)
            self.spark.sql(f"DELETE FROM delta.`{path}`")
            return {"ok": True, "table": name}
        except Exception as e:
            self.logger.exception("truncate_table failed: %s", name)
            return {"ok": False, "error": str(e)}

    def delete_table(self, name: str) -> Dict[str, Any]:
        import shutil
        table_def = get_table_def(*name.split(".", 1)) if "." in name else None
        path = self._table_path(table_def.relative_path) if table_def else os.path.join(self.base_path, name)
        if os.path.exists(path):
            shutil.rmtree(path)
        return {"ok": True, "table": name, "path": path}

    def rename_table(self, old_name: str, new_name: str) -> Dict[str, Any]:
        import shutil
        old_path = os.path.join(self.base_path, old_name)
        new_path = os.path.join(self.base_path, new_name)
        if os.path.exists(old_path):
            shutil.move(old_path, new_path)
        return {"ok": True, "from": old_path, "to": new_path}

    def insert(self, target: str, data: Any) -> Dict[str, Any]:
        return self.write_bronze(target, data)

    def update(self, target: str, query: Dict[str, Any], update_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Partial update via Delta MERGE (match on query keys, set update_doc fields)."""
        try:
            from delta.tables import DeltaTable  # type: ignore
            from pyspark.sql import functions as F

            table_def = get_table_def(*target.split(".", 1)) if "." in target else None
            path = self._table_path(table_def.relative_path) if table_def else os.path.join(self.base_path, target)

            delta_tbl = DeltaTable.forPath(self.spark, path)
            match_cond = " AND ".join(f"t.{k} = '{v}'" for k, v in query.items())
            set_map = {k: F.lit(v) for k, v in update_doc.items()}
            delta_tbl.update(condition=match_cond, set=set_map)
            return {"ok": True, "target": target}
        except Exception as e:
            self.logger.exception("update failed: %s", target)
            return {"ok": False, "error": str(e)}

    # ------------------------------------------------------------------
    # Table provisioning
    # ------------------------------------------------------------------

    def ensure_table(self, table_def: DeltaTableDef) -> Dict[str, Any]:
        """
        Create Delta table if it does not exist.
        Uses CREATE TABLE IF NOT EXISTS with schema from registry.
        """
        try:
            ddl = table_def.create_table_sql(self.base_path)
            self.spark.sql(ddl)
            self.logger.info("Ensured Delta table: %s at %s/%s",
                             table_def.table_name, self.base_path, table_def.relative_path)
            return {"ok": True, "table": table_def.table_name}
        except Exception as e:
            self.logger.exception("ensure_table failed: %s", table_def.table_name)
            return {"ok": False, "error": str(e)}

    def provision_all_tables(self, layers: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Create all Delta tables for specified layers (default: all).
        Call once during environment setup.
        """
        layers = layers or ["bronze", "silver", "gold"]
        results = {}
        layer_map = {
            "bronze": ALL_BRONZE_TABLES,
            "silver": ALL_SILVER_TABLES,
            "gold":   ALL_GOLD_TABLES,
        }
        for layer in layers:
            for tname, tdef in layer_map.get(layer, {}).items():
                results[f"{layer}.{tname}"] = self.ensure_table(tdef)
        return results

    # ------------------------------------------------------------------
    # BRONZE – append-only raw ingestion
    # ------------------------------------------------------------------

    def write_bronze(
        self,
        table_name: str,
        data: Any,
        extra_cols: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Append raw data to Bronze layer.
        Automatically adds audit columns (_ingested_at, _source, _pipeline_run_id).
        Subsystem 3 (Extract) + Subsystem 6 (Audit).

        Args:
            table_name: Key in ALL_BRONZE_TABLES (e.g. "trading_data")
            data: list[dict] | pd.DataFrame | dict | JSON string
            extra_cols: Additional columns to inject (e.g. {"symbol": "VCB"})
        """
        try:
            table_def = ALL_BRONZE_TABLES.get(table_name)
            if not table_def:
                # fallback: write to ad-hoc path
                path = self._table_path(f"bronze/{table_name}")
                table_def = None
            else:
                path = self._table_path(table_def.relative_path)

            df = _to_spark_df(self.spark, data)
            if df is None:
                return {"ok": True, "inserted_count": 0, "note": "empty data"}

            now = _now_utc()

            # Inject extra columns before audit
            if extra_cols:
                from pyspark.sql import functions as F
                for col_name, col_val in extra_cols.items():
                    df = df.withColumn(col_name, F.lit(col_val))

            # Audit columns (Subsystem 6)
            df = _add_audit_columns(df, now, self.source_name, self.pipeline_run_id)

            # Partition helper columns
            if table_def and "year" in [c.name for c in table_def.columns] and "_year" in [c.name for c in table_def.columns]:
                from pyspark.sql import functions as F
                df = (df.withColumn("_year", F.year(F.col("_ingested_at")))
                        .withColumn("_month", F.month(F.col("_ingested_at"))))

            # Write append
            writer = df.write.format("delta").mode("append").option("mergeSchema", "true")
            if table_def and table_def.partition_by:
                writer = writer.partitionBy(*table_def.partition_by)
            writer.save(path)

            count = df.count()
            self.logger.info("[BRONZE] Appended %d rows → %s", count, path)
            return {"ok": True, "inserted_count": count, "path": path, "layer": "bronze"}

        except Exception as e:
            self.logger.exception("[BRONZE] write_bronze failed: %s", table_name)
            return {"ok": False, "error": str(e), "table": table_name}

    # ------------------------------------------------------------------
    # SILVER – MERGE upsert (idempotent, Subsystem 7 Deduplication)
    # ------------------------------------------------------------------

    def write_silver_merge(
        self,
        table_name: str,
        data: Any,
        merge_keys: Optional[List[str]] = None,
        extra_cols: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Upsert data into Silver layer using Delta MERGE.
        Idempotent: re-running with same data produces same result.
        Subsystem 7 (Deduplication) + Subsystem 13 (Fact Table Builders).

        Args:
            table_name: Key in ALL_SILVER_TABLES
            data: list[dict] | pd.DataFrame | dict
            merge_keys: Columns to match on (default: table_def.primary_keys)
            extra_cols: Additional columns to inject
        """
        try:
            from delta.tables import DeltaTable  # type: ignore
            from pyspark.sql import functions as F

            table_def = ALL_SILVER_TABLES.get(table_name)
            if not table_def:
                path = self._table_path(f"silver/{table_name}")
            else:
                path = self._table_path(table_def.relative_path)

            merge_keys = merge_keys or (table_def.primary_keys if table_def else [])
            if not merge_keys:
                self.logger.warning("[SILVER] No merge keys for %s, falling back to append", table_name)
                return self._write_silver_append(table_name, data, table_def, extra_cols)

            df_new = _to_spark_df(self.spark, data)
            if df_new is None:
                return {"ok": True, "inserted_count": 0, "note": "empty data"}

            now = _now_utc()
            if extra_cols:
                for col_name, col_val in extra_cols.items():
                    df_new = df_new.withColumn(col_name, F.lit(col_val))

            df_new = _add_audit_columns(df_new, now, self.source_name, self.pipeline_run_id)

            # Ensure table exists
            if not os.path.exists(path):
                writer = df_new.write.format("delta").mode("overwrite").option("mergeSchema", "true")
                if table_def and table_def.partition_by:
                    writer = writer.partitionBy(*table_def.partition_by)
                writer.save(path)
                count = df_new.count()
                self.logger.info("[SILVER] Created + inserted %d rows → %s", count, path)
                return {"ok": True, "inserted_count": count, "path": path, "layer": "silver", "action": "create"}

            # MERGE
            delta_tbl = DeltaTable.forPath(self.spark, path)
            merge_cond = " AND ".join(f"t.{k} = s.{k}" for k in merge_keys)

            (
                delta_tbl.alias("t")
                .merge(df_new.alias("s"), merge_cond)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

            count = df_new.count()
            self.logger.info("[SILVER] Merged %d rows → %s", count, path)
            return {"ok": True, "merged_count": count, "path": path, "layer": "silver", "action": "merge"}

        except Exception as e:
            self.logger.exception("[SILVER] write_silver_merge failed: %s", table_name)
            return {"ok": False, "error": str(e), "table": table_name}

    def _write_silver_append(
        self,
        table_name: str,
        data: Any,
        table_def: Optional[DeltaTableDef],
        extra_cols: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Fallback append for silver tables without primary keys."""
        path = self._table_path(table_def.relative_path) if table_def else self._table_path(f"silver/{table_name}")
        df = _to_spark_df(self.spark, data)
        if df is None:
            return {"ok": True, "inserted_count": 0}
        now = _now_utc()
        if extra_cols:
            from pyspark.sql import functions as F
            for col_name, col_val in extra_cols.items():
                df = df.withColumn(col_name, F.lit(col_val))
        df = _add_audit_columns(df, now, self.source_name, self.pipeline_run_id)
        writer = df.write.format("delta").mode("append").option("mergeSchema", "true")
        if table_def and table_def.partition_by:
            writer = writer.partitionBy(*table_def.partition_by)
        writer.save(path)
        count = df.count()
        return {"ok": True, "inserted_count": count, "path": path, "layer": "silver"}

    # ------------------------------------------------------------------
    # SILVER – SCD Type 2 (Subsystem 9)
    # ------------------------------------------------------------------

    def write_silver_scd2(
        self,
        table_name: str,
        data: Any,
        natural_keys: List[str],
        tracked_cols: List[str],
        extra_cols: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Apply SCD Type 2 logic to a Silver dimension table.
        - Detects changes in tracked_cols via row hash.
        - Closes old records (end_date = today, is_current = False).
        - Inserts new records (effective_date = today, is_current = True).
        Subsystem 9 (SCD Manager).

        Args:
            table_name: Key in ALL_SILVER_TABLES (e.g. "dim_company")
            data: Incoming records
            natural_keys: Business keys (e.g. ["symbol"])
            tracked_cols: Columns that trigger a new SCD2 version when changed
        """
        try:
            from delta.tables import DeltaTable  # type: ignore
            from pyspark.sql import functions as F
            from pyspark.sql.types import BooleanType, DateType

            table_def = ALL_SILVER_TABLES.get(table_name)
            path = self._table_path(table_def.relative_path) if table_def else self._table_path(f"silver/{table_name}")

            df_new = _to_spark_df(self.spark, data)
            if df_new is None:
                return {"ok": True, "inserted_count": 0, "note": "empty data"}

            now = _now_utc()
            today = now.date()

            if extra_cols:
                for col_name, col_val in extra_cols.items():
                    df_new = df_new.withColumn(col_name, F.lit(col_val))

            # Compute row hash for change detection (Subsystem 7)
            hash_expr = F.sha2(
                F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in tracked_cols]),
                256,
            )
            df_new = (
                df_new
                .withColumn("_row_hash", hash_expr)
                .withColumn("effective_date", F.lit(today).cast(DateType()))
                .withColumn("end_date", F.lit(None).cast(DateType()))
                .withColumn("is_current", F.lit(True).cast(BooleanType()))
            )
            df_new = _add_audit_columns(df_new, now, self.source_name, self.pipeline_run_id)

            # First load: just write
            if not os.path.exists(path):
                writer = df_new.write.format("delta").mode("overwrite").option("mergeSchema", "true")
                writer.save(path)
                count = df_new.count()
                self.logger.info("[SILVER SCD2] Initial load %d rows → %s", count, path)
                return {"ok": True, "inserted_count": count, "path": path, "action": "initial_load"}

            delta_tbl = DeltaTable.forPath(self.spark, path)

            # Step 1: Close changed records
            match_cond = " AND ".join(f"t.{k} = s.{k}" for k in natural_keys)
            close_cond = f"({match_cond}) AND t.is_current = true AND t._row_hash <> s._row_hash"

            delta_tbl.alias("t").merge(
                df_new.alias("s"), close_cond
            ).whenMatchedUpdate(set={
                "is_current": F.lit(False),
                "end_date":   F.lit(today).cast(DateType()),
            }).execute()

            # Step 2: Insert new versions (only changed or new records)
            existing = delta_tbl.toDF()
            current_keys = existing.filter(F.col("is_current") == True).select(*natural_keys, "_row_hash")

            # Records that are new OR have changed hash
            df_to_insert = df_new.alias("n").join(
                current_keys.alias("e"),
                on=natural_keys,
                how="left_anti",
            )
            # Also insert changed records (matched but hash differs)
            df_changed = df_new.alias("n").join(
                current_keys.alias("e"),
                on=natural_keys,
                how="inner",
            ).filter(F.col("n._row_hash") != F.col("e._row_hash")).select("n.*")

            from functools import reduce
            from pyspark.sql import DataFrame
            dfs = [df for df in [df_to_insert, df_changed] if df.count() > 0]
            if dfs:
                df_final = reduce(DataFrame.unionByName, dfs)
                df_final.write.format("delta").mode("append").option("mergeSchema", "true").save(path)
                inserted = df_final.count()
            else:
                inserted = 0

            self.logger.info("[SILVER SCD2] Closed changed + inserted %d new versions → %s", inserted, path)
            return {"ok": True, "inserted_count": inserted, "path": path, "action": "scd2_update"}

        except Exception as e:
            self.logger.exception("[SILVER SCD2] write_silver_scd2 failed: %s", table_name)
            return {"ok": False, "error": str(e), "table": table_name}

    # ------------------------------------------------------------------
    # GOLD – overwrite + OPTIMIZE + ZORDER (Subsystem 19, 20)
    # ------------------------------------------------------------------

    def write_gold(
        self,
        table_name: str,
        data: Any,
        mode: str = "overwrite",
        run_optimize: bool = True,
        extra_cols: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Write to Gold layer. Overwrites by default (full refresh from Silver).
        Runs OPTIMIZE + ZORDER for Power BI query performance.
        Subsystem 19 (Aggregate Builder) + Subsystem 20 (OLAP Cube Builder).

        Args:
            table_name: Key in ALL_GOLD_TABLES
            data: Transformed/aggregated data
            mode: "overwrite" (default) or "append"
            run_optimize: Run OPTIMIZE + ZORDER after write
        """
        try:
            from pyspark.sql import functions as F

            table_def = ALL_GOLD_TABLES.get(table_name)
            path = self._table_path(table_def.relative_path) if table_def else self._table_path(f"gold/{table_name}")

            df = _to_spark_df(self.spark, data)
            if df is None:
                return {"ok": True, "inserted_count": 0, "note": "empty data"}

            now = _now_utc()
            if extra_cols:
                for col_name, col_val in extra_cols.items():
                    df = df.withColumn(col_name, F.lit(col_val))
            df = df.withColumn("_updated_at", F.lit(now).cast("timestamp"))

            writer = df.write.format("delta").mode(mode).option("mergeSchema", "true")
            if table_def and table_def.partition_by:
                writer = writer.partitionBy(*table_def.partition_by)
            writer.save(path)

            count = df.count()
            self.logger.info("[GOLD] Wrote %d rows → %s", count, path)

            # OPTIMIZE + ZORDER (Subsystem 19)
            if run_optimize and table_def and table_def.zorder_by:
                zorder_cols = ", ".join(table_def.zorder_by)
                self.spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({zorder_cols})")
                self.logger.info("[GOLD] OPTIMIZE ZORDER BY (%s) on %s", zorder_cols, path)

            return {"ok": True, "inserted_count": count, "path": path, "layer": "gold"}

        except Exception as e:
            self.logger.exception("[GOLD] write_gold failed: %s", table_name)
            return {"ok": False, "error": str(e), "table": table_name}

    # ------------------------------------------------------------------
    # READ helpers
    # ------------------------------------------------------------------

    def read_table(
        self,
        layer: str,
        table_name: str,
        version: Optional[int] = None,
        timestamp: Optional[str] = None,
        filters: Optional[str] = None,
    ):
        """
        Read a Delta table with optional time-travel.
        Subsystem 25 (Version Control) – Delta transaction log.

        Args:
            layer: bronze | silver | gold
            table_name: table key in registry
            version: Delta version number for time-travel
            timestamp: ISO timestamp string for time-travel
            filters: SQL WHERE clause string
        Returns:
            Spark DataFrame
        """
        layer_map = {"bronze": ALL_BRONZE_TABLES, "silver": ALL_SILVER_TABLES, "gold": ALL_GOLD_TABLES}
        table_def = layer_map.get(layer, {}).get(table_name)
        path = self._table_path(table_def.relative_path) if table_def else self._table_path(f"{layer}/{table_name}")

        reader = self.spark.read.format("delta")
        if version is not None:
            reader = reader.option("versionAsOf", version)
        elif timestamp:
            reader = reader.option("timestampAsOf", timestamp)

        df = reader.load(path)
        if filters:
            df = df.filter(filters)
        return df

    def get_table_history(self, layer: str, table_name: str, limit: int = 20):
        """
        Return Delta transaction history for a table.
        Subsystem 27 (Workflow Monitor) + Subsystem 29 (Lineage).
        """
        from delta.tables import DeltaTable  # type: ignore
        layer_map = {"bronze": ALL_BRONZE_TABLES, "silver": ALL_SILVER_TABLES, "gold": ALL_GOLD_TABLES}
        table_def = layer_map.get(layer, {}).get(table_name)
        path = self._table_path(table_def.relative_path) if table_def else self._table_path(f"{layer}/{table_name}")
        delta_tbl = DeltaTable.forPath(self.spark, path)
        return delta_tbl.history(limit)

    def read_as_pandas(
        self,
        layer: str,
        table_name: str,
        version: Optional[int] = None,
        filters: Optional[str] = None,
    ) -> pd.DataFrame:
        """Read Delta table into pandas DataFrame (for small result sets / BI export)."""
        df = self.read_table(layer, table_name, version=version, filters=filters)
        return df.toPandas()

    # ------------------------------------------------------------------
    # VACUUM & OPTIMIZE utilities (Subsystem 23 Backup + Subsystem 28 Sorting)
    # ------------------------------------------------------------------

    def vacuum(self, layer: str, table_name: str, retention_hours: int = 168) -> Dict[str, Any]:
        """
        Remove old Delta files beyond retention window.
        Subsystem 23 (Backup System) – keeps last N hours of versions.
        Default retention: 7 days (168 hours).
        """
        try:
            layer_map = {"bronze": ALL_BRONZE_TABLES, "silver": ALL_SILVER_TABLES, "gold": ALL_GOLD_TABLES}
            table_def = layer_map.get(layer, {}).get(table_name)
            path = self._table_path(table_def.relative_path) if table_def else self._table_path(f"{layer}/{table_name}")
            self.spark.sql(f"VACUUM delta.`{path}` RETAIN {retention_hours} HOURS")
            self.logger.info("[VACUUM] %s/%s retained %dh", layer, table_name, retention_hours)
            return {"ok": True, "table": table_name, "retention_hours": retention_hours}
        except Exception as e:
            self.logger.exception("[VACUUM] failed: %s/%s", layer, table_name)
            return {"ok": False, "error": str(e)}

    def optimize(self, layer: str, table_name: str) -> Dict[str, Any]:
        """
        Run OPTIMIZE (compaction) + ZORDER on a Delta table.
        Subsystem 28 (Sorting System) – improves query performance.
        """
        try:
            layer_map = {"bronze": ALL_BRONZE_TABLES, "silver": ALL_SILVER_TABLES, "gold": ALL_GOLD_TABLES}
            table_def = layer_map.get(layer, {}).get(table_name)
            path = self._table_path(table_def.relative_path) if table_def else self._table_path(f"{layer}/{table_name}")
            if table_def and table_def.zorder_by:
                zorder_cols = ", ".join(table_def.zorder_by)
                self.spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({zorder_cols})")
            else:
                self.spark.sql(f"OPTIMIZE delta.`{path}`")
            self.logger.info("[OPTIMIZE] %s/%s done", layer, table_name)
            return {"ok": True, "table": table_name}
        except Exception as e:
            self.logger.exception("[OPTIMIZE] failed: %s/%s", layer, table_name)
            return {"ok": False, "error": str(e)}

    # ------------------------------------------------------------------
    # Change Data Feed (CDC – Subsystem 2)
    # ------------------------------------------------------------------

    def read_cdf(self, layer: str, table_name: str, starting_version: int = 0):
        """
        Read Change Data Feed from a CDF-enabled Delta table.
        Subsystem 2 (Change Data Capture System).
        Returns Spark DataFrame with _change_type, _commit_version, _commit_timestamp.
        """
        layer_map = {"bronze": ALL_BRONZE_TABLES, "silver": ALL_SILVER_TABLES, "gold": ALL_GOLD_TABLES}
        table_def = layer_map.get(layer, {}).get(table_name)
        path = self._table_path(table_def.relative_path) if table_def else self._table_path(f"{layer}/{table_name}")
        return (
            self.spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", starting_version)
            .load(path)
        )


# ---------------------------------------------------------------------------
# Convenience factory
# ---------------------------------------------------------------------------

def build_delta_backend(
    config: Dict[str, Any],
    source_name: str = "cophieu68",
    pipeline_run_id: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
) -> DeltaLakeStorageBackend:
    """
    Build a DeltaLakeStorageBackend from pipeline config dict.

    Expected config structure (from cophieu68_config.yaml):
        storage:
          deltalake:
            base_path: /data/lakehouse
            app_name: ETL_Lakehouse
    """
    delta_cfg = config.get("storage", {}).get("deltalake", {})
    base_path = delta_cfg.get("base_path", "./data/lakehouse")
    app_name = delta_cfg.get("app_name", "ETL_Lakehouse")

    spark = _get_spark(app_name)
    return DeltaLakeStorageBackend(
        base_path=base_path,
        source_name=source_name,
        pipeline_run_id=pipeline_run_id,
        logger=logger,
        spark=spark,
    )
