#!/usr/bin/env python3
"""
Unified Master Lakehouse ETL Pipeline
=====================================

Tổng hợp: Master Orchestration + Subsystem Integration + CLI

Kiến trúc: Bronze → Silver → Gold → Serving (PostgreSQL)
Stack: Polars | DuckDB | SQLMesh | Delta Lake | Prefect | PostgreSQL

Usage:
  python scripts/deploy_full_pipeline.py full --symbols FPT VNM --date 2026-05-19
  python scripts/deploy_full_pipeline.py bronze --symbols FPT VNM
  python scripts/deploy_full_pipeline.py silver --date 2026-05-19
  python scripts/deploy_full_pipeline.py gold --date 2026-05-19
  python scripts/deploy_full_pipeline.py serving --date 2026-05-19
  python scripts/deploy_full_pipeline.py validate
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from dotenv import load_dotenv

# Ensure project root is on sys.path for internal imports.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

from shared.logger.python_main_logger import logger_manager

from platforms.processing.base_processing_subsystem.subsystem5_and_30_error_event_schema_and_escalate import (
    ErrorEventLog,
    ErrorEvent,
    ErrorLevel,
)
from platforms.processing.base_processing_subsystem.subsystem34_metadata_repo import MetadataRepository
from platforms.processing.base_processing_subsystem.subsystem1_data_profiling import DataProfiler
from platforms.processing.base_processing_subsystem.subsystem4_data_quality_pre_evaluate import CleansingRuleSet
from platforms.processing.base_processing_subsystem.subsystem7_deduplication import (
    DeduplicationEngine,
    DeduplicationStrategy,
)
from platforms.processing.base_processing_subsystem.subsystem10_surrogate_key_generator import SurrogateKeyGenerator

from platforms.orchestration.prefect.flows.prefect_orchestra_etl import PrefectETLPipelineConfig
from platforms.ingestion.cophieu68.dto.extract_models import CRAWL_MARKET_LIST_CONFIG
from platforms.ingestion.cophieu68.extract.extract_cophieu68 import ExtractCophieu68
from platforms.processing.polars.polars_engine import PolarsConfig, PolarsEngine
from platforms.processing.duckdb.duckdb_engine import DuckDBConfig, DuckDBEngine
from platforms.processing.sqlmesh.sqlmesh_engine import SqlMeshConfig, SqlMeshEngine

load_dotenv()

# ==========================================================================
# CONSTANTS & ENV
# ==========================================================================

LAKEHOUSE_BASE = os.getenv("LAKEHOUSE_BASE_PATH", "s3a://lakehouse")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
S3_SECRET = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin_secure_123")

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "etl_project")
PG_USER = os.getenv("POSTGRES_USER", "")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")

SQLMESH_PATH = os.getenv("SQLMESH_PATH", str(PROJECT_ROOT / "platforms" / "processing" / "sqlmesh"))
SQLMESH_GATEWAY = os.getenv("SQLMESH_GATEWAY", "local_duckdb")

DEFAULT_SYMBOLS = ["FPT", "VNM", "HPG", "MBB", "SSI"]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "platforms" / "orchestration" / "prefect" / "config" / "cophieu68_config.yaml"

STORAGE_OPTIONS: Dict[str, str] = {
    "endpoint_url": S3_ENDPOINT,
    "aws_access_key_id": S3_KEY,
    "aws_secret_access_key": S3_SECRET,
}

# ==========================================================================
# HELPERS: IDs + config builders
# ==========================================================================

def make_batch_id(symbol: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    uid = uuid.uuid4().hex[:8]
    return f"batch_{symbol.upper()}_{ts}_{uid}"


def make_run_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    uid = uuid.uuid4().hex[:6]
    return f"run_{ts}_{uid}"


class ExecutionPhase(str, Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    SERVING = "serving"
    FULL = "full"
    VALIDATE = "validate"


class ProcessingBackend(str, Enum):
    POLARS = "polars"
    SPARK = "spark"
    SQLMESH = "sqlmesh"
    DUCKDB = "duckdb"
    DBT = "dbt"


@dataclass
class ExecutionContext:
    run_id: str
    phase: ExecutionPhase
    symbols: List[str]
    backend: ProcessingBackend
    target_date: str
    dry_run: bool = False
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    end_time: Optional[datetime] = None
    status: str = "RUNNING"
    error_message: Optional[str] = None
    metadata_repo: Optional[MetadataRepository] = None
    error_log: Optional[ErrorEventLog] = None
    logger: Optional[logging.Logger] = None

    def duration_seconds(self) -> float:
        end = self.end_time or datetime.now(timezone.utc)
        return (end - self.start_time).total_seconds()


class ConfigurationManager:
    def __init__(self, config_path: Optional[str] = None, logger: Optional[logging.Logger] = None):
        self.logger = logger or logger_manager.get_logger(__name__)
        self.config_path = Path(config_path or DEFAULT_CONFIG_PATH)
        self.config: Dict[str, Any] = {}
        self.prefect_config: Optional[PrefectETLPipelineConfig] = None

    def load(self) -> Dict[str, Any]:
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                self.config = yaml.safe_load(f) or {}
            self.logger.info(f"[CONFIG] Loaded from {self.config_path}")
            return self.config
        except FileNotFoundError:
            self.logger.error(f"[CONFIG] File not found: {self.config_path}")
            return {}
        except Exception as exc:
            self.logger.error(f"[CONFIG] Error loading config: {exc}")
            return {}

    def load_prefect_config(self) -> Optional[PrefectETLPipelineConfig]:
        try:
            self.prefect_config = PrefectETLPipelineConfig(config_path=str(self.config_path))
            self.logger.info("[CONFIG] Prefect config loaded successfully")
            return self.prefect_config
        except Exception as exc:
            self.logger.error(f"[CONFIG] Error loading Prefect config: {exc}")
            return None

    def validate(self) -> Dict[str, Any]:
        errors = []
        if not self.config:
            errors.append("Configuration is empty")
        proj_params = self.config.get("project_params", self.config)
        required_sections = ["sources"]
        for section in required_sections:
            if section not in proj_params:
                errors.append(f"Missing required section: {section}")
        return {"is_valid": not errors, "errors": errors}


def _build_polars_engine(config: Dict[str, Any]) -> PolarsEngine:
    cfg = PolarsConfig(
        thread_pool_size=config.get("polars", {}).get("thread_pool_size"),
        enable_streaming=config.get("polars", {}).get("enable_streaming", True),
        storage_options=STORAGE_OPTIONS,
    )
    return PolarsEngine(config=cfg, logger=logger_manager.get_logger("polars_engine"))


def _build_duckdb_engine(config: Optional[Dict[str, Any]] = None) -> DuckDBEngine:
    duck_cfg = DuckDBConfig(
        database_path=":memory:",
        storage_options=STORAGE_OPTIONS,
    )
    return DuckDBEngine(config=duck_cfg, logger=logger_manager.get_logger("duckdb_engine"))


def _build_sqlmesh_engine(config: Optional[Dict[str, Any]] = None) -> SqlMeshEngine:
    cfg = SqlMeshConfig(
        project_path=SQLMESH_PATH,
        gateway=SQLMESH_GATEWAY,
    )
    return SqlMeshEngine(config=cfg, logger=logger_manager.get_logger("sqlmesh_engine"))


def _build_extractor(config: Dict[str, Any]) -> ExtractCophieu68:
    proj_params = config.get("project_params", config)
    cophieu_cfg = proj_params.get("sources", {}).get("cophieu68", {})
    pipeline_cfg = {
        "sources": {"cophieu68": cophieu_cfg},
        "http": proj_params.get("http", {"delay_seconds": 0.5, "timeout_seconds": 30}),
    }
    return ExtractCophieu68(pipeline_config=pipeline_cfg, pipeline_logger=logger_manager.get_logger("extractor"))


def _build_cleansing_rules(symbol: str) -> CleansingRuleSet:
    ruleset = CleansingRuleSet(table_name="stock_prices")

    def symbol_not_null(rec: Dict[str, Any]):
        val = rec.get("symbol")
        if not val:
            return False, f"[DQ] symbol is null for record: {rec}"
        return True, ""

    def positive_close(rec: Dict[str, Any]):
        try:
            close = float(rec.get("close_price") or rec.get("close") or 0)
            if close <= 0:
                return False, f"[DQ] close_price <= 0: {close}"
        except (TypeError, ValueError):
            return False, f"[DQ] close_price không parse được: {rec.get('close_price')}"
        return True, ""

    def non_negative_volume(rec: Dict[str, Any]):
        try:
            vol = float(rec.get("volume") or 0)
            if vol < 0:
                return False, f"[DQ] volume < 0: {vol}"
        except (TypeError, ValueError):
            pass
        return True, ""

    ruleset.add_rule(symbol_not_null)
    ruleset.add_rule(positive_close)
    ruleset.add_rule(non_negative_volume)
    return ruleset


class BronzePolarsIngester:
    def __init__(self, engine: PolarsEngine, table_name: str = "stock_prices", base_path: str = LAKEHOUSE_BASE):
        self.engine = engine
        self.table_name = table_name
        self.base_path = base_path
        self.logger = engine.logger

    def _profile(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not records:
            return {"total_rows": 0, "columns": []}
        sample = records[0]
        cols = list(sample.keys())
        total = len(records)
        null_counts = {col: sum(1 for r in records if r.get(col) is None) for col in cols}
        return {"total_rows": total, "columns": cols, "null_counts": null_counts}

    def _pre_evaluate(
        self,
        records: List[Dict[str, Any]],
        cleansing: CleansingRuleSet,
        run_id: str,
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        clean: List[Dict[str, Any]] = []
        rejects: List[Dict[str, Any]] = []
        for rec in records:
            valid, messages = cleansing.apply(rec)
            if valid:
                clean.append(rec)
            else:
                rec["_dq_status"] = "REJECT"
                rec["_dq_errors"] = "; ".join(messages)
                rec["_run_id"] = run_id
                rejects.append(rec)
        return clean, rejects

    def process(
        self,
        raw_records: List[Dict[str, Any]],
        batch_id: str,
        run_id: str,
        symbol: str,
        cleansing: Optional[CleansingRuleSet] = None,
    ) -> Dict[str, Any]:
        import polars as pl

        self.logger.info(f"[BronzeIngester:{symbol}] Start processing {len(raw_records)} records.")
        profile = self._profile(raw_records)
        if cleansing:
            clean_records, reject_records = self._pre_evaluate(raw_records, cleansing, run_id)
        else:
            clean_records = raw_records
            reject_records = []

        if not clean_records:
            self.logger.warning(f"[BronzeIngester:{symbol}] No records passed DQ checks.")
            return {
                "saved_path": None,
                "reject_path": None,
                "stats": {**profile, "clean": 0, "rejects": len(reject_records)},
            }

        df = pl.DataFrame(clean_records)
        if "symbol" not in df.columns:
            df = df.with_columns(pl.lit(symbol.upper()).alias("symbol"))

        df = df.with_columns([
            pl.lit(batch_id).alias("_batch_id"),
            pl.lit(run_id).alias("_run_id"),
            pl.lit(datetime.now(timezone.utc).isoformat()).alias("_ingest_timestamp"),
        ])

        bronze_path = f"{self.base_path}/bronze/{self.table_name}/"
        # [FIX] Sửa lỗi TypeError: unexpected keyword argument 'storage_options'.
        # Polars write_parquet cần storage_options khi ghi vào S3.
        # Chúng ta sẽ gọi trực tiếp hàm của DataFrame thay vì qua engine để đảm bảo tham số đúng.
        df.write_parquet(
            bronze_path, partition_by=["ingest_date"], storage_options=self.engine.config.storage_options
        )
        saved_path = bronze_path # Đường dẫn đã bao gồm partition

        reject_path = None
        if reject_records:
            reject_df = pl.DataFrame(reject_records)
            reject_path = f"{self.base_path}/bronze/_rejects/{self.table_name}/"
            self.engine.write_parquet(df=reject_df, target_path=reject_path)

        return {
            "saved_path": saved_path,
            "reject_path": reject_path,
            "stats": {**profile, "clean": len(clean_records), "rejects": len(reject_records)},
        }


class SilverProcessor:
    def __init__(
        self,
        duckdb_engine: DuckDBEngine,
        polars_engine: PolarsEngine,
        base_path: str = LAKEHOUSE_BASE,
    ) -> None:
        self.duck = duckdb_engine
        self.polars = polars_engine
        self.sk_gen = SurrogateKeyGenerator(prefix="STK_", key_length=32)
        self.base = base_path
        self.logger = polars_engine.logger

    def transform_stock_prices(
        self,
        target_date: str,
        run_id: str,
        dedup_strategy: DeduplicationStrategy = DeduplicationStrategy.KEEP_LAST,
    ) -> Dict[str, Any]:
        import polars as pl

        bronze_glob = f"{self.base}/bronze/stock_prices/ingest_date={target_date}/*.parquet"
        self.logger.info(f"[SilverProcessor] Reading bronze data from {bronze_glob}")

        df_lf = self.duck.query_to_polars(f"SELECT * FROM read_parquet('{bronze_glob}')")
        df = df_lf.collect()

        if df.is_empty():
            self.logger.warning(f"[SilverProcessor] No bronze data for date {target_date}")
            return {"rows_in": 0, "rows_out": 0, "silver_path": None}

        rows_in = len(df)
        self.logger.info(f"[SilverProcessor] Read {rows_in} rows from bronze.")

        import pandas as pd
        df_pd = df.to_pandas()
        dedup_engine = DeduplicationEngine(
            keys=["symbol", "date"],
            strategy=dedup_strategy,
            tiebreaker_col="ingest_timestamp",
        )
        deduped_pd = dedup_engine.deduplicate(df_pd)
        dedup_stats = dedup_engine.last_stats
        self.logger.info(f"[SilverProcessor] Dedup stats: {dedup_stats}")

        df = pl.from_pandas(deduped_pd)
        df = df.with_columns(
            pl.col("symbol")
            .map_elements(lambda sym: self.sk_gen.hash_key(sym), return_dtype=pl.Utf8)
            .alias("stock_sk")
        )

        now_ts = datetime.now(timezone.utc).isoformat()
        df = df.with_columns([
            pl.lit(run_id).alias("_silver_run_id"),
            pl.lit(now_ts).alias("_silver_processed_at"),
        ])

        silver_path = f"{self.base}/silver/fact_stock_price/"
        self.polars.write_parquet(df=df, target_path=silver_path, partition_by=["ingest_date"])
        rows_out = len(df)

        return {
            "rows_in": rows_in,
            "rows_out": rows_out,
            "dedup_stats": dedup_stats,
            "silver_path": silver_path,
        }


class GoldProcessor:
    def __init__(self, sqlmesh_engine: SqlMeshEngine, duck_engine: DuckDBEngine) -> None:
        self.sqlmesh = sqlmesh_engine
        self.duck = duck_engine
        self.logger = sqlmesh_engine.logger

    def run_gold_models(
        self,
        environment: str = "prod",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        run_audits: bool = True,
    ) -> Dict[str, Any]:
        self.logger.info(f"[GoldProcessor] Running SQLMesh models env={environment} start={start_date} end={end_date}")
        result: Dict[str, Any] = {"environment": environment}

        try:
            plan_result = self.sqlmesh.plan(environment=environment)
            result["plan"] = str(plan_result)
            self.logger.info("[GoldProcessor] SQLMesh plan complete")
        except Exception as exc:
            self.logger.warning(f"[GoldProcessor] SQLMesh plan warning: {exc}")

        self.sqlmesh.run(environment=environment, start=start_date, end=end_date)
        result["run_status"] = "SUCCESS"
        self.logger.info("[GoldProcessor] SQLMesh run complete")

        if run_audits:
            self.sqlmesh.audit()
            result["audits"] = "PASSED"
            self.logger.info("[GoldProcessor] SQLMesh audits complete")

        return result


class ServingSyncProcessor:
    def __init__(self, duck_engine: DuckDBEngine, base_path: str = LAKEHOUSE_BASE):
        self.duck = duck_engine
        self.base = base_path
        self.logger = duck_engine.logger

    def _pg_conn_string(self) -> str:
        return f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

    def sync_gold_to_postgres(self, mart_table: str = "mart_kpi_daily", target_date: Optional[str] = None) -> Dict[str, Any]:
        if not PG_USER:
            self.logger.warning("[ServingSync] PostgreSQL credentials not configured")
            return {"skipped": True, "reason": "missing_pg_credentials"}

        gold_glob = f"{self.base}/gold/{mart_table}/"
        if target_date:
            gold_glob += f"date={target_date}/*.parquet"
        else:
            gold_glob += "**/*.parquet"

        self.logger.info(f"[ServingSync] Syncing {mart_table} from {gold_glob} to PostgreSQL")
        con = self.duck.connection
        con.execute("INSTALL postgres; LOAD postgres;")
        pg_conn = self._pg_conn_string()
        con.execute(f"ATTACH '{pg_conn}' AS pg_serving (TYPE POSTGRES, READ_WRITE);")
        con.execute(
            f"""
            INSERT INTO pg_serving.gold.{mart_table}
                SELECT * FROM read_parquet('{gold_glob}', hive_partitioning=true)
            ON CONFLICT DO NOTHING;
            """
        )

        return {"status": "SUCCESS", "mart_table": mart_table}

    def optimize_gold_tables(self) -> None:
        self.logger.info("[ServingSync] OPTIMIZE placeholder called")


class BronzeExecutor:
    def __init__(self, context: ExecutionContext, config: Dict[str, Any]):
        self.context = context
        self.config = config
        self.logger = context.logger

    def execute(self) -> Dict[str, Any]:
        self.logger.info(f"[BronzeExecutor] Starting bronze phase for symbols={self.context.symbols}")
        result = {
            "phase": "bronze",
            "symbols_processed": 0,
            "rows_ingested": 0,
            "rows_rejected": 0,
            "errors": 0,
        }

        polars_engine = _build_polars_engine(self.config)
        bronze_ingester = BronzePolarsIngester(engine=polars_engine, base_path=LAKEHOUSE_BASE)
        extractor = _build_extractor(self.config)

        for symbol in self.context.symbols:
            batch_id = make_batch_id(symbol)
            self.logger.info(f"[BronzeExecutor] Processing symbol={symbol} batch_id={batch_id}")

            try:
                trading_data = extractor.crawl_trading_data(symbol=symbol, page=1)
                if not trading_data or not trading_data.get("records"):
                    self.logger.warning(f"[BronzeExecutor] No trading records for {symbol}")
                    continue

                raw_records = trading_data["records"]
                # [FIX] Thêm 'symbol' vào mỗi record TRƯỚC KHI kiểm tra chất lượng dữ liệu.
                # Dữ liệu gốc từ extractor không chứa cột symbol trong mỗi record.
                for record in raw_records:
                    record['symbol'] = symbol.upper()

                cleansing = _build_cleansing_rules(symbol)
                ingestion_result = bronze_ingester.process(
                    raw_records=raw_records,
                    batch_id=batch_id,
                    run_id=self.context.run_id,
                    symbol=symbol,
                    cleansing=cleansing,
                )

                clean_count = ingestion_result["stats"]["clean"]
                reject_count = ingestion_result["stats"]["rejects"]
                result["symbols_processed"] += 1
                result["rows_ingested"] += clean_count
                result["rows_rejected"] += reject_count

                if reject_count > 0:
                    self.context.error_log.add(
                        ErrorLevel.WARNING,
                        f"{reject_count} rejected records in bronze ingestion for {symbol}",
                    )

                self.context.metadata_repo.log_lineage(
                    run_id=self.context.run_id,
                    source_layer="external",
                    source_table=f"cophieu68_{symbol}",
                    target_layer="bronze",
                    target_table="bronze_stock_prices",
                    operation="APPEND",
                    rows_affected=clean_count,
                )

            except Exception as exc:
                self.logger.error(f"[BronzeExecutor] Error for {symbol}: {exc}")
                self.context.error_log.add(
                    ErrorLevel.ERROR,
                    f"Bronze ingestion failed for {symbol}: {exc}",
                )
                result["errors"] += 1

        self.logger.info(f"[BronzeExecutor] Completed bronze phase: rows_ingested={result['rows_ingested']}")
        return result


class SilverExecutor:
    def __init__(self, context: ExecutionContext, config: Dict[str, Any]):
        self.context = context
        self.config = config
        self.logger = context.logger

    def execute(self) -> Dict[str, Any]:
        self.logger.info(f"[SilverExecutor] Starting silver phase for date={self.context.target_date}")
        result = {
            "phase": "silver",
            "rows_in": 0,
            "rows_out": 0,
            "errors": 0,
        }

        duck_engine = _build_duckdb_engine(self.config)
        polars_engine = _build_polars_engine(self.config)
        processor = SilverProcessor(
            duckdb_engine=duck_engine,
            polars_engine=polars_engine,
            base_path=LAKEHOUSE_BASE,
        )

        try:
            silver_result = processor.transform_stock_prices(
                target_date=self.context.target_date,
                run_id=self.context.run_id,
                dedup_strategy=DeduplicationStrategy.KEEP_LAST,
            )
            result.update(silver_result)

            if silver_result.get("rows_out", 0) > 0:
                self.context.metadata_repo.log_lineage(
                    run_id=self.context.run_id,
                    source_layer="bronze",
                    source_table="bronze_stock_prices",
                    target_layer="silver",
                    target_table="silver_fact_stock_price",
                    operation="MERGE",
                    rows_affected=silver_result.get("rows_out", 0),
                )

        except Exception as exc:
            self.logger.error(f"[SilverExecutor] Error: {exc}")
            self.context.error_log.add(
                ErrorLevel.ERROR,
                f"Silver transformation failed: {exc}",
            )
            result["errors"] += 1

        finally:
            duck_engine.close()

        self.logger.info(f"[SilverExecutor] Completed silver phase with rows_out={result.get('rows_out', 0)}")
        return result


class GoldExecutor:
    def __init__(self, context: ExecutionContext, config: Dict[str, Any]):
        self.context = context
        self.config = config
        self.logger = context.logger

    def execute(self) -> Dict[str, Any]:
        self.logger.info(f"[GoldExecutor] Starting gold phase for date={self.context.target_date}")
        result = {
            "phase": "gold",
            "run_status": "UNKNOWN",
            "audits": None,
            "errors": 0,
        }

        sqlmesh_engine = _build_sqlmesh_engine(self.config)
        duck_engine = _build_duckdb_engine(self.config)
        processor = GoldProcessor(sqlmesh_engine=sqlmesh_engine, duck_engine=duck_engine)

        try:
            gold_result = processor.run_gold_models(
                environment=self.config.get("environment", "prod"),
                start_date=self.context.target_date,
                end_date=self.context.target_date,
                run_audits=True,
            )
            result.update(gold_result)

            if gold_result.get("run_status") == "SUCCESS":
                self.context.metadata_repo.log_lineage(
                    run_id=self.context.run_id,
                    source_layer="silver",
                    source_table="silver_fact_stock_price",
                    target_layer="gold",
                    target_table="gold_stock_kpis",
                    operation="OVERWRITE",
                    rows_affected=gold_result.get("audits") == "PASSED" and 1 or 0,
                )

        except Exception as exc:
            self.logger.error(f"[GoldExecutor] Error: {exc}")
            self.context.error_log.add(
                ErrorLevel.ERROR,
                f"Gold modeling failed: {exc}",
            )
            result["errors"] += 1

        finally:
            duck_engine.close()

        self.logger.info(f"[GoldExecutor] Completed gold phase with status={result.get('run_status')}")
        return result


class ServingExecutor:
    def __init__(self, context: ExecutionContext, config: Dict[str, Any]):
        self.context = context
        self.config = config
        self.logger = context.logger

    def execute(self, mart_tables: Optional[List[str]] = None) -> Dict[str, Any]:
        self.logger.info(f"[ServingExecutor] Starting serving phase for date={self.context.target_date}")
        result = {
            "phase": "serving",
            "tables_synced": 0,
            "errors": 0,
            "details": {},
        }

        mart_tables = mart_tables or ["mart_kpi_daily"]
        duck_engine = _build_duckdb_engine(self.config)
        sync_proc = ServingSyncProcessor(duck_engine=duck_engine, base_path=LAKEHOUSE_BASE)

        try:
            for table in mart_tables:
                sync_result = sync_proc.sync_gold_to_postgres(
                    mart_table=table,
                    target_date=self.context.target_date,
                )
                result["details"][table] = sync_result
                if sync_result.get("status") == "SUCCESS":
                    result["tables_synced"] += 1
        except Exception as exc:
            self.logger.error(f"[ServingExecutor] Error: {exc}")
            self.context.error_log.add(
                ErrorLevel.ERROR,
                f"Serving sync failed: {exc}",
            )
            result["errors"] += 1
        finally:
            duck_engine.close()

        sync_proc.optimize_gold_tables()
        self.logger.info(f"[ServingExecutor] Completed serving phase tables_synced={result['tables_synced']}")
        return result


class MasterPipelineOrchestrator:
    def __init__(self, config_path: Optional[str] = None):
        self.logger = logger_manager.get_logger(__name__)
        self.config_manager = ConfigurationManager(config_path, self.logger)
        self.config = self.config_manager.load()
        self.prefect_config = self.config_manager.load_prefect_config()
        self.metadata_repo = MetadataRepository(delta_backend=None, logger=self.logger, in_memory=True)

    def _resolve_symbols(self, phase: ExecutionPhase, symbols: Optional[List[str]]) -> List[str]:
        # Ưu tiên 1: Sử dụng symbols được cung cấp qua CLI, nếu chúng khác với danh sách mặc định.
        # Điều này cho phép người dùng ghi đè danh sách mặc định.
        if symbols and symbols != DEFAULT_SYMBOLS:
            return [symbol.strip().upper() for symbol in symbols if symbol and symbol.strip()]

        # Ưu tiên 2: Tự động lấy danh sách symbols từ extractor.
        self.logger.info("[ORCHESTRATOR] Discovering all symbols from Cophieu68 extractor...")
        extractor = _build_extractor(self.config)
        discovered_symbols: List[str] = []

        for market_type in CRAWL_MARKET_LIST_CONFIG.keys():
            try:
                result = extractor.crawl_market_list(market_type)
                if isinstance(result, dict) and result.get("symbols"):
                    discovered_symbols.extend(result["symbols"])
                    self.logger.info(f"Discovered {len(result['symbols'])} symbols from market {market_type}")
            except Exception as exc:
                self.logger.warning(f"[ORCHESTRATOR] Failed to discover symbols for market {market_type}: {exc}")

        normalized_symbols = sorted({symbol.strip().upper() for symbol in discovered_symbols if isinstance(symbol, str) and symbol.strip()})
        if normalized_symbols:
            self.logger.info(f"[ORCHESTRATOR] Total discovered unique symbols: {len(normalized_symbols)}")
            return normalized_symbols

        # Ưu tiên 3: Nếu không thể lấy tự động, sử dụng danh sách mặc định làm phương án dự phòng.
        self.logger.warning("[ORCHESTRATOR] Could not discover symbols. Falling back to DEFAULT_SYMBOLS.")
        return [symbol.strip().upper() for symbol in DEFAULT_SYMBOLS if symbol and symbol.strip()]

    def execute(
        self,
        phase: ExecutionPhase,
        symbols: Optional[List[str]] = None,
        backend: str = "polars",
        target_date: Optional[str] = None,
        environment: str = "prod",
        dry_run: bool = False,
        mart_tables: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        symbols = self._resolve_symbols(phase, symbols)
        backend_enum = ProcessingBackend(backend)
        target_date = target_date or date.today().isoformat()
        run_id = make_run_id()

        context = ExecutionContext(
            run_id=run_id,
            phase=phase,
            symbols=symbols,
            backend=backend_enum,
            target_date=target_date,
            dry_run=dry_run,
            metadata_repo=self.metadata_repo,
            error_log=ErrorEventLog(run_id=run_id, job_name=f"etl_{phase.value}"),
            logger=self.logger,
        )

        self.logger.info(
            f"[ORCHESTRATOR] Starting pipeline phase={phase.value} run_id={run_id} target_date={target_date} symbols={symbols} backend={backend}"
        )

        try:
            self.metadata_repo.start_run(
                job_name=f"master_etl_{phase.value}",
                layer=phase.value,
                table_name="all_tables",
                run_id=run_id,
            )

            if dry_run or phase == ExecutionPhase.VALIDATE:
                result = self._validate_pipeline(context)
            else:
                result = self._run_phases(context, mart_tables, environment)

            self.metadata_repo.end_run(run_id, status="SUCCESS", rows_written=None)
            context.status = "SUCCESS"
        except Exception as exc:
            self.logger.error(f"[ORCHESTRATOR] Pipeline failed: {exc}")
            self.metadata_repo.end_run(run_id, status="FAILED", error_message=str(exc))
            context.error_log.add(ErrorLevel.FATAL, f"Pipeline execution failed: {exc}")
            context.status = "FAILED"
            context.error_message = str(exc)
            result = {
                "run_id": run_id,
                "status": "FAILED",
                "error": str(exc),
            }

        context.end_time = datetime.now(timezone.utc)
        self.metadata_repo.log_run_summary(run_id)

        result["duration_seconds"] = context.duration_seconds()
        result["error_events"] = [e.to_dict() for e in context.error_log.events]
        return result

    def _validate_pipeline(self, context: ExecutionContext) -> Dict[str, Any]:
        self.logger.info("[VALIDATION] Running configuration validation")
        validation = self.config_manager.validate()
        if not validation["is_valid"]:
            return {
                "run_id": context.run_id,
                "status": "VALIDATION_FAILED",
                "errors": validation["errors"],
            }
        return {
            "run_id": context.run_id,
            "status": "VALIDATION_PASSED",
            "phase": context.phase.value,
            "symbols": context.symbols,
            "backend": context.backend.value,
            "target_date": context.target_date,
        }

    def _run_phases(
        self,
        context: ExecutionContext,
        mart_tables: Optional[List[str]],
        environment: str,
    ) -> Dict[str, Any]:
        phases: List[ExecutionPhase] = []
        if context.phase == ExecutionPhase.FULL:
            phases = [ExecutionPhase.BRONZE, ExecutionPhase.SILVER, ExecutionPhase.GOLD, ExecutionPhase.SERVING]
        else:
            phases = [context.phase]

        results: Dict[str, Any] = {"run_id": context.run_id, "phases_executed": [], "phase_results": {}}

        for phase in phases:
            phase_result: Dict[str, Any] = {}
            if phase == ExecutionPhase.BRONZE:
                phase_result = BronzeExecutor(context, self.config).execute()
            elif phase == ExecutionPhase.SILVER:
                phase_result = SilverExecutor(context, self.config).execute()
            elif phase == ExecutionPhase.GOLD:
                phase_result = GoldExecutor(context, self.config).execute()
            elif phase == ExecutionPhase.SERVING:
                phase_result = ServingExecutor(context, self.config).execute(mart_tables)

            results["phase_results"][phase.value] = phase_result
            results["phases_executed"].append(phase.value)

        results["status"] = "COMPLETED"
        results["phase_count"] = len(results["phases_executed"])
        results["metrics"] = {
            "total_errors": len(context.error_log.events),
            "error_summary": context.error_log.summary(),
        }
        return results


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Unified Lakehouse ETL Pipeline Orchestrator")
    parser.add_argument(
        "phase",
        choices=[p.value for p in ExecutionPhase],
        help="Pipeline phase to execute",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=DEFAULT_SYMBOLS,
        help="Stock symbols to process. If omitted, the pipeline will attempt to discover and process all symbols from all markets.",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Processing date YYYY-MM-DD (default: today)",
    )
    parser.add_argument(
        "--backend",
        choices=[b.value for b in ProcessingBackend],
        default="polars",
        help="Processing backend",
    )
    parser.add_argument(
        "--env",
        default="prod",
        choices=["prod", "dev", "staging"],
        help="Environment for SQLMesh models",
    )
    parser.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG_PATH),
        help="Path to pipeline config YAML",
    )
    parser.add_argument(
        "--marts",
        nargs="*",
        default=None,
        help="Gold mart tables to sync in serving phase",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configuration and skip execution",
    )
    parser.add_argument(
        "--output",
        choices=["json", "text", "summary"],
        default="summary",
        help="Result output format",
    )
    return parser.parse_args()


def format_result(result: Dict[str, Any], format_type: str = "summary") -> str:
    if format_type == "json":
        return json.dumps(result, indent=2, default=str)
    if format_type == "text":
        lines = ["=" * 72, "ETL Pipeline Execution Result", "=" * 72]
        lines.append(f"Run ID: {result.get('run_id', 'N/A')}")
        lines.append(f"Status: {result.get('status', 'UNKNOWN')}")
        lines.append(f"Phase: {result.get('phase', 'N/A')}")
        lines.append(f"Duration: {result.get('duration_seconds', 0):.1f}s")
        if result.get('phases_executed'):
            lines.append(f"Phases executed: {', '.join(result['phases_executed'])}")
        if result.get('error_events'):
            lines.append("Errors:")
            for err in result['error_events'][:5]:
                lines.append(f"  - [{err.get('error_level')}] {err.get('error_message')}")
        if result.get('error'):
            lines.append(f"Error: {result['error']}")
        lines.append("=" * 72)
        return "\n".join(lines)
    return f"{result.get('status', 'UNKNOWN')} | {result.get('phase', 'N/A')} | {result.get('duration_seconds', 0):.1f}s"


def main() -> None:
    args = parse_arguments()
    logger_manager.configure_from_project_config(args.config)
    orchestrator = MasterPipelineOrchestrator(config_path=args.config)
    result = orchestrator.execute(
        phase=ExecutionPhase(args.phase),
        symbols=args.symbols,
        backend=args.backend,
        target_date=args.date,
        environment=args.env,
        dry_run=args.dry_run,
        mart_tables=args.marts,
    )
    print(format_result(result, args.output))
    if result.get("status") not in ("SUCCESS", "COMPLETED", "VALIDATION_PASSED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
