"""
Master Lakehouse Pipeline - Cophieu68 ETL
==========================================
Kiến trúc: Bronze → Silver → Gold → Serving (PostgreSQL)
Framework: Lakehouse_Project_Framework.docx

Mapping subsystems theo framework:
  Phase 2 (Bronze)  → Subsystems 1 (Profiling), 3 (Extract), 4 (Archive/Lineage), 6 (Audit)
  Phase 3 (Silver)  → Subsystems 5 (Cleansing), 7 (Deduplication), 9 (SCD), 10 (SurrogateKey), 34 (Metadata)
  Phase 4 (Gold)    → Subsystems 11 (SK Pipeline), 14 (Fact Builder), 20 (Aggregate Builder)
  Phase 5 (Serving) → Subsystems 19 (Fact Provider), 27 (Sort/Optimize), 29 (Lineage)
  Cross-cutting     → Subsystems 21 (Scheduler), 22 (Job), 26 (Monitor), 29 (Escalation)

Stack:
  - Polars        : Bronze ingestion (batch, single-node, < 10M rows/day)
  - DuckDB        : Silver/Gold transform, bridge Parquet → PostgreSQL
  - SQLMesh       : Versioned SQL models (Silver → Gold incremental)
  - Delta Lake    : Storage format (Parquet + transaction log)
  - Prefect       : Orchestration, retries, observability
  - MinIO/S3      : Object storage (lakehouse bucket)
  - PostgreSQL    : Serving layer (BI / API)

Quy tắc công cụ (từ framework):
  - Polars nếu daily rows < 10M và single-node RAM đủ
  - DuckDB cho ad-hoc analytics và bridge Parquet → Postgres
  - Spark khi cần cluster parallelism (> 50M rows hoặc heavy joins)
  - SQLMesh cho SQL-first modeling và test tự động

Tác giả: ETL Data Engineering
Ngày:    2026-05-13
"""

from __future__ import annotations

import os
import sys
import uuid
import logging
import yaml
from enum import Enum
from pathlib import Path
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Dict, Generator, List, Optional

from dotenv import load_dotenv
from platforms.orchestration.prefect.flows import prefect_orchestra_etl
from platforms.processing.base_processing_subsystem.subsystem5_and_30_error_event_schema_and_escalate import (
    ErrorEventLog, ErrorEvent, ErrorLevel
)
# ---------------------------------------------------------------------------
# Project root resolution
# ---------------------------------------------------------------------------
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# ---------------------------------------------------------------------------
# Internal imports
# ---------------------------------------------------------------------------
from shared.logger.python_main_logger import logger_manager

from platforms.orchestration.prefect.flows.prefect_orchestra_etl import PrefectETLPipelineConfig

# Ingestion
from platforms.ingestion.cophieu68.extract.extract_cophieu68 import ExtractCophieu68

# Processing engines
from platforms.processing.polars.polars_engine import PolarsConfig, PolarsEngine
from platforms.processing.polars.base_polars_processor import BasePolarsProcessor
from platforms.processing.duckdb.duckdb_engine import DuckDBConfig, DuckDBEngine
from platforms.processing.sqlmesh.sqlmesh_engine import SqlMeshConfig, SqlMeshEngine

# Subsystems
from platforms.processing.base_processing_subsystem.subsystem1_data_profiling import (
    DataProfiler,
    DataQualityRecord,
)
from platforms.processing.base_processing_subsystem.subsystem4_data_quality_pre_evaluate import (
    CleansingRuleSet,
)
from platforms.processing.base_processing_subsystem.subsystem7_deduplication import (
    DeduplicationEngine,
    DeduplicationStrategy,
)
from platforms.processing.base_processing_subsystem.subsystem9_and_25_scd_manage_and_version import (
    SCD2Result,
)
from platforms.processing.base_processing_subsystem.subsystem10_surrogate_key_generator import (
    SurrogateKeyGenerator,
)
from platforms.processing.base_processing_subsystem.subsystem29_data_lineage import (
    LineageRecord,
    LineageTracker,
)
from platforms.processing.base_processing_subsystem.subsystem34_metadata_repo import (
    MetadataRepository,
)

# Storage
from platforms.storage.lake_storage.delta_lake_storage import DeltaLakeStorageBackend

load_dotenv()

# ===========================================================================
# CONSTANTS & ENV
# ===========================================================================

# Project setup
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

_LAKEHOUSE_BASE   = os.getenv("LAKEHOUSE_BASE_PATH", "s3a://lakehouse")


_S3_ENDPOINT      = os.getenv("S3_ENDPOINT", "http://localhost:9000")
_S3_KEY           = os.getenv("MINIO_ROOT_USER", "minioadmin")
_S3_SECRET        = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin_secure_123")

_PG_USER          = os.getenv("POSTGRES_USER", "")
_PG_PASSWORD      = os.getenv("POSTGRES_PASSWORD", "")
_PG_HOST          = os.getenv("POSTGRES_HOST", "localhost")
_PG_PORT          = os.getenv("POSTGRES_PORT", "5432")
_PG_DB            = os.getenv("POSTGRES_DB", "etl_project")

_SQLMESH_PATH     = os.path.join(_PROJECT_ROOT, "sqlmesh")
_SQLMESH_GATEWAY  = os.getenv("SQLMESH_GATEWAY", "local_duckdb")

# Default symbols if none provided
# DEFAULT_SYMBOLS = ["FPT", "VNM", "HPG", "MBB", "SSI"]
DEFAULT_CONFIG_PATH = (
    PROJECT_ROOT / "platforms" / "orchestration" / "prefect" / "config" / "cophieu68_config.yaml"
)
# ---------------------------------------------------------------------------
# Storage options (shared across Polars / DuckDB)
# ---------------------------------------------------------------------------
_STORAGE_OPTIONS: Dict[str, str] = {
    "endpoint_url":        _S3_ENDPOINT,
    "aws_access_key_id":    _S3_KEY,
    "aws_secret_access_key": _S3_SECRET,
}


# ===========================================================================
# HELPER: run_id & batch_id generators  (Subsystem 22 – Job Scheduler)
# ===========================================================================

def _make_batch_id(symbol: str) -> str:
    """Tạo batch_id dạng: batch_<SYMBOL>_<YYYYMMDDHHMMSS>_<uuid4[:8]>"""
    ts   = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    uid  = uuid.uuid4().hex[:8]
    return f"batch_{symbol.upper()}_{ts}_{uid}"


def _make_run_id() -> str:
    """Tạo pipeline run_id duy nhất (Subsystem 22)."""
    ts  = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    uid = uuid.uuid4().hex[:6]
    return f"run_{ts}_{uid}"


# ===========================================================================
# CONTEXT MANAGER: Metadata Repository lifecycle  (Subsystem 34)
# ===========================================================================

@contextmanager
def _meta_run_ctx(
    repo: MetadataRepository,
    job_name: str,
    layer: str,
) -> Generator[str, None, None]:
    """
    Context manager tự động start/end một ETL run trong MetadataRepository.
    Bắt exception và ghi status=FAILED nếu có lỗi.
    """
    run_id = repo.start_run(job_name, layer=layer)
    try:
        yield run_id
        repo.end_run(run_id, status="SUCCESS")
    except Exception as exc:
        repo.end_run(run_id, status="FAILED", error=str(exc))
        raise

# ===========================================================================
# ENUMS & TYPES
# ===========================================================================

class ExecutionPhase(str, Enum):
    """Pipeline execution phases."""
    BRONZE = "bronze"      # Ingestion
    SILVER = "silver"      # Transform/Cleanse
    GOLD = "gold"          # KPI/Aggregation
    SERVING = "serving"    # DB Sync
    FULL = "full"          # All phases
    VALIDATE = "validate"  # Config validation only


class ProcessingBackend(str, Enum):
    """Data processing backends."""
    POLARS = "polars"
    SPARK = "spark"
    SQLMESH = "sqlmesh"
    DUCKDB = "duckdb"
    DBT = "dbt"


@dataclass
class ExecutionContext:
    """Execution context for pipeline run."""
    run_id: str
    phase: ExecutionPhase
    symbols: List[str]
    backend: ProcessingBackend
    target_date: Optional[str] = None
    dry_run: bool = False
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    end_time: Optional[datetime] = None
    status: str = "RUNNING"
    error_message: Optional[str] = None

    metadata_repo: Optional[MetadataRepository] = None
    error_log: Optional[ErrorEventLog] = None
    logger: Optional[logging.Logger] = None

    def duration_seconds(self) -> float:
        """Calculate execution duration."""
        end = self.end_time or datetime.now(timezone.utc)
        return (end - self.start_time).total_seconds()


# ===========================================================================
# CONFIGURATION LOADER
# ===========================================================================

class ConfigurationManager:
    """Load and manage pipeline configuration."""

    def __init__(self, config_path: Optional[str] = None, logger: Optional[logging.Logger] = None):
        self.logger = logger or logger_manager.get_logger(__name__)
        self.config_path = Path(config_path or DEFAULT_CONFIG_PATH)
        self.config: Dict[str, Any] = {}
        self.prefect_config: Optional[PrefectETLPipelineConfig] = None

    def load(self) -> Dict[str, Any]:
        """Load configuration from YAML."""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f) or {}
            self.logger.info(f"[CONFIG] Loaded from {self.config_path}")
            return self.config
        except FileNotFoundError:
            self.logger.error(f"[CONFIG] File not found: {self.config_path}")
            return {}
        except Exception as exc:
            self.logger.error(f"[CONFIG] Error loading: {exc}")
            return {}

    def load_prefect_config(self) -> Optional[PrefectETLPipelineConfig]:
        """Load Prefect config object."""
        try:
            self.prefect_config = PrefectETLPipelineConfig.from_yaml(str(self.config_path))
            self.logger.info("[CONFIG] Prefect config loaded successfully")
            return self.prefect_config
        except Exception as exc:
            self.logger.error(f"[CONFIG] Error loading Prefect config: {exc}")
            return None

    def validate(self) -> Tuple[bool, List[str]]:
        """Validate configuration."""
        errors = []

        if not self.config:
            errors.append("Configuration is empty")
            return False, errors

        # Check required sections
        required_sections = ["sources"]
        for section in required_sections:
            if section not in self.config:
                errors.append(f"Missing required section: {section}")

        return len(errors) == 0, errors

# ===========================================================================
# ENGINE FACTORIES
# ===========================================================================

def get_polars_engine(config: Dict[str, Any]) -> Any:
    """Initialize Polars engine."""
    try:
        from platforms.processing.polars.polars_engine import PolarsEngine, PolarsConfig
        
        cfg = PolarsConfig(
            thread_pool_size=config.get("polars", {}).get("thread_pool_size"),
            enable_streaming=config.get("polars", {}).get("enable_streaming", True),
            storage_options=_STORAGE_OPTIONS,
        )
        return PolarsEngine(config=cfg, logger=logger_manager.get_logger("polars_engine"))
    except ImportError:
        raise ImportError("Polars processing engine not available")


def get_duckdb_engine(config: Dict[str, Any]) -> Any:
    """Initialize DuckDB engine."""
    try:
        from platforms.processing.duckdb.duckdb_engine import DuckDBEngine, DuckDBConfig
        
        cfg = DuckDBConfig(
            database_path=":memory:",
            storage_options=_STORAGE_OPTIONS,
        )
        return DuckDBEngine(config=cfg, logger=logger_manager.get_logger("duckdb_engine"))
    except ImportError:
        raise ImportError("DuckDB processing engine not available")


def get_sqlmesh_engine(config: Dict[str, Any]) -> Any:
    """Initialize SQLMesh engine."""
    try:
        from platforms.processing.sqlmesh.sqlmesh_engine import SqlMeshEngine, SqlMeshConfig
        
        cfg = SqlMeshConfig(
            project_path=SQLMESH_PATH,
            gateway=SQLMESH_GATEWAY,
        )
        return SqlMeshEngine(config=cfg, logger=logger_manager.get_logger("sqlmesh_engine"))
    except ImportError:
        raise ImportError("SQLMesh processing engine not available")
# ===========================================================================
# POLARS BRONZE INGESTER  (BasePolarsProcessor use-case)
# ===========================================================================


class BronzePolarsIngester(BasePolarsProcessor):
    """
    Subsystem 3 (Extract) + Subsystem 6 (Audit) + Subsystem 1 (Profiling).

    Nhận list[dict] từ crawler, thêm audit metadata, ghi Parquet
    partitioned by ingest_date vào MinIO bronze layer.
    """

    def __init__(
        self,
        engine: PolarsEngine,
        table_name: str = "stock_prices",
        base_path: str = _LAKEHOUSE_BASE,
    ) -> None:
        super().__init__(engine)
        self.table_name = table_name
        self.base_path   = base_path

    # ------------------------------------------------------------------
    # Subsystem 1: Data Profiling (trả về stats để log vào Metadata)
    # ------------------------------------------------------------------
    def _profile(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Đơn giản: đếm row, null counts, schema columns."""
        if not records:
            return {"total_rows": 0, "columns": []}
        sample = records[0]
        cols   = list(sample.keys())
        total  = len(records)
        null_counts = {
            col: sum(1 for r in records if r.get(col) is None)
            for col in cols
        }
        return {
            "total_rows":   total,
            "columns":      cols,
            "null_counts":  null_counts,
        }

    # ------------------------------------------------------------------
    # Subsystem 4: Data Quality Pre-evaluation (trước khi ghi)
    # ------------------------------------------------------------------
    def _pre_evaluate(
        self,
        records:     List[Dict[str, Any]],
        cleansing:   CleansingRuleSet,
        run_id:      str,
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Áp dụng cleansing rules. Trả về (clean_records, reject_records).
        Reject records được ghi sang bronze/_rejects theo framework.
        """
        clean   = []
        rejects = []
        for rec in records:
            valid, messages = cleansing.apply(rec)
            if valid:
                clean.append(rec)
            else:
                rec["_dq_status"] = "REJECT"
                rec["_dq_errors"] = "; ".join(messages)
                rec["_run_id"]    = run_id
                rejects.append(rec)
        return clean, rejects

    # ------------------------------------------------------------------
    # process() – override abstract method
    # ------------------------------------------------------------------
    def process(
        self,
        raw_records:  List[Dict[str, Any]],
        batch_id:     str,
        run_id:       str,
        symbol:       str,
        cleansing:    Optional[CleansingRuleSet] = None,
    ) -> Dict[str, Any]:
        """
        1. Pre-evaluate / cleansing (Subsystem 4)
        2. Data profiling (Subsystem 1)
        3. Polars DataFrame → add audit metadata (Subsystem 6)
        4. Write Parquet partitioned (Bronze layer)
        5. Write rejects to bronze/_rejects (DQ framework)

        Returns: dict với paths và stats
        """
        import polars as pl

        logger = self.logger
        logger.info(f"[BronzeIngester:{symbol}] Bắt đầu xử lý {len(raw_records)} records.")

        # — Profiling trước (Subsystem 1) —
        profile = self._profile(raw_records)
        logger.info(
            f"[BronzeIngester:{symbol}] Profile: total={profile['total_rows']}, "
            f"cols={len(profile['columns'])}"
        )

        # — Pre-evaluate (Subsystem 4) —
        if cleansing:
            clean_records, reject_records = self._pre_evaluate(raw_records, cleansing, run_id)
            logger.info(
                f"[BronzeIngester:{symbol}] DQ pre-eval: "
                f"clean={len(clean_records)}, rejects={len(reject_records)}"
            )
        else:
            clean_records  = raw_records
            reject_records = []

        if not clean_records:
            logger.warning(f"[BronzeIngester:{symbol}] Không có records nào pass DQ check.")
            return {
                "saved_path":   None,
                "reject_path":  None,
                "stats":        {**profile, "clean": 0, "rejects": len(reject_records)},
            }

        # — Build DataFrame (Polars) —
        df = pl.DataFrame(clean_records)
        df = self.add_audit_metadata(df, batch_id=batch_id, source_system="cophieu68")
        # Thêm symbol nếu chưa có
        if "symbol" not in df.columns:
            df = df.with_columns(pl.lit(symbol.upper()).alias("symbol"))

        # — Ghi Bronze layer —
        bronze_path = f"{self.base_path}/bronze/{self.table_name}/"
        saved_path  = self.engine.write_parquet(
            df=df,
            target_path=bronze_path,
            partition_by=["ingest_date"],
        )
        logger.info(f"[BronzeIngester:{symbol}] Đã ghi {len(clean_records)} records → {saved_path}")

        # — Ghi rejects (DQ framework) —
        reject_path = None
        if reject_records:
            reject_df   = pl.DataFrame(reject_records)
            reject_path = f"{self.base_path}/bronze/_rejects/{self.table_name}/"
            self.engine.write_parquet(df=reject_df, target_path=reject_path)
            logger.warning(
                f"[BronzeIngester:{symbol}] {len(reject_records)} rejects ghi tại {reject_path}"
            )

        return {
            "saved_path":  saved_path,
            "reject_path": reject_path,
            "stats": {
                **profile,
                "clean":   len(clean_records),
                "rejects": len(reject_records),
            },
        }




# ===========================================================================
# SILVER PROCESSOR  (DuckDB + Subsystem 7, 9, 10)
# ===========================================================================

class SilverProcessor:
    """
    Subsystem 5 (Cleansing) + Subsystem 7 (Deduplication)
     + Subsystem 9 (SCD) + Subsystem 10 (Surrogate Key)

    Sử dụng DuckDB để đọc Parquet từ Bronze layer, áp dụng
    business rules và ghi Silver Delta tables (Polars → Parquet).
    """

    def __init__(
        self,
        duckdb_engine: DuckDBEngine,
        polars_engine: PolarsEngine,
        base_path:     str = _LAKEHOUSE_BASE,
    ) -> None:
        self.duck    = duckdb_engine
        self.polars  = polars_engine
        self.sk_gen  = SurrogateKeyGenerator(prefix="STK_", key_length=32)
        self.base    = base_path
        self.logger  = polars_engine.logger

    # ------------------------------------------------------------------
    # Stock prices: Bronze → Silver fact_stock_price
    # ------------------------------------------------------------------
    def transform_stock_prices(
        self,
        target_date: str,
        run_id:      str,
        dedup_strategy: DeduplicationStrategy = DeduplicationStrategy.KEEP_LAST,
    ) -> Dict[str, Any]:
        """
        1. Đọc Bronze stock_prices cho ngày target_date qua DuckDB
        2. Deduplicate (Subsystem 7)
        3. Thêm surrogate key (Subsystem 10)
        4. Ghi Silver fact_stock_price partitioned by date

        Returns: dict với row counts và path.
        """
        import polars as pl

        bronze_glob = (
            f"{self.base}/bronze/stock_prices/"
            f"ingest_date={target_date}/*.parquet"
        )
        self.logger.info(
            f"[SilverProcessor] Đọc Bronze ngày {target_date} từ: {bronze_glob}"
        )

        # — Đọc qua DuckDB (có S3 credentials) —
        try:
            df_lf: pl.LazyFrame = self.duck.query_to_polars(
                f"SELECT * FROM read_parquet('{bronze_glob}')"
            )
            df: pl.DataFrame = df_lf.collect()
        except Exception as exc:
            self.logger.error(f"[SilverProcessor] Lỗi đọc Bronze: {exc}")
            raise

        if df.is_empty():
            self.logger.warning(
                f"[SilverProcessor] Không có dữ liệu Bronze cho ngày {target_date}."
            )
            return {"rows_in": 0, "rows_out": 0, "silver_path": None}

        rows_in = len(df)
        self.logger.info(f"[SilverProcessor] Đọc được {rows_in} rows từ Bronze.")

        # — Deduplication (Subsystem 7) —
        # Pandas interface cho DeduplicationEngine
        import pandas as pd
        df_pd = df.to_pandas()
        dedup_engine = DeduplicationEngine(
            keys=["symbol", "date"],
            strategy=dedup_strategy,
            tiebreaker_col="ingest_timestamp",
        )
        deduped_pd = dedup_engine.deduplicate(df_pd)
        dedup_stats = dedup_engine.last_stats
        self.logger.info(
            f"[SilverProcessor] Dedup stats: {dedup_stats}. "
            f"Còn lại {len(deduped_pd)} rows."
        )
        df = pl.from_pandas(deduped_pd)

        # — Surrogate Key (Subsystem 10) —
        # Thêm cột stock_sk = SHA256(symbol)
        df = df.with_columns(
            pl.col("symbol")
            .map_elements(
                lambda sym: self.sk_gen.hash_key(sym),
                return_dtype=pl.Utf8,
            )
            .alias("stock_sk")
        )

        # — Thêm silver audit columns —
        now_ts = datetime.now(timezone.utc).isoformat()
        df = df.with_columns([
            pl.lit(run_id).alias("_silver_run_id"),
            pl.lit(now_ts).alias("_silver_processed_at"),
        ])

        # — Ghi Silver —
        silver_path = f"{self.base}/silver/fact_stock_price/"
        saved = self.polars.write_parquet(
            df=df,
            target_path=silver_path,
            partition_by=["ingest_date"],
        )
        rows_out = len(df)
        self.logger.info(
            f"[SilverProcessor] Ghi {rows_out} rows Silver → {saved}"
        )
        return {
            "rows_in":     rows_in,
            "rows_out":    rows_out,
            "dedup_stats": dedup_stats,
            "silver_path": saved,
        }


# ===========================================================================
# GOLD PROCESSOR  (SQLMesh + DuckDB)
# ===========================================================================

class GoldProcessor:
    """
    Subsystem 11 (SK Pipeline) + Subsystem 14 (Fact Builder)
     + Subsystem 20 (Aggregate Builder)

    Sử dụng SQLMesh để tính toán Gold models (KPIs, marts).
    SQLMesh quản lý incremental runs và DQ audits tự động.
    """

    def __init__(
        self,
        sqlmesh_engine: SqlMeshEngine,
        duck_engine:    DuckDBEngine,
    ) -> None:
        self.sqlmesh = sqlmesh_engine
        self.duck    = duck_engine
        self.logger  = sqlmesh_engine.logger

    def run_gold_models(
        self,
        environment: str   = "prod",
        start_date:  Optional[str] = None,
        end_date:    Optional[str] = None,
        run_audits:  bool  = True,
    ) -> Dict[str, Any]:
        """
        Chạy SQLMesh DAG cho Gold layer:
          1. plan() – kiểm tra thay đổi models
          2. run()  – thực thi incremental/full
          3. audit()– chạy DQ tests (Subsystem 28)

        Returns: dict kết quả
        """
        self.logger.info(
            f"[GoldProcessor] Chạy SQLMesh Gold models. "
            f"Env={environment}, start={start_date}, end={end_date}"
        )

        result: Dict[str, Any] = {"environment": environment}

        try:
            # Plan (dry-run review changes)
            plan_result = self.sqlmesh.plan(environment=environment)
            result["plan"] = str(plan_result)
            self.logger.info("[GoldProcessor] SQLMesh Plan hoàn tất.")
        except Exception as exc:
            self.logger.warning(
                f"[GoldProcessor] SQLMesh plan warning (non-fatal): {exc}"
            )

        # Run models
        self.sqlmesh.run(
            environment=environment,
            start=start_date,
            end=end_date,
        )
        result["run_status"] = "SUCCESS"
        self.logger.info("[GoldProcessor] SQLMesh Run hoàn tất.")

        # DQ Audits (Subsystem 28 – Test Harness)
        if run_audits:
            self.sqlmesh.audit()
            result["audits"] = "PASSED"
            self.logger.info("[GoldProcessor] DQ Audits hoàn tất.")

        return result


# ===========================================================================
# SERVING SYNC  (Subsystem 19 – Fact Provider, Subsystem 27 – Sort/Optimize)
# ===========================================================================

class ServingSyncProcessor:
    """
    Subsystem 19 (Fact Provider) + Subsystem 27 (Sort/Optimize).

    Sync Gold Parquet → PostgreSQL via DuckDB postgres extension.
    DuckDB là bridge tốc độ cao, không cần PySpark.
    """

    def __init__(
        self,
        duck_engine: DuckDBEngine,
        base_path:   str = _LAKEHOUSE_BASE,
    ) -> None:
        self.duck   = duck_engine
        self.base   = base_path
        self.logger = duck_engine.logger

    def _pg_conn_string(self) -> str:
        return (
            f"postgresql://{_PG_USER}:{_PG_PASSWORD}"
            f"@{_PG_HOST}:{_PG_PORT}/{_PG_DB}"
        )

    def sync_gold_to_postgres(
        self,
        mart_table:  str = "mart_kpi_daily",
        target_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Đọc Gold mart từ MinIO, ghi sang PostgreSQL serving table.
        Sử dụng DuckDB ATTACH + postgres extension (Subsystem 19).
        """
        if not _PG_USER:
            self.logger.warning(
                "[ServingSync] Bỏ qua: thiếu POSTGRES_USER trong .env"
            )
            return {"skipped": True, "reason": "missing_pg_credentials"}

        gold_glob = f"{self.base}/gold/{mart_table}/"
        if target_date:
            gold_glob += f"date={target_date}/*.parquet"
        else:
            gold_glob += "**/*.parquet"

        self.logger.info(
            f"[ServingSync] Sync Gold '{mart_table}' → PostgreSQL. "
            f"Source: {gold_glob}"
        )

        try:
            con = self.duck.connection

            # Install + load postgres extension
            con.execute("INSTALL postgres; LOAD postgres;")

            # Attach PostgreSQL
            pg_conn = self._pg_conn_string()
            con.execute(
                f"ATTACH '{pg_conn}' AS pg_serving (TYPE POSTGRES, READ_WRITE);"
            )

            # UPSERT: read Parquet → insert into PostgreSQL
            con.execute(
                f"""
                INSERT INTO pg_serving.gold.{mart_table}
                    SELECT * FROM read_parquet('{gold_glob}', hive_partitioning=true)
                ON CONFLICT DO NOTHING;
                """
            )

            self.logger.info(
                f"[ServingSync] Hoàn tất sync '{mart_table}' → PostgreSQL."
            )
            return {"status": "SUCCESS", "mart": mart_table}

        except Exception as exc:
            self.logger.error(f"[ServingSync] Lỗi khi sync: {exc}")
            raise

    def optimize_gold_tables(self) -> None:
        """
        Subsystem 27 (Sort/Optimize): chạy OPTIMIZE + ZORDER trên Gold tables.
        Gọi sau khi write xong để compact small files.
        """
        self.logger.info("[ServingSync] Bắt đầu OPTIMIZE Gold tables (Subsystem 27)...")
        # Nếu dùng Delta Lake backend, gọi DeltaLakeStorageBackend.optimize()
        # Ở đây log placeholder vì OPTIMIZE cần PySpark + delta-spark
        self.logger.info(
            "[ServingSync] OPTIMIZE placeholder – cần DeltaLakeStorageBackend.optimize() "
            "khi PySpark cluster available."
        )


# ===========================================================================
# LINEAGE RECORDER  (Subsystem 29 – Lineage & Dependency Analyzer)
# ===========================================================================

def _record_lineage(
    lineage_recorder: LineageRecorder,
    run_id:           str,
    source_layer:     str,
    source_table:     str,
    target_layer:     str,
    target_table:     str,
    operation:        str,
    rows_affected:    int,
) -> None:
    """Ghi data lineage record (Subsystem 29)."""
    try:
        lineage_recorder.record(
            run_id=run_id,
            source_layer=source_layer,
            source_table=source_table,
            target_layer=target_layer,
            target_table=target_table,
            operation=operation,
            rows_affected=rows_affected,
        )
    except Exception as exc:
        # Lineage failure không nên block pipeline chính
        pass


# ===========================================================================
# FACTORY HELPERS – khởi tạo engines từ env/config
# ===========================================================================

def _build_polars_engine(config: PrefectETLPipelineConfig) -> PolarsEngine:
    """Khởi tạo PolarsEngine với cấu hình từ yaml + env."""
    polars_cfg_dict = config.polars_config or {}
    polars_cfg = PolarsConfig(
        thread_pool_size=polars_cfg_dict.get("thread_pool_size"),
        enable_streaming=polars_cfg_dict.get("enable_streaming", True),
        storage_options=_STORAGE_OPTIONS,
    )
    return PolarsEngine(config=polars_cfg, logger=logger_manager.get_logger("polars_engine"))


def _build_duckdb_engine() -> DuckDBEngine:
    """Khởi tạo DuckDBEngine với S3/MinIO credentials."""
    duck_cfg = DuckDBConfig(
        database_path=":memory:",
        storage_options=_STORAGE_OPTIONS,
    )
    return DuckDBEngine(config=duck_cfg, logger=logger_manager.get_logger("duckdb_engine"))


def _build_sqlmesh_engine() -> SqlMeshEngine:
    """Khởi tạo SqlMeshEngine trỏ tới thư mục sqlmesh/."""
    cfg = SqlMeshConfig(
        project_path=_SQLMESH_PATH,
        gateway=_SQLMESH_GATEWAY,
    )
    return SqlMeshEngine(config=cfg, logger=logger_manager.get_logger("sqlmesh_engine"))


def _build_extractor(config: PrefectETLPipelineConfig) -> ExtractCophieu68:
    """Khởi tạo crawler Cophieu68 với config từ yaml."""
    crawler_cfg = config.cophieu68_config or {}
    # Wrap sang pipeline_config format mà ExtractCophieu68 mong đợi
    mock_pipeline_cfg = {
        "sources": crawler_cfg.get("project_params", {}).get("sources", crawler_cfg),
        "http": crawler_cfg.get("project_params", {}).get(
            "http", {"delay_seconds": 0.5, "timeout_seconds": 30}
        ),
    }
    return ExtractCophieu68(
        pipeline_config=mock_pipeline_cfg,
        pipeline_logger=logger_manager.get_logger("extractor"),
    )


def _build_cleansing_rules(symbol: str) -> CleansingRuleSet:
    """
    Subsystem 4 (Data Cleansing): định nghĩa business rules cho stock_prices.
    Mở rộng bằng cách thêm rule vào CleansingRuleSet.
    """
    ruleset = CleansingRuleSet(table_name="stock_prices")

    # Rule 1: Symbol không được null
    def rule_symbol_not_null(rec: Dict[str, Any]):
        val = rec.get("symbol")
        if not val:
            return False, f"[DQ] symbol is null for record: {rec}"
        return True, ""

    # Rule 2: Giá đóng cửa phải là số dương
    def rule_positive_close(rec: Dict[str, Any]):
        try:
            close = float(rec.get("close_price") or rec.get("close") or 0)
            if close <= 0:
                return False, f"[DQ] close_price <= 0: {close}"
        except (TypeError, ValueError):
            return False, f"[DQ] close_price không parse được: {rec.get('close_price')}"
        return True, ""

    # Rule 3: Volume phải >= 0
    def rule_non_negative_volume(rec: Dict[str, Any]):
        try:
            vol = float(rec.get("volume") or 0)
            if vol < 0:
                return False, f"[DQ] volume < 0: {vol}"
        except (TypeError, ValueError):
            pass  # Volume null → warning nhưng không reject
        return True, ""

    ruleset.add_rule(rule_symbol_not_null)
    ruleset.add_rule(rule_positive_close)
    ruleset.add_rule(rule_non_negative_volume)
    return ruleset


# ===========================================================================
# PREFECT TASKS
# ===========================================================================

# ---------------------------------------------------------------------------
# TASK 1: BRONZE INGESTION  (Phase 2)
# ---------------------------------------------------------------------------
@task(
    name="Phase 2: Bronze Ingestion",
    description="Crawl Cophieu68 → Polars DQ → Parquet Bronze (MinIO)",
    retries=2,
    retry_delay_seconds=30,
    tags=["bronze", "ingestion", "polars"],
)
def task_ingest_bronze(
    symbol:  str,
    config:  PrefectETLPipelineConfig,
    run_id:  str,
) -> Dict[str, Any]:
    """
    Subsystem 3 (Extract) + Subsystem 1 (Profiling)
     + Subsystem 4 (DQ Pre-eval) + Subsystem 6 (Audit)

    Flow:
      1. Crawl dữ liệu lịch sử từ Cophieu68
      2. DQ pre-evaluation + cleansing rules
      3. Thêm audit metadata (batch_id, ingest_ts, source)
      4. Ghi Polars Parquet → Bronze MinIO
      5. Ghi lineage record (Subsystem 29)
    """
    logger = get_run_logger()
    batch_id = _make_batch_id(symbol)

    logger.info(
        f"[Bronze:{symbol}] Bắt đầu. batch_id={batch_id}, run_id={run_id}"
    )

    # 1. Crawl dữ liệu (Subsystem 3)
    extractor = _build_extractor(config)
    trading_data = extractor.crawl_trading_data(symbol=symbol, page=1)

    if not trading_data or not trading_data.get("records"):
        logger.warning(f"[Bronze:{symbol}] Không có dữ liệu trả về từ crawler.")
        return {
            "symbol":      symbol,
            "batch_id":    batch_id,
            "skipped":     True,
            "rows_saved":  0,
        }

    raw_records: List[Dict[str, Any]] = trading_data["records"]
    logger.info(f"[Bronze:{symbol}] Crawler trả về {len(raw_records)} records.")

    # 2. Polars Engine + Ingester
    polars_engine  = _build_polars_engine(config)
    bronze_ingester = BronzePolarsIngester(
        engine=polars_engine,
        table_name="stock_prices",
        base_path=_LAKEHOUSE_BASE,
    )

    # 3. Cleansing rules (Subsystem 4)
    cleansing = _build_cleansing_rules(symbol)

    # 4. Process: DQ pre-eval → Polars DataFrame → Write Parquet
    result = bronze_ingester.process(
        raw_records=raw_records,
        batch_id=batch_id,
        run_id=run_id,
        symbol=symbol,
        cleansing=cleansing,
    )

    logger.info(
        f"[Bronze:{symbol}] Hoàn tất. "
        f"clean={result['stats'].get('clean')}, "
        f"rejects={result['stats'].get('rejects')}, "
        f"path={result.get('saved_path')}"
    )

    return {
        "symbol":      symbol,
        "batch_id":    batch_id,
        "run_id":      run_id,
        "rows_saved":  result["stats"].get("clean", 0),
        "rows_rejected": result["stats"].get("rejects", 0),
        "saved_path":  result.get("saved_path"),
        "reject_path": result.get("reject_path"),
    }


# ---------------------------------------------------------------------------
# TASK 2: SILVER TRANSFORM  (Phase 3)
# ---------------------------------------------------------------------------
@task(
    name="Phase 3: Silver Transformation",
    description="DuckDB read Bronze → Dedup → SK → Polars write Silver",
    retries=1,
    retry_delay_seconds=60,
    tags=["silver", "transform", "duckdb", "polars"],
)
def task_transform_silver(
    target_date: str,
    run_id:      str,
) -> Dict[str, Any]:
    """
    Subsystem 5 (Cleansing) + Subsystem 7 (Deduplication)
     + Subsystem 9 (SCD) + Subsystem 10 (Surrogate Key)

    Flow:
      1. DuckDB đọc Bronze Parquet (có S3)
      2. Deduplication theo (symbol, date)
      3. Thêm surrogate key stock_sk
      4. Ghi Polars DataFrame → Silver Parquet
      5. Ghi lineage record (Subsystem 29)
    """
    logger = get_run_logger()
    logger.info(
        f"[Silver] Bắt đầu transform. target_date={target_date}, run_id={run_id}"
    )

    duck_engine   = _build_duckdb_engine()
    polars_engine = PolarsEngine(
        config=PolarsConfig(storage_options=_STORAGE_OPTIONS),
        logger=logger_manager.get_logger("polars_silver"),
    )

    processor = SilverProcessor(
        duckdb_engine=duck_engine,
        polars_engine=polars_engine,
        base_path=_LAKEHOUSE_BASE,
    )

    try:
        result = processor.transform_stock_prices(
            target_date=target_date,
            run_id=run_id,
            dedup_strategy=DeduplicationStrategy.KEEP_LAST,
        )
    finally:
        duck_engine.close()

    logger.info(
        f"[Silver] Hoàn tất. "
        f"rows_in={result.get('rows_in')}, "
        f"rows_out={result.get('rows_out')}, "
        f"path={result.get('silver_path')}"
    )
    return {**result, "run_id": run_id, "target_date": target_date}


# ---------------------------------------------------------------------------
# TASK 3: GOLD MODELING via SQLMesh  (Phase 4)
# ---------------------------------------------------------------------------
@task(
    name="Phase 4: Gold Modeling (SQLMesh)",
    description="SQLMesh incremental models → Gold layer (KPIs, marts)",
    retries=1,
    retry_delay_seconds=120,
    tags=["gold", "sqlmesh", "modeling"],
)
def task_run_gold_sqlmesh(
    target_date: str,
    run_id:      str,
    environment: str = "prod",
) -> Dict[str, Any]:
    """
    Subsystem 11 (SK Pipeline) + Subsystem 14 (Fact Builder)
     + Subsystem 20 (Aggregate Builder) + Subsystem 28 (Test/Audit)

    Flow:
      1. SQLMesh plan() – review model changes
      2. SQLMesh run() – incremental tính toán Gold models (DuckDB gateway)
      3. SQLMesh audit() – DQ checks tự động sau khi chạy
    """
    logger = get_run_logger()
    logger.info(
        f"[Gold] Chạy SQLMesh Gold models. "
        f"date={target_date}, env={environment}, run_id={run_id}"
    )

    sqlmesh_engine = _build_sqlmesh_engine()
    processor      = GoldProcessor(
        sqlmesh_engine=sqlmesh_engine,
        duck_engine=_build_duckdb_engine(),
    )

    result = processor.run_gold_models(
        environment=environment,
        start_date=target_date,
        end_date=target_date,
        run_audits=True,
    )

    logger.info(f"[Gold] SQLMesh hoàn tất: {result.get('run_status')}")
    return {**result, "run_id": run_id, "target_date": target_date}


# ---------------------------------------------------------------------------
# TASK 4: SERVING SYNC  (Phase 5)
# ---------------------------------------------------------------------------
@task(
    name="Phase 5: Serving Sync (Gold → PostgreSQL)",
    description="DuckDB bridge: Gold Parquet → PostgreSQL serving tables",
    retries=2,
    retry_delay_seconds=30,
    tags=["serving", "postgres", "duckdb"],
)
def task_sync_serving(
    target_date: str,
    run_id:      str,
    mart_tables: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Subsystem 19 (Fact Provider) + Subsystem 27 (Sort/Optimize)

    Flow:
      1. DuckDB đọc Gold Parquet từ MinIO
      2. ATTACH PostgreSQL → INSERT / UPSERT
      3. Optimize (placeholder cho Delta OPTIMIZE)
    """
    logger = get_run_logger()
    logger.info(
        f"[Serving] Bắt đầu sync Gold → PostgreSQL. "
        f"date={target_date}, run_id={run_id}"
    )

    if not _PG_USER:
        logger.warning("[Serving] Bỏ qua: POSTGRES_USER chưa được set trong .env")
        return {"skipped": True}

    tables = mart_tables or ["mart_kpi_daily"]
    duck_engine = _build_duckdb_engine()
    sync_proc   = ServingSyncProcessor(
        duck_engine=duck_engine,
        base_path=_LAKEHOUSE_BASE,
    )

    results = {}
    try:
        for table in tables:
            res = sync_proc.sync_gold_to_postgres(
                mart_table=table,
                target_date=target_date,
            )
            results[table] = res
            logger.info(f"[Serving] {table}: {res}")

        # Optimize Gold tables (Subsystem 27)
        sync_proc.optimize_gold_tables()

    finally:
        duck_engine.close()

    logger.info(f"[Serving] Hoàn tất sync {len(tables)} mart table(s).")
    return {"run_id": run_id, "target_date": target_date, "marts": results}


# ===========================================================================
# MASTER PIPELINE FLOW
# ===========================================================================

@flow(
    name="Daily Master Cophieu68 Lakehouse Flow",
    description=(
        "Điều phối toàn bộ ETL pipeline theo kiến trúc Lakehouse Framework: "
        "Bronze (Polars) → Silver (DuckDB+Dedup+SK) → Gold (SQLMesh) → Serving (PostgreSQL). "
        "Tích hợp 34 ETL Subsystems."
    ),
    log_prints=True,
)
def master_lakehouse_flow(
    symbols:     List[str] = DEFAULT_SYMBOLS,
    target_date: Optional[str] = None,
    environment: str = "prod",
    mart_tables: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Pipeline chính – chạy hàng ngày qua Prefect Scheduler (Subsystem 21).

    Args:
        symbols:     Danh sách mã cổ phiếu cần ingest (default: 5 mã tiêu biểu)
        target_date: Ngày xử lý (YYYY-MM-DD). Default: hôm nay
        environment: SQLMesh environment (prod / dev)
        mart_tables: Danh sách Gold mart tables để sync (default: mart_kpi_daily)

    Luồng xử lý:
      1. Phase 2 – Bronze: crawl + DQ pre-eval + Polars write (parallel per symbol)
      2. Phase 3 – Silver: DuckDB read + Dedup + SK + Polars write
      3. Phase 4 – Gold:   SQLMesh incremental models + DQ audits
      4. Phase 5 – Serving: DuckDB → PostgreSQL sync + OPTIMIZE

    Subsystems tích hợp:
      Bronze  → 1, 3, 4, 6
      Silver  → 5, 7, 9, 10, 29
      Gold    → 11, 14, 20, 28
      Serving → 19, 27, 29
      Infra   → 21, 22, 26, 34
    """
    logger = get_run_logger()

    # Resolve target date
    if target_date is None:
        target_date = date.today().isoformat()

    run_id = _make_run_id()

    logger.info(
        f"🚀 Master Lakehouse Flow bắt đầu. "
        f"run_id={run_id}, target_date={target_date}, "
        f"symbols={symbols}, env={environment}"
    )

    # Load central config (PrefectETLPipelineConfig – bridge tất cả subsystem configs)
    config = PrefectETLPipelineConfig(project_root=_PROJECT_ROOT)

    # -----------------------------------------------------------------------
    # PHASE 2 – BRONZE INGESTION  (parallel per symbol)
    # -----------------------------------------------------------------------
    logger.info(f"📥 Phase 2: Bronze Ingestion cho {len(symbols)} symbols...")

    bronze_results = []
    for symbol in symbols:
        result = task_ingest_bronze(
            symbol=symbol,
            config=config,
            run_id=run_id,
        )
        bronze_results.append(result)

    total_ingested = sum(
        r.get("rows_saved", 0) for r in bronze_results if isinstance(r, dict)
    )
    total_rejected = sum(
        r.get("rows_rejected", 0) for r in bronze_results if isinstance(r, dict)
    )
    logger.info(
        f"✅ Phase 2 hoàn tất. "
        f"Tổng rows ingested={total_ingested}, rejected={total_rejected}"
    )

    # -----------------------------------------------------------------------
    # PHASE 3 – SILVER TRANSFORMATION  (sau khi Bronze done)
    # -----------------------------------------------------------------------
    logger.info(f"🔄 Phase 3: Silver Transform cho ngày {target_date}...")

    silver_result = task_transform_silver(
        target_date=target_date,
        run_id=run_id,
        wait_for=bronze_results,  # Đảm bảo Bronze xong mới chạy
    )

    logger.info(
        f"✅ Phase 3 hoàn tất. "
        f"rows_out={silver_result.get('rows_out', 'N/A') if isinstance(silver_result, dict) else 'N/A'}"
    )

    # -----------------------------------------------------------------------
    # PHASE 4 – GOLD MODELING via SQLMesh
    # -----------------------------------------------------------------------
    logger.info(f"🏆 Phase 4: Gold Modeling (SQLMesh env={environment})...")

    gold_result = task_run_gold_sqlmesh(
        target_date=target_date,
        run_id=run_id,
        environment=environment,
        wait_for=[silver_result],
    )

    logger.info(
        f"✅ Phase 4 hoàn tất. "
        f"status={gold_result.get('run_status', 'N/A') if isinstance(gold_result, dict) else 'N/A'}"
    )

    # -----------------------------------------------------------------------
    # PHASE 5 – SERVING SYNC (Gold → PostgreSQL)
    # -----------------------------------------------------------------------
    logger.info(f"📤 Phase 5: Serving Sync → PostgreSQL...")

    serving_result = task_sync_serving(
        target_date=target_date,
        run_id=run_id,
        mart_tables=mart_tables,
        wait_for=[gold_result],
    )

    logger.info("✅ Phase 5 hoàn tất.")

    # -----------------------------------------------------------------------
    # PIPELINE SUMMARY  (Subsystem 26 – Workflow Monitor)
    # -----------------------------------------------------------------------
    summary = {
        "run_id":        run_id,
        "target_date":   target_date,
        "symbols":       symbols,
        "environment":   environment,
        "bronze": {
            "total_symbols":  len(symbols),
            "rows_ingested":  total_ingested,
            "rows_rejected":  total_rejected,
        },
        "silver": silver_result if isinstance(silver_result, dict) else {},
        "gold":   gold_result   if isinstance(gold_result, dict)   else {},
        "serving": serving_result if isinstance(serving_result, dict) else {},
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "status": "SUCCESS",
    }

    logger.info(
        f"🎉 Master Lakehouse Flow HOÀN TẤT.\n"
        f"   run_id      : {run_id}\n"
        f"   target_date : {target_date}\n"
        f"   ingested    : {total_ingested} rows\n"
        f"   rejected    : {total_rejected} rows\n"
        f"   status      : SUCCESS"
    )

    return summary


# ===========================================================================
# DEPLOYMENT ENTRYPOINT
# ===========================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Cophieu68 Master Lakehouse ETL Pipeline"
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=DEFAULT_SYMBOLS,
        help="Danh sách mã cổ phiếu (VD: FPT VNM HPG)",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Ngày xử lý YYYY-MM-DD (default: hôm nay)",
    )
    parser.add_argument(
        "--env",
        default="prod",
        choices=["prod", "dev", "staging"],
        help="SQLMesh environment",
    )
    parser.add_argument(
        "--marts",
        nargs="*",
        default=None,
        help="Gold mart tables để sync (VD: mart_kpi_daily)",
    )
    args = parser.parse_args()

    master_lakehouse_flow(
        symbols=args.symbols,
        target_date=args.date,
        environment=args.env,
        mart_tables=args.marts,
    )