import os
import yaml
from pathlib import Path
from datetime import timedelta
from typing import Dict, Any, Optional
import logging

from platforms.processing.base_processing import ConfigLoader, FileConfigLoader, LoggerFactory, DefaultLoggerFactory
from platforms.schema_models_manage.vietnam_stock.delta_schema_registry import (
    ALL_BRONZE_TABLES,
    ALL_SILVER_TABLES,
    ALL_GOLD_TABLES
)

class PrefectETLPipelineConfig:
    """
    Centralized configuration manager cho ETL pipeline.
    Đóng vai trò Cầu nối (Bridge) giữa Ingestion và Platform Setup.
    Hỗ trợ load động các config từ Storage, Processing tools, và Subsystems.
    """

    def __init__(
        self,
        project_root: Optional[str] = None,
        config_loader: Optional[ConfigLoader] = None,
        logger_factory: Optional[LoggerFactory] = None,
    ):
        # Xác định thư mục gốc của dự án (ETL_Project)
        if project_root:
            self.project_root = Path(project_root)
        else:
            self.project_root = Path(__file__).resolve().parent.parent.parent.parent.parent
        
        self._loader = config_loader or FileConfigLoader()
        self._logger_factory = logger_factory or DefaultLoggerFactory()
        
        self._loggers: Dict[str, logging.Logger] = {}
        self._configs_cache: Dict[str, Dict[str, Any]] = {}
        
    def _load_yaml_config(self, relative_path: str) -> Dict[str, Any]:
        """Lazy load và cache các file cấu hình YAML"""
        if relative_path in self._configs_cache:
            return self._configs_cache[relative_path]
            
        config_path = self.project_root / relative_path
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
                self._configs_cache[relative_path] = data or {}
                return self._configs_cache[relative_path]
        except Exception:
            self._get_logger("config_loader").warning(f"Could not load config: {config_path}")
            return {}

    # --- Logger helpers (lazy) ---
    def _get_logger(self, key: str) -> logging.Logger:
        if key not in self._loggers:
            self._loggers[key] = logging.getLogger(f"etl.{key}")
        return self._loggers[key]

    def reload(self) -> None:
        """Force re-load all configs from source."""
        self._configs_cache.clear()
        self._loggers = {}

    # =========================================================================
    # PROCESSING TOOLS CONFIGURATIONS
    # =========================================================================
    @property
    def spark_config(self) -> Dict[str, Any]:
        return self._load_yaml_config("platforms/processing/spark/config/spark_config.yaml").get("spark", {})

    @property
    def polars_config(self) -> Dict[str, Any]:
        return self._load_yaml_config("platforms/processing/polars/config/polars_config.yaml").get("polars", {})

    # =========================================================================
    # INGESTION CONFIGURATIONS
    # =========================================================================
    @property
    def cophieu68_config(self) -> Dict[str, Any]:
        return self._load_yaml_config("platforms/processing/prefect/config/cophieu68_config.yaml")

    # =========================================================================
    # DATA LAKEHOUSE SUBSYSTEMS CONFIGURATIONS
    # =========================================================================
    @property
    def metadata_repo_config(self) -> Dict[str, Any]:
        return self._load_yaml_config("platforms/processing/base_processing_subsystem/config/metadata_repo.yaml").get("metadata_repo", {})
        
    @property
    def event_log_schema(self) -> Dict[str, Any]:
        return self._load_yaml_config("shared/logger/config/event_log_schema.yaml")

    @property
    def scd_manager_config(self) -> Dict[str, Any]:
        return self._load_yaml_config("platforms/processing/base_processing_subsystem/config/scd_manager.yaml").get("scd_manager", {})

    @property
    def deduplication_config(self) -> Dict[str, Any]:
        return self._load_yaml_config("platforms/processing/base_processing_subsystem/config/deduplication.yaml").get("deduplication", {})

    @property
    def data_quality_config(self) -> Dict[str, Any]:
        return self._load_yaml_config("platforms/processing/base_processing_subsystem/config/data_quality_pre_evaluation.yaml").get("data_quality_pre_evaluation", {})

    @property
    def data_lineage_config(self) -> Dict[str, Any]:
        return self._load_yaml_config("platforms/processing/base_processing_subsystem/config/data_lineage.yaml").get("data_lineage", {})

    @property
    def data_profiling_config(self) -> Dict[str, Any]:
        return self._load_yaml_config("platforms/processing/base_processing_subsystem/config/data_profiling_config.yaml").get("data_profiling", {})

    # =========================================================================
    # SCHEMA REGISTRY (Bridge to Implementation)
    # =========================================================================
    def get_table_schema(self, layer: str, table_name: str) -> Any:
        """
        Truy xuất Schema định nghĩa Delta Table dựa theo tên bảng và layer.
        Giúp Polars/Spark Ingestion có thể map schema hoặc tạo bảng tự động.
        """
        schema_map = {
            "bronze": ALL_BRONZE_TABLES,
            "silver": ALL_SILVER_TABLES,
            "gold": ALL_GOLD_TABLES
        }
        
        target_registry = schema_map.get(layer.lower())
        if target_registry is None:
            raise ValueError(f"Layer '{layer}' không hợp lệ. Chỉ chấp nhận: bronze, silver, gold")
            
        table_def = target_registry.get(table_name)
        if table_def is None:
            self._get_logger("schema_registry").warning(f"⚠️ Bảng '{table_name}' không tồn tại trong Registry Layer '{layer}'")
            
        return table_def