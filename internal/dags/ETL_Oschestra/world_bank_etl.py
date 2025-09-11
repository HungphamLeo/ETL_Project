import logging
import sys
import time
import threading
from pathlib import Path
from typing import Dict, Any, Optional, Type

import pandas as pd
from dataclasses import fields
from datetime import timedelta

from airflow.models import Variable
from cmd_.load_config import load_config
from src.logger import FastLogger
from internal.dags.wbapi_dag.transform.wbapi_transform_implement import SparkTransformConfig
from internal.dags.wbapi_dag.load.wbapi_load_implement import DatabaseConfig, SecurityManager



# ==============================
# ETL Pipeline Config
# ==============================

class ETLPipelineConfig:
    """
    Centralized configuration manager cho ETL pipeline.
    Phối hợp các config thành phần: Database, Spark, Security.
    """

    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path
        self.config = self._load_config()
        self.logger = self._setup_logger()

        # Khởi tạo các config thành phần
        self.database_config = DatabaseConfig(config_path=config_path)
        self.spark_config = SparkTransformConfig(self.config.get('spark', {}))
        self.security_manager: Optional[SecurityManager] = None

    # ========== Setup & Load Config ==========

    def _load_config(self) -> Dict[str, Any]:
        """
        Load configuration từ Airflow Variable trước, nếu không có thì fallback sang file.
        """
        try:
            etl_config = Variable.get(
                "project_services",
                default_var=None,
                deserialize_json=True
            )
            if etl_config:
                print("etl_config loaded from Airflow Variable.", etl_config)
                return etl_config
        except Exception:
            etl_config = load_config(self.config_path).get["project_params", {}]
            return etl_config

    def _setup_logger(self) -> logging.Logger:
        """Setup logger instance"""
        return FastLogger(self.config).get_logger()

    # ========== Accessor Methods ==========

    def set_security_manager(self, db_session):
        """Khởi tạo SecurityManager với session"""
        self.security_manager = SecurityManager(db_session)

    def get_database_config(self) -> DatabaseConfig:
        return self.database_config

    def get_spark_config(self, mode: str = "default") -> dict:
        return self.spark_config.get_config(mode)

    def get_security_manager(self) -> Optional[SecurityManager]:
        return self.security_manager

    def get_airflow_default_args(self) -> Dict[str, Any]:
        """
        Trả về default_args cho Airflow DAG.
        """
        airflow_config = self.config.get('airflow', {}).get('default_args', {})
        return {
            'owner': airflow_config.get('owner', 'data-engineering'),
            'depends_on_past': airflow_config.get('depends_on_past', False),
            'email_on_failure': airflow_config.get('email_on_failure', True),
            'email_on_retry': airflow_config.get('email_on_retry', False),
            'retries': airflow_config.get('retries', 1),
            'retry_delay': timedelta(seconds=airflow_config.get('retry_delay_sec', 300)),
            'execution_timeout': timedelta(seconds=airflow_config.get('execution_timeout_sec', 7200)),
        }

    def get_environment(self) -> str:
        return self.config.get('environment', 'development')

    def is_production(self) -> bool:
        return self.get_environment() == 'production'

    def is_development(self) -> bool:
        return self.get_environment() == 'development'

    def get_monitoring_config(self) -> Dict[str, Any]:
        return self.config.get('monitoring', {})


