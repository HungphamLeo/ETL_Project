import os
import yaml
import logging
from datetime import timedelta
from typing import Dict, Any, Optional
from airflow.models import Variable

from cmd_.load_config import load_config
from src.logger import FastLogger


class ETLPipelineConfig:
    """Centralized configuration manager for ETL pipeline using YAML only"""

    def __init__(self):
        self.config = self._load_config()
        self.logger = self._setup_logger()

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file, with Airflow Variable fallback"""
        try:
            etl_config = Variable.get("etl_pipeline_config", default_var=None, deserialize_json=True)
            if etl_config:
                return etl_config
        except Exception:
            pass

        config_path = os.getenv('ETL_CONFIG_PATH', './internal/config/data_craw_web_config/data_craw_web_config.yaml')
        return load_config(config_path)

    def _setup_logger(self) -> logging.Logger:
        return FastLogger(self.config).get_logger()

    def get_database_url(self, use_replica: bool = False) -> str:
        db_config = self.config['database']['read_replica'] if use_replica and 'read_replica' in self.config['database'] else self.config['database']['primary']

        url = (
            f"mysql+pymysql://{db_config['username']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config.get('port', 3306)}"
            f"/{db_config['database']}"
            f"?charset={db_config.get('charset', 'utf8mb4')}"
        )

        ssl = self.config['database'].get('ssl', {})
        if ssl.get('enabled'):
            ssl_params = [f"{k}={v}" for k, v in ssl.items() if k != 'enabled' and v]
            if ssl_params:
                url += "&" + "&".join(ssl_params)

        return url

    def get_kafka_producer_config(self) -> Dict[str, Any]:
        kafka = self.config['kafka']
        return {
            **kafka.get('default_producer_config', {}),
            **kafka.get('producer_config', {})
        }

    def get_kafka_consumer_config(self, group_id: str) -> Dict[str, Any]:
        kafka = self.config['kafka']
        return {
            'group_id': group_id,
            **kafka.get('default_consumer_config', {}),
            **kafka.get('consumer_config', {})
        }

    def get_spark_config(self) -> Dict[str, Any]:
        spark = self.config['spark']
        hdfs = self.config['hdfs']
        return {
            'app_name': spark.get('app_name'),
            'master': spark.get('master'),
            'packages': spark.get('packages', []),
            'conf': {
                **spark.get('conf', {}),
                'spark.hadoop.fs.defaultFS': hdfs['namenode'],
            }
        }

    def get_hdfs_config(self) -> Dict[str, Any]:
        return self.config['hdfs']

    def get_monitoring_config(self) -> Dict[str, Any]:
        return self.config.get('monitoring', {})

    def get_airflow_default_args(self) -> Dict[str, Any]:
        args = self.config.get('airflow', {}).get('default_args', {})
        return {
            'owner': args.get('owner', 'data-engineering'),
            'depends_on_past': args.get('depends_on_past', False),
            'email_on_failure': args.get('email_on_failure', True),
            'email_on_retry': args.get('email_on_retry', False),
            'retries': args.get('retries', 1),
            'retry_delay': timedelta(seconds=args.get('retry_delay_sec', 300)),
            'execution_timeout': timedelta(seconds=args.get('execution_timeout_sec', 7200)),
        }

    def get_environment(self) -> str:
        return self.config.get('environment', 'development')

    def is_production(self) -> bool:
        return self.get_environment() == 'production'

    def is_development(self) -> bool:
        return self.get_environment() == 'development'


# Singleton for reuse
_pipeline_config: Optional[ETLPipelineConfig] = None

def get_pipeline_config() -> ETLPipelineConfig:
    global _pipeline_config
    if _pipeline_config is None:
        _pipeline_config = ETLPipelineConfig()
    return _pipeline_config
