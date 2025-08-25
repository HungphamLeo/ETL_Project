from datetime import datetime
import os
from internal.models import *
from internal.dags.wbapi_dag.extract import wbapi_extract
import  airflow.operators as ops


class WorldBankExtractOperator(ops.python.PythonOperator):
    """Custom operator cho World Bank API data extraction"""
    
    def __init__(self, pipeline_config, extract_type: str, **kwargs):
        self.extract_type = extract_type
        self.pipeline_config = pipeline_config
        super().__init__(python_callable=self._extract_data, **kwargs)
    
    def _extract_data(self, **context):
        """Extract data từ World Bank API"""
        logger = self.pipeline_config.logger
        
        try:
            extract_obj = wbapi_extract()
            execution_date = context['execution_date']
            
            # Dynamic extraction based on type
            extraction_map = {
                'economy': self._extract_economy_data,
                'series': self._extract_series_data,
                'topic': self._extract_topic_data,
                'time': self._extract_income_data,
                'source': self._extract_source_data,
                'region': self._extract_region_data,
                'income': self._extract_income_data,
                'lending': self._extract_lending_data
            }
            
            if self.extract_type not in extraction_map:
                raise ValueError(f"Unknown extract type: {self.extract_type}")
            
            data = extraction_map[self.extract_type](extract_obj)
            
            # Store data in XCom for downstream tasks
            return {
                'extract_type': self.extract_type,
                'record_count': len(data) if isinstance(data, list) else data.count() if hasattr(data, 'count') else 0,
                'extraction_timestamp': datetime.now().isoformat(),
                'data': data  # Note: For large datasets, consider storing in external storage
            }
            
        except Exception as e:
            logger.error(f"Extraction failed for {self.extract_type}: {str(e)}")
            raise
    
    def _extract_economy_data(self, extract_obj: wbapi_extract):
        """Extract economy-related data"""
        from internal.models.extract_input_models import EconomyDataFrameInput, EconomyMetadataInput
        
        return {
            'dataframe': extract_obj.economy.dataframe_display(EconomyDataFrameInput(id='all')),
            'metadata': extract_obj.economy.get_metadata(EconomyMetadataInput(id='all'))
        }
    
    def _extract_series_data(self, extract_obj:wbapi_extract):
        """Extract series-related data"""
        from internal.models.extract_input_models import SeriesMetadataInput, SeriesGetInput
        
        return {
            'dataframe': extract_obj.series.get_series(SeriesGetInput(id='all')),
            'metadata': extract_obj.series.get_series_metadata(SeriesMetadataInput(id='all'))
        }

    # Additional extraction methods for other data types...
    def _extract_topic_data(self, extract_obj: wbapi_extract):
        from internal.models.extract_input_models import TopicInfoInput, TopicSeriesInput, TopicMembersInput
        return {
            'info': extract_obj.topic.get_info(TopicInfoInput(id='all')),
            'series': extract_obj.topic.get_series(TopicSeriesInput(id='all')),
            'members': extract_obj.topic.get_members(TopicMembersInput(id='all'))
        }

    def _extract_time_data(self, extract_obj: wbapi_extract):
        from internal.models.extract_input_models import TimeSeriesInput
        return extract_obj.time.get_time_periods_series(TimeSeriesInput(id='all'))
    
    def _extract_source_data(self, extract_obj: wbapi_extract):
        from internal.models.extract_input_models import SourceInfoInput, SourceSeriesInput
        return {
            'info': extract_obj.source.get_info(SourceInfoInput(id='all')),
            'series': extract_obj.source.get_series(SourceSeriesInput(id='all'))
        }
    def _extract_region_data(self, extract_obj: wbapi_extract):
        from internal.models.extract_input_models import RegionSeriesInput
        return extract_obj.region.get_series(RegionSeriesInput(id='all'))
    
    def _extract_income_data(self, extract_obj: wbapi_extract):
        from internal.models.extract_input_models import IncomeSeriesInput
        return extract_obj.income.get_series(IncomeSeriesInput(id='all'))
    
    def _extract_lending_data(self, extract_obj: wbapi_extract):
        from internal.models.extract_input_models import LendingSeriesInput
        return extract_obj.lending.get_series(LendingSeriesInput(id='all'))
    

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


import pymysql
from internal.dags.wbapi_dag.load import BaseDBLoader
from internal.models.transform_models import EconomyTransform, SeriesTransform, TopicTransform, TimeTransform, SourceTransform, RegionTransform, IncomeTransform, LendingTransform
from src.utils import TableCreator

class DatabaseLoaderService:
    def __init__(self, pipeline_config):
        self.config = pipeline_config
        self.conn = None
        self.cursor = None
        self.logger = self.config.logger

    def _get_database_connection(self):
        try:
            self.conn = pymysql.connect(
                host=self.config['database']['primary']['host'],
                user=self.config['database']['primary']['username'],
                password=self.config['database']['primary']['password'],
                database=self.config['database']['primary']['database'],
                charset=self.config['database']['primary']['charset'],
                cursorclass=pymysql.cursors.DictCursor
            )
            self.cursor = self.conn.cursor()
        except Exception as e:
            self.logger.error(f"Error during loading: {e}")
            return

    def create_or_replace_database(self):
        try:
            loader = BaseDBLoader(cursor=self.cursor, connection=self.conn, logger=self.logger, config=self.config)
            loader.create_or_replace_database(self.config['database']['primary']['database'])
        except Exception as e:
            self.logger.error(f"Error creating or replacing database: {e}")
            return

    def load_data(self, transformed_data):
        try:
            transform_classes = [
                (EconomyTransform, "ECONOMY", EconomyTransform.ECONOMY_DF_RULES),
                (SeriesTransform, "SERIES", SeriesTransform.SERIES_DF_RULES),
                (TopicTransform, "TOPIC", TopicTransform.TOPIC_DF_RULES),
                (TimeTransform, "TIME", TimeTransform.TIME_DF_RULES),
                (SourceTransform, "SOURCE", SourceTransform.SOURCE_DF_RULES),
                (RegionTransform, "REGION", RegionTransform.REGION_DF_RULES),
                (IncomeTransform, "INCOME", IncomeTransform.INCOME_DF_RULES),
                (LendingTransform, "LENDING", LendingTransform.LENDING_DF_RULES),
            ]

            table_df_map = {
                "economy_data": transformed_data["economy_data"],
                "economy_metadata": transformed_data["economy_metadata"],
                "series_data": transformed_data["series_data"],
                "series_metadata": transformed_data["series_metadata"],
                "topic_info": transformed_data["topic_info"],
                "topic_series": transformed_data["topic_series"],
                "topic_metadata": transformed_data["topic_metadata"],
                "time_series": transformed_data["time_series"],
                "source_info": transformed_data["source_info"],
                "source_series": transformed_data["source_series"],
                "region_info": transformed_data["region_info"],
                "income_series": transformed_data["income_series"],
                "lending_series": transformed_data["lending_series"],
            }

            for transform_cls, prefix, rules in transform_classes:
                for rule_name, rule_dict in rules.items():
                    if not rule_name.startswith("transform_"):
                        continue
                    table_name = f"{prefix.lower()}_{rule_name.replace('transform_', '').replace('_dataframe','').replace('_metadata','')}"
                    character_specific = ''.join([w.capitalize() for w in table_name.split('_')])
                    creator = TableCreator(machine_id=1, character_specific=character_specific)

                    try:
                        sql = creator.generate_create_table_sql(table_name, rule_dict)
                    except Exception as e:
                        self.logger.error(f"Error generating SQL for {table_name}: {e}")
                        return

                    try:
                        self.logger.create_or_replace_table(sql)
                    except Exception as e:
                        self.logger.error(f"Error creating or replacing table {table_name}: {e}")
                        return

                    try:
                        df = table_df_map.get(table_name)
                        if df is None:
                            self.logger.warning(f"No data found for table {table_name}. Skipping insert.")
                            continue
                        # Nếu df là None, bỏ qua insert
                        if df is not None:
                            df_with_id = creator.add_id_column(df)
                            # Insert từng dòng
                            cols = ','.join(df_with_id.columns)
                            placeholders = ','.join(['%s'] * len(df_with_id.columns))
                            insert_sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
                            for row in df_with_id.itertuples(index=False, name=None):
                                self.logger.insert_table(insert_sql, row)
                    except Exception as e:
                        self.logger.error(f"Error inserting data into table {table_name}: {e}")
                        return
        except Exception as e:
            self.logger.error(f"Error loading data: {e}")
            return
        finally:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
                           


# print("PYTHONPATH =", os.environ.get("PYTHONPATH"))
from internal.models import *
import  airflow.providers.standard.operators  as ops

class SparkTransformOperator(ops.SparkSubmitOperator):
    """Custom Spark operator cho data transformation"""
    
    def __init__(
        self,
        pipeline_config,
        transform_script: str,
        transform_type: str,
        input_data_path: str = None,
        output_data_path: str = None,
        **kwargs
    ):
        spark_config = pipeline_config.get_spark_config()
        
        # Build Spark submit arguments
        application_args = [
            '--transform-type', transform_type,
            '--config-path', './internal/config/data_craw_web_config/data_craw_web_config.yaml'
        ]
        
        if input_data_path:
            application_args.extend(['--input-path', input_data_path])
        if output_data_path:
            application_args.extend(['--output-path', output_data_path])
        
        super().__init__(
            application=transform_script,
            name=f"transform_{transform_type}",
            conf=spark_config['conf'],
            packages=','.join(spark_config['packages']),
            application_args=application_args,
            **kwargs
        )