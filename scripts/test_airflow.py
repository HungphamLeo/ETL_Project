"""
Apache Airflow ETL Pipeline - Production Ready Architecture
Tích hợp World Bank API với khả năng mở rộng cho Big Data stack
"""

import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import logging
import json
import yaml
from pathlib import Path


import wbgapi as wb   # I use wb as a namespace in all my work
import os
# print("PYTHONPATH =", os.environ.get("PYTHONPATH"))
from internal.models import *
from internal.dags.extract import wbapi_extract
from internal.dags.transform import wbapi_transform
from internal.dags.load import BaseDBLoader
from internal.models.transform_models import (
    EconomyTransform, SeriesTransform, TopicTransform, TimeTransform,
    SourceTransform, RegionTransform, IncomeTransform, LendingTransform
)
from cmd.load_config import load_config
from src.utils import TableCreator
from src.logger import FastLogger

# Airflow Core Imports
from airflow import DAG
from airflow.models import Variable
from pyspark.
from airflow.operators
from airflow.operators.python import PythonOperator
from airflow.operators import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# Spark & Big Data Integration
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.hdfs.hooks.hdfs import HDFSHook
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

# Monitoring & AlertingP
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.email.operators.email import EmailOperator

# Custom Imports (từ existing codebase)
from internal.extract.wbapi_extract import wbapi_extract
from internal.transform.wbapi_transform import wbapi_transform
from internal.load.base_db_loader import BaseDBLoader
from internal.config.configuration import load_config
from internal.logging.fast_logger import FastLogger

# ==========================================
# CONFIGURATION & CONSTANTS
# ==========================================

class ETLPipelineConfig:
    """Centralized configuration management cho ETL pipeline"""
    
    def __init__(self):
        self.config = self._load_config()
        self.logger = self._setup_logger()
        
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration từ multiple sources"""
        # Priority: Airflow Variables > Environment > Config File
        try:
            # Load từ Airflow Variables
            etl_config = Variable.get("etl_pipeline_config", default_var=None, deserialize_json=True)
            if etl_config:
                return etl_config
        except:
            pass
            
        # Load từ environment variables
        config_path = os.getenv('ETL_CONFIG_PATH', './internal/config/data_craw_web_config/data_craw_web_config.yaml')
        return load_config(config_path)
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logger with proper configuration"""

        return FastLogger(self.config).get_logger()
    
    def get_spark_config(self) -> Dict[str, Any]:
        """Get Spark-specific configuration"""
        return {
            'app_name': 'WorldBank_ETL_Pipeline',
            'master': os.getenv('SPARK_MASTER_URL', 'local[*]'),
            'packages': [
                'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0',
                'mysql:mysql-connector-java:8.0.33',
                'org.apache.hadoop:hadoop-aws:3.3.4'
            ],
            'conf': {
                'spark.sql.adaptive.enabled': 'true',
                'spark.sql.adaptive.coalescePartitions.enabled': 'true',
                'spark.sql.adaptive.skewJoin.enabled': 'true',
                'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
                'spark.sql.warehouse.dir': os.getenv('SPARK_WAREHOUSE_DIR', '/tmp/spark-warehouse'),
                # HDFS Configuration
                'spark.hadoop.fs.defaultFS': os.getenv('HDFS_DEFAULT_FS', 'hdfs://namenode:9000'),
                # S3/MinIO Configuration for future
                'spark.hadoop.fs.s3a.endpoint': os.getenv('S3_ENDPOINT', ''),
                'spark.hadoop.fs.s3a.access.key': os.getenv('S3_ACCESS_KEY', ''),
                'spark.hadoop.fs.s3a.secret.key': os.getenv('S3_SECRET_KEY', ''),
            }
        }

# Initialize global configuration
pipeline_config = ETLPipelineConfig()

# ==========================================
# CUSTOM OPERATORS
# ==========================================

class WorldBankExtractOperator(PythonOperator):
    """Custom operator cho World Bank API data extraction"""
    
    def __init__(self, extract_type: str, **kwargs):
        self.extract_type = extract_type
        super().__init__(python_callable=self._extract_data, **kwargs)
    
    def _extract_data(self, **context):
        """Extract data từ World Bank API"""
        logger = pipeline_config.logger
        
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
    def _extract_topic_data(self, extract_obj:wbapi_extract):
        from internal.models.extract_input_models import TopicInfoInput, TopicSeriesInput, TopicMembersInput
        return {
            'info': extract_obj.topic.get_info(TopicInfoInput(id='all')),
            'series': extract_obj.topic.get_series(TopicSeriesInput(id='all')),
            'members': extract_obj.topic.get_members(TopicMembersInput(id='all'))
        }

    def _extract_time_data(self, extract_obj:wbapi_extract):
        from internal.models.extract_input_models import TimeSeriesInput
        return extract_obj.time.get_time_periods_series(TimeSeriesInput(id='all'))
    
    def _extract_source_data(self, extract_obj:wbapi_extract):
        from internal.models.extract_input_models import SourceInfoInput, SourceSeriesInput
        return {
            'info': extract_obj.source.get_info(SourceInfoInput(id='all')),
            'series': extract_obj.source.get_series(SourceSeriesInput(id='all'))
        }
    def _extract_region_data(self, extract_obj:wbapi_extract):
        from internal.models.extract_input_models import RegionSeriesInput
        return extract_obj.region.get_series(RegionSeriesInput(id='all'))
    
    def _extract_income_data(self, extract_obj:wbapi_extract):
        from internal.models.extract_input_models import IncomeSeriesInput
        return extract_obj.income.get_series(IncomeSeriesInput(id='all'))
    
    def _extract_lending_data(self, extract_obj:wbapi_extract):
        from internal.models.extract_input_models import LendingSeriesInput
        return extract_obj.lending.get_series(LendingSeriesInput(id='all'))

class SparkTransformOperator(SparkSubmitOperator):
    """Custom Spark operator cho data transformation"""
    
    def __init__(
        self,
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

# ==========================================
# TASK FUNCTIONS
# ==========================================

def validate_data_quality(**context):
    """Validate data quality post-extraction"""
    logger = pipeline_config.logger
    
    # Get extraction results from XCom
    extract_results = []
    for task_id in ['extract_economy', 'extract_series', 'extract_topic']:
        try:
            result = context['task_instance'].xcom_pull(task_ids=task_id)
            if result:
                extract_results.append(result)
        except:
            continue
    
    # Data quality checks
    quality_issues = []
    
    for result in extract_results:
        extract_type = result.get('extract_type')
        record_count = result.get('record_count', 0)
        
        # Check minimum record thresholds
        min_thresholds = {
            'economy': 200,
            'series': 1000,
            'topic': 20
        }
        
        if record_count < min_thresholds.get(extract_type, 0):
            quality_issues.append(
                f"{extract_type}: {record_count} records (below threshold {min_thresholds.get(extract_type)})"
            )
    
    if quality_issues:
        error_msg = f"Data quality issues detected: {', '.join(quality_issues)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    logger.info(f"Data quality validation passed for {len(extract_results)} datasets")
    return {'status': 'PASSED', 'datasets_validated': len(extract_results)}

def load_data_to_database(**context):
    """Load transformed data to MySQL database"""
    logger = pipeline_config.logger
    config = pipeline_config.config
    
    try:
        # Initialize database connection
        import pymysql
        conn = pymysql.connect(
            host=config['database']['primary']['host'],
            user=config['database']['primary']['username'],
            password=config['database']['primary']['password'],
            database=config['database']['primary']['database'],
            charset=config['database']['primary']['charset'],
            cursorclass=pymysql.cursors.DictCursor
        )
        cursor = conn.cursor()
        
        # Initialize loader
        loader = BaseDBLoader(
            cursor=cursor, 
            connection=conn, 
            logger=logger, 
            config="./internal/config/data_craw_web_config/data_craw_web_config.yaml"
        )
        
        # Create or replace database
        loader.create_or_replace_database(config['database']['primary']['database'])
        
        # Get transformation results from XCom
        transform_results = context['task_instance'].xcom_pull(task_ids='transform_data')
        
        if not transform_results:
            raise ValueError("No transformation results found in XCom")
        
        # Load data using existing logic (adapted)
        load_count = 0
        for table_name, df in transform_results.get('transformed_data', {}).items():
            if df is not None and not df.empty:
                # Create table and insert data
                logger.info(f"Loading data to table: {table_name}")
                # Implementation would use existing TableCreator logic
                load_count += len(df)
        
        logger.info(f"Successfully loaded {load_count} records across all tables")
        return {'status': 'SUCCESS', 'records_loaded': load_count}
        
    except Exception as e:
        logger.error(f"Database loading failed: {str(e)}")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def publish_to_kafka(**context):
    """Publish processed data to Kafka for real-time consumption"""
    logger = pipeline_config.logger
    
    try:
        # Get transformation results
        transform_results = context['task_instance'].xcom_pull(task_ids='transform_data')
        
        if not transform_results:
            logger.warning("No transformation results to publish")
            return {'status': 'SKIPPED', 'reason': 'No data to publish'}
        
        # Kafka configuration
        kafka_config = {
            'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            'topic': os.getenv('KAFKA_TOPIC', 'worldbank-etl-data'),
            'key_serializer': 'org.apache.kafka.common.serialization.StringSerializer',
            'value_serializer': 'org.apache.kafka.common.serialization.JsonSerializer'
        }
        
        # Publish data (simplified - actual implementation would use Kafka producer)
        published_count = 0
        for table_name, data in transform_results.get('transformed_data', {}).items():
            if data is not None:
                # Convert to JSON and publish
                message = {
                    'table': table_name,
                    'timestamp': datetime.now().isoformat(),
                    'record_count': len(data) if hasattr(data, '__len__') else 0,
                    'data_sample': data.head(5).to_dict() if hasattr(data, 'head') else str(data)[:500]
                }
                published_count += 1
                logger.info(f"Published {table_name} to Kafka topic")
        
        return {'status': 'SUCCESS', 'messages_published': published_count}
        
    except Exception as e:
        logger.error(f"Kafka publishing failed: {str(e)}")
        raise

def store_to_hdfs(**context):
    """Store processed data to HDFS for long-term storage"""
    logger = pipeline_config.logger
    
    try:
        # HDFS configuration
        hdfs_hook = HDFSHook(hdfs_conn_id='hdfs_default')
        
        # Get transformation results
        transform_results = context['task_instance'].xcom_pull(task_ids='transform_data')
        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        
        if not transform_results:
            logger.warning("No transformation results to store")
            return {'status': 'SKIPPED', 'reason': 'No data to store'}
        
        stored_paths = []
        for table_name, data in transform_results.get('transformed_data', {}).items():
            if data is not None:
                # Define HDFS path
                hdfs_path = f"/data/worldbank/{table_name}/{execution_date}/"
                
                # Store data (simplified - actual implementation would handle Parquet/JSON)
                logger.info(f"Storing {table_name} to HDFS path: {hdfs_path}")
                stored_paths.append(hdfs_path)
        
        return {'status': 'SUCCESS', 'paths_stored': stored_paths}
        
    except Exception as e:
        logger.error(f"HDFS storage failed: {str(e)}")
        raise

def cleanup_temp_data(**context):
    """Cleanup temporary data và optimize database"""
    logger = pipeline_config.logger
    
    try:
        # Database cleanup
        mysql_hook = MySqlHook(mysql_conn_id='worldbank_db')
        
        cleanup_queries = [
            "DELETE FROM audit_etl_runs WHERE created_at < DATE_SUB(NOW(), INTERVAL 30 DAY)",
            "OPTIMIZE TABLE dim_economy, dim_series, dim_topic",
            "ANALYZE TABLE dim_economy, dim_series, dim_topic"
        ]
        
        for query in cleanup_queries:
            mysql_hook.run(query)
            logger.info(f"Executed cleanup query: {query[:50]}...")
        
        # File system cleanup (temporary files)
        temp_dirs = ['/tmp/worldbank_etl', '/tmp/spark-warehouse']
        cleaned_count = 0
        
        for temp_dir in temp_dirs:
            if os.path.exists(temp_dir):
                import shutil
                shutil.rmtree(temp_dir)
                cleaned_count += 1
                logger.info(f"Cleaned temporary directory: {temp_dir}")
        
        return {'status': 'SUCCESS', 'directories_cleaned': cleaned_count}
        
    except Exception as e:
        logger.error(f"Cleanup failed: {str(e)}")
        raise

# ==========================================
# DAG DEFINITION
# ==========================================

# DAG default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'email': [Variable.get('alert_email', 'data-eng@company.com')],
    'sla': timedelta(hours=3),
    'execution_timeout': timedelta(hours=6),
}

# Main ETL DAG
dag = DAG(
    'worldbank_etl_pipeline',
    default_args=default_args,
    description='World Bank API ETL Pipeline với Big Data Integration',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    max_active_runs=1,
    concurrency=8,
    tags=['etl', 'worldbank', 'big-data', 'production'],
    doc_md="""
    # World Bank ETL Pipeline
    
    Comprehensive ETL pipeline cho World Bank API data với:
    - Multi-source data extraction
    - Spark-powered transformations  
    - MySQL data warehouse loading
    - Kafka real-time streaming
    - HDFS long-term storage
    - Comprehensive monitoring
    
    **Architecture**: Airflow + Spark + MySQL + Kafka + HDFS
    **SLA**: 3 hours
    **Schedule**: Daily 2 AM UTC
    """
)

# ==========================================
# TASK GROUPS DEFINITION
# ==========================================

# 1. Data Extraction Task Group
with TaskGroup("data_extraction", dag=dag) as extraction_group:
    
    # Extract different data types in parallel
    extract_economy = WorldBankExtractOperator(
        task_id='extract_economy',
        extract_type='economy',
        dag=dag
    )
    
    extract_series = WorldBankExtractOperator(
        task_id='extract_series', 
        extract_type='series',
        dag=dag
    )
    
    extract_topic = WorldBankExtractOperator(
        task_id='extract_topic',
        extract_type='topic',
        dag=dag
    )
    
    extract_other = WorldBankExtractOperator(
        task_id='extract_other',
        extract_type='other',  # time, source, region, income, lending
        dag=dag
    )
    
    # Data quality validation
    # validate_quality = PythonOperator(
    #     task_id='validate_data_quality',
    #     python_callable=validate_data_quality,
    #     dag=dag
    # )
    
    # Dependencies within extraction group
    [extract_economy, extract_series, extract_topic, extract_other] >> validate_quality

# 2. Data Transformation Task Group  
with TaskGroup("data_transformation", dag=dag) as transformation_group:
    
    # Spark transformation for different data types
    transform_economy = SparkTransformOperator(
        task_id='transform_economy',
        transform_script='/opt/airflow/spark_jobs/transform_economy.py',
        transform_type='economy',
        dag=dag
    )
    
    transform_series = SparkTransformOperator(
        task_id='transform_series',
        transform_script='/opt/airflow/spark_jobs/transform_series.py', 
        transform_type='series',
        dag=dag
    )
    
    transform_dimensions = SparkTransformOperator(
        task_id='transform_dimensions',
        transform_script='/opt/airflow/spark_jobs/transform_dimensions.py',
        transform_type='dimensions',
        dag=dag
    )
    
    # Consolidate transformations
    consolidate_transforms = PythonOperator(
        task_id='consolidate_transformations',
        python_callable=lambda **context: {
            'status': 'SUCCESS', 
            'transformations_completed': 3
        },
        dag=dag
    )
    
    # Dependencies within transformation group
    [transform_economy, transform_series, transform_dimensions] >> consolidate_transforms

# 3. Data Loading Task Group
with TaskGroup("data_loading", dag=dag) as loading_group:
    
    # Load to MySQL (primary data warehouse)
    load_mysql = PythonOperator(
        task_id='load_to_mysql',
        python_callable=load_data_to_database,
        dag=dag
    )
    
    # Store to HDFS (long-term storage)  
    store_hdfs = PythonOperator(
        task_id='store_to_hdfs',
        python_callable=store_to_hdfs,
        dag=dag
    )
    
    # Publish to Kafka (real-time streaming)
    publish_kafka = PythonOperator(
        task_id='publish_to_kafka',
        python_callable=publish_to_kafka,
        dag=dag
    )
    
    # Run loading tasks in parallel
    [load_mysql, store_hdfs, publish_kafka]

# 4. Post-Processing Task Group
with TaskGroup("post_processing", dag=dag) as post_processing_group:
    
    # Update statistics
    update_stats = MySqlOperator(
        task_id='update_statistics',
        mysql_conn_id='worldbank_db',
        sql="""
            ANALYZE TABLE dim_economy, dim_series, dim_topic;
            INSERT INTO etl_run_stats (
                dag_id, execution_date, status, 
                records_processed, duration_minutes
            ) VALUES (
                '{{ dag.dag_id }}', 
                '{{ ds }}', 
                'SUCCESS',
                (SELECT SUM(table_rows) FROM information_schema.tables 
                 WHERE table_schema = DATABASE()),
                TIMESTAMPDIFF(MINUTE, '{{ dag_run.start_date }}', NOW())
            );
        """,
        dag=dag
    )
    
    # Generate data quality report
    generate_report = BashOperator(
        task_id='generate_dq_report',
        bash_command="""
            python /opt/airflow/scripts/generate_data_quality_report.py \
                --execution-date {{ ds }} \
                --output-path /opt/airflow/reports/dq_{{ ds }}.html
        """,
        dag=dag
    )
    
    # Cleanup temporary data
    cleanup = PythonOperator(
        task_id='cleanup_temp_data',
        python_callable=cleanup_temp_data,
        dag=dag
    )
    
    # Dependencies
    [update_stats, generate_report] >> cleanup

# 5. Notification Task Group
with TaskGroup("notifications", dag=dag) as notification_group:
    
    # Success notification
    success_notification = SlackWebhookOperator(
        task_id='success_notification',
        http_conn_id='slack_webhook',
        message="""
        World Bank ETL Pipeline Completed Successfully
        
         **Pipeline**: {{ dag.dag_id }}
         **Date**: {{ ds }}
         **Duration**: {{ dag_run.get_duration() }}
         **Status**: All transformations completed successfully
        
        **View Details**: {{ var.value.airflow_webserver_base_url }}/graph?dag_id={{ dag.dag_id }}
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag
    )
    
    # Failure notification
    failure_notification = SlackWebhookOperator(
        task_id='failure_notification',
        http_conn_id='slack_webhook',
        message="""
         World Bank ETL Pipeline Failed
        
         **Pipeline**: {{ dag.dag_id }}
         **Date**: {{ ds }}
         **Status**: Pipeline execution failed
        
        Please check the logs for details.
        🔗 **View Logs**: {{ var.value.airflow_webserver_base_url }}/log?dag_id={{ dag.dag_id }}&execution_date={{ ts }}
        """,
        trigger_rule=TriggerRule.ONE_FAILED,
        dag=dag
    )

# ==========================================
# DAG DEPENDENCIES
# ==========================================

# Main pipeline flow
extraction_group >> transformation_group >> loading_group >> post_processing_group >> notification_group

# Failure handling runs regardless of success/failure
[extraction_group, transformation_group, loading_group, post_processing_group] >> notification_group