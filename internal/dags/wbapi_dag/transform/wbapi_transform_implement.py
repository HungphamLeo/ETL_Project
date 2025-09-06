"""
WorldBank Transform Service - Refactored from existing code
Maintains compatibility with existing transform objects while adding Airflow/Spark support
"""

import wbgapi as wb
from cmd_.load_config import load_config
from src.logger import FastLogger
from internal.dags.wbapi_dag.transform.wbapi_transform_obj import (
    TransformEconomy, TransformTopic, TransformSeries, TransformTime, 
    TransformSource, TransformRegion, TransformIncome, TransformLending
)
from internal.models import *
import airflow.providers.standard.operators as ops
import os
from typing import Dict, Any, Optional, List
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pyspark.sql import SparkSession


class wbapi_transform:
    """Main transform service - refactored to support both local and Spark processing"""
    
    def __init__(self, pipeline_config=None):
        # Keep existing structure
        self.economy = TransformEconomy()
        self.topic = TransformTopic()
        self.series = TransformSeries()
        self.time = TransformTime()
        self.source = TransformSource()
        self.region = TransformRegion() 
        self.income = TransformIncome()
        self.lending = TransformLending()
        
        # Add pipeline config support
        self.pipeline_config = pipeline_config
        self.logger = FastLogger(load_config()).get_logger()
        self._spark_session = None
    
    def get_or_create_spark_session(self):
        """Get or create Spark session for large dataset processing"""
        if self._spark_session is None:
            if self.pipeline_config:
                spark_config = self.pipeline_config.get_spark_config()
                builder = SparkSession.builder.appName(spark_config.get('app_name', 'WorldBank_Transform'))
                
                # Apply Spark configurations
                for key, value in spark_config.get('conf', {}).items():
                    builder = builder.config(key, value)
                
                self._spark_session = builder.getOrCreate()
            else:
                # Default Spark session
                self._spark_session = SparkSession.builder.appName("WorldBank_Transform").getOrCreate()
        
        return self._spark_session
    
    def transform_economy_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform economy data - wrapper for existing methods"""
        spark = self.get_or_create_spark_session()
        result = {}
        
        try:
            if 'dataframe' in raw_data and raw_data['dataframe'] is not None:
                result['economy_data'] = self.economy.transform_economy_dataframe(
                    spark, raw_data['dataframe']
                )
            
            if 'metadata' in raw_data and raw_data['metadata'] is not None:
                result['economy_metadata'] = self.economy.transform_economy_metadata(
                    spark, raw_data['metadata']
                )
            
            self.logger.info("Economy data transformation completed")
            return result
            
        except Exception as e:
            self.logger.error(f"Economy transformation failed: {e}")
            raise
    
    def transform_series_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform series data - wrapper for existing methods"""
        spark = self.get_or_create_spark_session()
        result = {}
        
        try:
            if 'dataframe' in raw_data and raw_data['dataframe'] is not None:
                result['series_data'] = self.series.transform_series_dataframe(
                    spark, raw_data['dataframe']
                )
            
            if 'metadata' in raw_data and raw_data['metadata'] is not None:
                result['series_metadata'] = self.series.transform_series_metadata(
                    raw_data['metadata']
                )
            
            self.logger.info("Series data transformation completed")
            return result
            
        except Exception as e:
            self.logger.error(f"Series transformation failed: {e}")
            raise
    
    def transform_topic_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform topic data - wrapper for existing methods"""
        spark = self.get_or_create_spark_session()
        result = {}
        
        try:
            if 'info' in raw_data and raw_data['info'] is not None:
                result['topic_info'] = self.topic.transform_topic_info_dataframe(
                    spark, raw_data['info']
                )
            
            if 'series' in raw_data and raw_data['series'] is not None:
                result['topic_series'] = self.topic.transform_topic_series_dataframe(
                    spark, raw_data['series']
                )
            
            if 'members' in raw_data and raw_data['members'] is not None:
                result['topic_metadata'] = self.topic.transform_topic_metadata_dataframe(
                    spark, raw_data['members']
                )
            
            self.logger.info("Topic data transformation completed")
            return result
            
        except Exception as e:
            self.logger.error(f"Topic transformation failed: {e}")
            raise
    
    def transform_time_data(self, raw_data: Any) -> Dict[str, Any]:
        """Transform time data - wrapper for existing methods"""
        spark = self.get_or_create_spark_session()
        
        try:
            result = {
                'time_series': self.time.transform_time_dataframe(spark, raw_data)
            }
            
            self.logger.info("Time data transformation completed")
            return result
            
        except Exception as e:
            self.logger.error(f"Time transformation failed: {e}")
            raise
    
    def transform_source_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform source data - wrapper for existing methods"""
        spark = self.get_or_create_spark_session()
        result = {}
        
        try:
            if 'info' in raw_data and raw_data['info'] is not None:
                result['source_info'] = self.source.transform_source_info_dataframe(
                    spark, raw_data['info']
                )
            
            if 'series' in raw_data and raw_data['series'] is not None:
                result['source_series'] = self.source.transform_source_series_dataframe(
                    spark, raw_data['series']
                )
            
            self.logger.info("Source data transformation completed")
            return result
            
        except Exception as e:
            self.logger.error(f"Source transformation failed: {e}")
            raise
    
    def transform_region_data(self, raw_data: Any) -> Dict[str, Any]:
        """Transform region data - wrapper for existing methods"""
        spark = self.get_or_create_spark_session()
        
        try:
            result = {
                'region_info': self.region.transform_region_dataframe(spark, raw_data)
            }
            
            self.logger.info("Region data transformation completed")
            return result
            
        except Exception as e:
            self.logger.error(f"Region transformation failed: {e}")
            raise
    
    def transform_income_data(self, raw_data: Any) -> Dict[str, Any]:
        """Transform income data - wrapper for existing methods"""
        spark = self.get_or_create_spark_session()
        
        try:
            result = {
                'income_series': self.income.transform_income_dataframe(spark, raw_data)
            }
            
            self.logger.info("Income data transformation completed")
            return result
            
        except Exception as e:
            self.logger.error(f"Income transformation failed: {e}")
            raise
    
    def transform_lending_data(self, raw_data: Any) -> Dict[str, Any]:
        """Transform lending data - wrapper for existing methods"""
        spark = self.get_or_create_spark_session()
        
        try:
            result = {
                'lending_series': self.lending.transform_lending_dataframe(spark, raw_data)
            }
            
            self.logger.info("Lending data transformation completed")
            return result
            
        except Exception as e:
            self.logger.error(f"Lending transformation failed: {e}")
            raise
    
    def transform_all_data(self, raw_data_collection: Dict[str, Any]) -> Dict[str, Any]:
        """Transform all data types - main orchestration method"""
        try:
            transformed_collection = {}
            
            # Transform mapping
            transform_methods = {
                'economy': self.transform_economy_data,
                'series': self.transform_series_data,
                'topic': self.transform_topic_data,
                'time': self.transform_time_data,
                'source': self.transform_source_data,
                'region': self.transform_region_data,
                'income': self.transform_income_data,
                'lending': self.transform_lending_data
            }
            
            for data_type, raw_data in raw_data_collection.items():
                if raw_data is None:
                    self.logger.warning(f"No data provided for {data_type}, skipping")
                    continue
                
                if data_type in transform_methods:
                    transformed_data = transform_methods[data_type](raw_data)
                    transformed_collection.update(transformed_data)
                else:
                    self.logger.warning(f"Unknown data type: {data_type}")
            
            self.logger.info(f"Successfully transformed {len(transformed_collection)} data types")
            return transformed_collection
            
        except Exception as e:
            self.logger.error(f"Batch transformation failed: {e}")
            raise
        finally:
            # Clean up Spark session if created
            if self._spark_session:
                self._spark_session.stop()
                self._spark_session = None


class SparkTransformOperator(SparkSubmitOperator):
    """Custom Spark operator for WorldBank data transformation"""
    
    SUPPORTED_TRANSFORMS = [
        'economy', 'series', 'topic', 'time', 
        'source', 'region', 'income', 'lending'
    ]
    
    def __init__(
        self,
        pipeline_config,
        transform_type: str,
        transform_script: Optional[str] = None,
        input_data_path: Optional[str] = None,
        output_data_path: Optional[str] = None,
        spark_config_override: Optional[Dict] = None,
        **kwargs
    ):
        if transform_type not in self.SUPPORTED_TRANSFORMS:
            raise ValueError(f"Unsupported transform type: {transform_type}. "
                           f"Supported types: {self.SUPPORTED_TRANSFORMS}")
        
        self.pipeline_config = pipeline_config
        self.transform_type = transform_type
        
        # Get Spark configuration
        spark_config = self._build_spark_config(spark_config_override)
        
        # Determine transform script path
        if not transform_script:
            transform_script = self._get_default_transform_script()
        
        # Build application arguments
        application_args = self._build_application_args(
            input_data_path, output_data_path
        )
        
        super().__init__(
            application=transform_script,
            name=f"worldbank_transform_{transform_type}",
            conf=spark_config['conf'],
            packages=spark_config.get('packages', []),
            application_args=application_args,
            conn_id=spark_config.get('conn_id', 'spark_default'),
            executor_cores=spark_config.get('executor_cores', 2),
            executor_memory=spark_config.get('executor_memory', '2g'),
            driver_memory=spark_config.get('driver_memory', '1g'),
            num_executors=spark_config.get('num_executors', 2),
            verbose=spark_config.get('verbose', True),
            **kwargs
        )

    def _build_spark_config(self, config_override: Optional[Dict] = None) -> Dict[str, Any]:
        """Build Spark configuration with overrides"""
        base_config = self.pipeline_config.get_spark_config() if self.pipeline_config else {}
        
        default_spark_config = {
            'conf': {
                'spark.sql.adaptive.enabled': 'true',
                'spark.sql.adaptive.coalescePartitions.enabled': 'true',
                'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
                'spark.sql.execution.arrow.pyspark.enabled': 'true',
                'spark.sql.adaptive.advisoryPartitionSizeInBytes': '128MB',
                **base_config.get('conf', {})
            },
            'packages': [
                'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0',
                'mysql:mysql-connector-java:8.0.33',
                *base_config.get('packages', [])
            ],
            'executor_cores': 2,
            'executor_memory': '2g',
            'driver_memory': '1g',
            'num_executors': 2,
            'conn_id': 'spark_default',
            'verbose': True
        }
        
        spark_config = {**default_spark_config, **base_config}
        if config_override:
            spark_config.update(config_override)
            if 'conf' in config_override:
                spark_config['conf'].update(config_override['conf'])
        
        return spark_config

    def _get_default_transform_script(self) -> str:
        """Get default Spark transformation script path"""
        script_name = f"spark_transform_{self.transform_type}.py"
        script_path = os.path.join(
            os.path.dirname(__file__), 
            'spark_scripts', 
            script_name
        )
        
        if not os.path.exists(script_path):
            script_path = os.path.join(
                os.path.dirname(__file__), 
                'spark_scripts', 
                'generic_worldbank_transform.py'
            )
        
        return script_path

    def _build_application_args(
        self, 
        input_data_path: Optional[str] = None,
        output_data_path: Optional[str] = None
    ) -> List[str]:
        """Build Spark application arguments"""
        args = [
            '--transform-type', self.transform_type,
            '--config-path', './internal/config/data_craw_web_config/data_craw_web_config.yaml'
        ]
        
        if input_data_path:
            args.extend(['--input-path', input_data_path])
        else:
            hdfs_config = getattr(self.pipeline_config, 'config', {}).get('hdfs', {})
            default_input = f"{hdfs_config.get('data_dir', '/data/worldbank')}/raw/{self.transform_type}"
            args.extend(['--input-path', default_input])
        
        if output_data_path:
            args.extend(['--output-path', output_data_path])
        else:
            hdfs_config = getattr(self.pipeline_config, 'config', {}).get('hdfs', {})
            default_output = f"{hdfs_config.get('data_dir', '/data/worldbank')}/transformed/{self.transform_type}"
            args.extend(['--output-path', default_output])
        
        if self.pipeline_config and hasattr(self.pipeline_config, 'is_production'):
            if self.pipeline_config.is_production():
                args.extend(['--environment', 'production'])
            else:
                args.extend(['--environment', 'development'])
        
        return args

    def execute(self, context):
        """Execute Spark transformation with enhanced logging"""
        if hasattr(self.pipeline_config, 'logger'):
            self.pipeline_config.logger.info(
                f"Starting Spark transformation for {self.transform_type}"
            )
        
        try:
            result = super().execute(context)
            
            if hasattr(self.pipeline_config, 'logger'):
                self.pipeline_config.logger.info(
                    f"Spark transformation completed successfully for {self.transform_type}"
                )
            
            return result
            
        except Exception as e:
            if hasattr(self.pipeline_config, 'logger'):
                self.pipeline_config.logger.error(
                    f"Spark transformation failed for {self.transform_type}: {e}"
                )
            raise


class SparkTransformFactory:
    """Factory for creating Spark transform operators"""
    
    @staticmethod
    def create_economy_transform(pipeline_config, **kwargs) -> SparkTransformOperator:
        return SparkTransformOperator(
            pipeline_config=pipeline_config,
            transform_type='economy',
            task_id='spark_transform_economy',
            **kwargs
        )
    
    @staticmethod
    def create_series_transform(pipeline_config, **kwargs) -> SparkTransformOperator:
        return SparkTransformOperator(
            pipeline_config=pipeline_config,
            transform_type='series',
            task_id='spark_transform_series',
            **kwargs
        )
    
    @staticmethod
    def create_all_transforms(pipeline_config, **kwargs) -> List[SparkTransformOperator]:
        operators = []
        for transform_type in SparkTransformOperator.SUPPORTED_TRANSFORMS:
            operator = SparkTransformOperator(
                pipeline_config=pipeline_config,
                transform_type=transform_type,
                task_id=f'spark_transform_{transform_type}',
                **kwargs
            )
            operators.append(operator)
        return operators


class SparkTransformConfig:
    """Configuration builder for Spark transforms"""
    
    @staticmethod
    def for_small_dataset() -> Dict[str, Any]:
        return {
            'executor_cores': 1,
            'executor_memory': '1g',
            'driver_memory': '512m',
            'num_executors': 1,
            'conf': {
                'spark.sql.adaptive.advisoryPartitionSizeInBytes': '64MB'
            }
        }
    
    @staticmethod
    def for_large_dataset() -> Dict[str, Any]:
        return {
            'executor_cores': 4,
            'executor_memory': '4g',
            'driver_memory': '2g',
            'num_executors': 4,
            'conf': {
                'spark.sql.adaptive.advisoryPartitionSizeInBytes': '256MB',
                'spark.sql.adaptive.maxRecordsPerBatch': '10000'
            }
        }
    
    @staticmethod
    def for_streaming() -> Dict[str, Any]:
        return {
            'executor_cores': 2,
            'executor_memory': '2g',
            'driver_memory': '1g',
            'num_executors': 3,
            'conf': {
                'spark.streaming.kafka.maxRatePerPartition': '1000',
                'spark.sql.streaming.checkpointLocation': '/tmp/spark-checkpoint'
            }
        }