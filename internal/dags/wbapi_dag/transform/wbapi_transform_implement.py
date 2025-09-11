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
    
    def __init__(self, pipeline_config=None, pipeline_logger = None):
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
        self.logger = pipeline_logger
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
        
        default_spark_config = self.pipeline_config.get('default_spark_config', {})
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
            '--config-path', './internal/config/data_craw_web_config/world_bank_config.yaml'
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
            self.pipeline_config.loggerself.logger.info(
                f"Starting Spark transformation for {self.transform_type}"
            )
        
        try:
            result = super().execute(context)
            
            if hasattr(self.pipeline_config, 'logger'):
                self.pipeline_config.loggerself.logger.info(
                    f"Spark transformation completed successfully for {self.transform_type}"
                )
            
            return result
            
        except Exception as e:
            if hasattr(self.pipeline_config, 'logger'):
                self.pipeline_config.loggerself.logger.error(
                    f"Spark transformation failed for {self.transform_type}: {e}"
                )
            raise

class SparkTransformConfig:
    """Configuration builder for Spark transforms, lấy config từ file YAML/dict"""

    def __init__(self, spark_config: dict):
        self.spark_config = spark_config

    def get_default(self) -> dict:
        return self.spark_config.get('default', {})

    def get_small_dataset(self) -> dict:
        return self.spark_config.get('small_dataset', self.get_default())

    def get_large_dataset(self) -> dict:
        return self.spark_config.get('large_dataset', self.get_default())

    def get_streaming(self) -> dict:
        return self.spark_config.get('streaming', self.get_default())

    def get_config(self, mode: str = "default") -> dict:
        return self.spark_config.get(mode, self.get_default())


        
class SparkTransformFactory:
    """Factory for creating Spark transform operators."""

    @staticmethod
    def create_economy_transform(pipeline_config, **kwargs) -> "SparkTransformOperator":
        return SparkTransformOperator(
            pipeline_config=pipeline_config,
            transform_type="economy",
            task_id="spark_transform_economy",
            **kwargs
        )

    @staticmethod
    def create_series_transform(pipeline_config, **kwargs) -> "SparkTransformOperator":
        return SparkTransformOperator(
            pipeline_config=pipeline_config,
            transform_type="series",
            task_id="spark_transform_series",
            **kwargs
        )

    @staticmethod
    def create_topic_transform(pipeline_config, **kwargs) -> "SparkTransformOperator":
        return SparkTransformOperator(
            pipeline_config=pipeline_config,
            transform_type="topic",
            task_id="spark_transform_topic",
            **kwargs
        )

    @staticmethod
    def create_time_transform(pipeline_config, **kwargs) -> "SparkTransformOperator":
        return SparkTransformOperator(
            pipeline_config=pipeline_config,
            transform_type="time",
            task_id="spark_transform_time",
            **kwargs
        )

    @staticmethod
    def create_source_transform(pipeline_config, **kwargs) -> "SparkTransformOperator":
        return SparkTransformOperator(
            pipeline_config=pipeline_config,
            transform_type="source",
            task_id="spark_transform_source",
            **kwargs
        )

    @staticmethod
    def create_region_transform(pipeline_config, **kwargs) -> "SparkTransformOperator":
        return SparkTransformOperator(
            pipeline_config=pipeline_config,
            transform_type="region",
            task_id="spark_transform_region",
            **kwargs
        )

    @staticmethod
    def create_income_transform(pipeline_config, **kwargs) -> "SparkTransformOperator":
        return SparkTransformOperator(
            pipeline_config=pipeline_config,
            transform_type="income",
            task_id="spark_transform_income",
            **kwargs
        )

    @staticmethod
    def create_lending_transform(pipeline_config, **kwargs) -> "SparkTransformOperator":
        return SparkTransformOperator(
            pipeline_config=pipeline_config,
            transform_type="lending",
            task_id="spark_transform_lending",
            **kwargs
        )

    @staticmethod
    def create_all_transforms(pipeline_config, **kwargs) -> List["SparkTransformOperator"]:
        """Create operators for all supported transform types."""
        operators = []
        for transform_type in SparkTransformOperator.SUPPORTED_TRANSFORMS:
            operators.append(
                SparkTransformOperator(
                    pipeline_config=pipeline_config,
                    transform_type=transform_type,
                    task_id=f"spark_transform_{transform_type}",
                    **kwargs
                )
            )
        return operators




class HybridTransformService:
    """Service that intelligently chooses between local and Spark processing"""
    
    def __init__(self, pipeline_config, pipeline_logger):
        self.pipeline_config = pipeline_config
        self.local_transformer = wbapi_transform(pipeline_config)
        self.logger = pipeline_logger
    
    def should_use_spark(self, data_size: int, data_type: str) -> bool:
        """Decide whether to use Spark based on data characteristics"""
        # Configurable thresholds
        size_threshold = self.pipeline_config.config.get('spark_threshold', {}).get(data_type, 100000)
        
        return data_size > size_threshold
    
    def transform_data(self, data_type: str, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform data using optimal processing method"""
        # Estimate data size
        data_size = self._estimate_data_size(raw_data)
        
        if self.should_use_spark(data_size, data_type):
            self.logger.info(f"Using Spark for {data_type} (size: {data_size})")
            return self._transform_with_spark(data_type, raw_data)
        else:
            self.logger.info(f"Using local processing for {data_type} (size: {data_size})")
            return self._transform_locally(data_type, raw_data)
    
    def _estimate_data_size(self, raw_data: Dict[str, Any]) -> int:
        """Estimate the size of raw data"""
        total_size = 0
        for key, value in raw_data.items():
            if hasattr(value, 'shape'):
                total_size += value.shape[0]
            elif hasattr(value, '__len__'):
                total_size += len(value)
        return total_size
    
    def _transform_locally(self, data_type: str, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform using local wbapi_transform"""
        transform_methods = {
            'economy': self.local_transformer.transform_economy_data,
            'series': self.local_transformer.transform_series_data,
            'topic': self.local_transformer.transform_topic_data,
            'time': self.local_transformer.transform_time_data,
            'source': self.local_transformer.transform_source_data,
            'region': self.local_transformer.transform_region_data,
            'income': self.local_transformer.transform_income_data,
            'lending': self.local_transformer.transform_lending_data
        }
        
        if data_type in transform_methods:
            return transform_methods[data_type](raw_data)
        else:
            raise ValueError(f"Unsupported transform type: {data_type}")
    
    def _transform_with_spark(self, data_type: str, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform using Spark (could save to temp file and use SparkTransformOperator)"""
        import tempfile
        import pickle
        
        # Save raw data to temporary file
        with tempfile.NamedTemporaryFile(suffix='.pkl', delete=False) as f:
            pickle.dump(raw_data, f)
            temp_input = f.name
        
        # Create output file path
        temp_output = tempfile.mktemp(suffix='.pkl')
        
        # Create and execute Spark operator
        spark_operator = SparkTransformOperator(
            pipeline_config=self.pipeline_config,
            transform_type=data_type,
            raw_data_path=temp_input,
            output_data_path=temp_output,
            task_id=f'hybrid_spark_transform_{data_type}'
        )
        
        # In a real Airflow context, this would be handled by the scheduler
        # For now, we'll fall back to local processing
        self.logger.warning("Spark execution in hybrid mode not fully implemented, falling back to local")
        return self._transform_locally(data_type, raw_data)


