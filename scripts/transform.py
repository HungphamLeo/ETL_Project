# print("PYTHONPATH =", os.environ.get("PYTHONPATH"))
from internal.models import *
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


class SparkTransformOperator(SparkSubmitOperator):
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