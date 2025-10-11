# """
# WorldBank ETL DAG - Complete ETL Pipeline using refactored components
# Deploy this file to your Airflow DAGs folder
# """

# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.operators.bash_operator import BashOperator
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.utils.trigger_rule import TriggerRule

# # Import our refactored components
# from internal.dags.wbapi_dag.extract.worldbank_extract_operator import WorldBankExtractOperator
# from internal.dags.wbapi_dag.transform.worldbank_transform_service import WorldBankTransformService
# from internal.dags.wbapi_dag.load.db_loader_service import DatabaseLoaderService
# from internal.dags.wbapi_dag.config.pipeline_config_security import get_pipeline_config


# def extract_worldbank_data(**context):
#     """Extract data from WorldBank API"""
#     try:
#         config = get_pipeline_config()
#         extract_type = context['params']['extract_type']
        
#         # Create extract operator
#         extractor = WorldBankExtractOperator(
#             pipeline_config=config,
#             extract_type=extract_type,
#             params=context.get('params', {})
#         )
        
#         # Execute extraction
#         result = extractor._extract_data(**context)
        
#         # Store in XCom for next task
#         context['task_instance'].xcom_push(
#             key=f'{extract_type}_data',
#             value=result
#         )
        
#         config.logger.info(f"Extracted {result['record_count']} records for {extract_type}")
#         return result
        
#     except Exception as e:
#         config.logger.error(f"Extraction failed: {e}")
#         raise


# def transform_worldbank_data(**context):
#     """Transform extracted data"""
#     try:
#         config = get_pipeline_config()
        
#         # Get all extracted data from XCom
#         extract_types = ['economy', 'series', 'topic', 'time', 'source', 'region', 'income', 'lending']
#         raw_data = {}
        
#         for extract_type in extract_types:
#             data = context['task_instance'].xcom_pull(
#                 task_ids=f'extract_{extract_type}',
#                 key=f'{extract_type}_data'
#             )
#             if data:
#                 raw_data[extract_type] = data['data']
        
#         # Transform data
#         transformer = WorldBankTransformService(config)
#         transformed_data = transformer.transform_all_data(raw_data)
        
#         # Store transformed data
#         context['task_instance'].xcom_push(
#             key='transformed_data',
#             value=transformed_data
#         )
        
#         config.logger.info(f"Transformed {len(transformed_data)} data types")
#         return transformed_data
        
#     except Exception as e:
#         config.logger.error(f"Transformation failed: {e}")
#         raise


# def load_worldbank_data(**context):
#     """Load transformed data to database"""
#     try:
#         config = get_pipeline_config()
        
#         # Get transformed data from XCom
#         transformed_data = context['task_instance'].xcom_pull(
#             task_ids='transform_data',
#             key='transformed_data'
#         )
        
#         if not transformed_data:
#             raise ValueError("No transformed data available")
        
#         # Load data using context manager
#         with DatabaseLoaderService(config) as loader:
#             loader.create_database()
#             loader.load_transformed_data(transformed_data)
        
#         config.logger.info("Data loading completed successfully")
#         return {"status": "success", "loaded_tables": len(transformed_data)}
        
#     except Exception as e:
#         config.logger.error(f"Loading failed: {e}")
#         raise


# def validate_data_quality(**context):
#     """Validate loaded data quality"""
#     try:
#         config = get_pipeline_config()
        
#         # Basic validation queries
#         validation_queries = [
#             "SELECT COUNT(*) as economy_count FROM economy_data",
#             "SELECT COUNT(*) as series_count FROM series_data", 
#             "SELECT COUNT(*) as topic_count FROM topic_info"
#         ]
        
#         with DatabaseLoaderService(config) as loader:
#             results = {}
#             for query in validation_queries:
#                 try:
#                     loader.loader.execute(query)
#                     result = loader.cursor.fetchone()
#                     table_name = query.split('FROM ')[1].split(' ')[0]
#                     results[table_name] = result
#                 except Exception as e:
#                     config.logger.warning(f"Validation query failed: {query} - {e}")
        
#         config.logger.info(f"Data validation completed: {results}")
#         return results
        
#     except Exception as e:
#         config.logger.error(f"Data validation failed: {e}")
#         raise


# def cleanup_temp_data(**context):
#     """Cleanup temporary data and resources"""
#     try:
#         config = get_pipeline_config()
        
#         # Clear XCom data for current DAG run
#         extract_types = ['economy', 'series', 'topic', 'time', 'source', 'region', 'income', 'lending']
#         for extract_type in extract_types:
#             context['task_instance'].xcom_push(key=f'{extract_type}_data', value=None)
        
#         context['task_instance'].xcom_push(key='transformed_data', value=None)
        
#         config.logger.info("Cleanup completed")
#         return {"status": "cleanup_completed"}
        
#     except Exception as e:
#         config.logger.error(f"Cleanup failed: {e}")
#         # Don't raise here - cleanup failures shouldn't break the pipeline


# # DAG Configuration
# default_args = {
#     'owner': 'data-engineering',
#     'depends_on_past': False,
#     'email_on_failure': True,
#     'email_on_retry': False,
#     'retries': 2,
#     'retry_delay': timedelta(minutes=5),
#     'execution_timeout': timedelta(hours=2),
#     'email': ['data-team@company.com']
# }

# # Create DAG
# dag = DAG(
#     'worldbank_etl_pipeline',
#     default_args=default_args,
#     description='WorldBank Data ETL Pipeline',
#     schedule_interval='@daily',  # Run daily
#     start_date=datetime(2024, 1, 1),
#     catchup=False,
#     max_active_runs=1,
#     tags=['etl', 'worldbank', 'data-pipeline']
# )

# # Start task
# start_task = DummyOperator(
#     task_id='start_pipeline',
#     dag=dag
# )

# # Extract tasks for each data type
# extract_tasks = []
# extract_configs = [
#     {
#         'type': 'economy',
#         'params': {
#             'economy_dataframe_id': 'all',
#             'economy_metadata_id': 'all'
#         }
#     },
#     {
#         'type': 'series', 
#         'params': {
#             'series_id': 'all',
#             'series_metadata_id': 'all'
#         }
#     },
#     {
#         'type': 'topic',
#         'params': {'topic_id': 'all'}
#     },
#     {
#         'type': 'time',
#         'params': {'time_id': 'all'}
#     },
#     {
#         'type': 'source',
#         'params': {'source_id': 'all'}
#     },
#     {
#         'type': 'region',
#         'params': {'region_id': 'all'}
#     },
#     {
#         'type': 'income',
#         'params': {'income_id': 'all'}
#     },
#     {
#         'type': 'lending',
#         'params': {'lending_id': 'all'}
#     }
# ]

# for config in extract_configs:
#     extract_task = PythonOperator(
#         task_id=f'extract_{config["type"]}',
#         python_callable=extract_worldbank_data,
#         params={
#             'extract_type': config['type'],
#             **config['params']
#         },
#         dag=dag
#     )
#     extract_tasks.append(extract_task)

# # Transform task
# transform_task = PythonOperator(
#     task_id='transform_data',
#     python_callable=transform_worldbank_data,
#     dag=dag
# )

# # Load task
# load_task = PythonOperator(
#     task_id='load_data',
#     python_callable=load_worldbank_data,
#     dag=dag
# )

# # Data quality validation
# validate_task = PythonOperator(
#     task_id='validate_data_quality',
#     python_callable=validate_data_quality,
#     dag=dag
# )

# # Cleanup task (runs even if other tasks fail)
# cleanup_task = PythonOperator(
#     task_id='cleanup_temp_data',
#     python_callable=cleanup_temp_data,
#     trigger_rule=TriggerRule.ALL_DONE,  # Run regardless of upstream task status
#     dag=dag
# )

# # Success notification
# success_task = BashOperator(
#     task_id='pipeline_success_notification',
#     bash_command='echo "WorldBank ETL Pipeline completed successfully"',
#     dag=dag
# )

# # End task
# end_task = DummyOperator(
#     task_id='end_pipeline',
#     trigger_rule=TriggerRule.ALL_DONE,
#     dag=dag
# )

# # Define task dependencies
# start_task >> extract_tasks
# extract_tasks >> transform_task >> load_task >> validate_task >> success_task
# [success_task, validate_task] >> cleanup_task >> end_task

# # Alternative: For specific country/region extraction
# # You can create separate DAGs or use dynamic DAG generation:

# def create_country_specific_dag(country_code: str, country_name: str):
#     """Create country-specific ETL DAG"""
    
#     country_dag = DAG(
#         f'worldbank_etl_{country_code.lower()}',
#         default_args=default_args,
#         description=f'WorldBank ETL Pipeline for {country_name}',
#         schedule_interval='@weekly',
#         start_date=datetime(2024, 1, 1),
#         catchup=False,
#         tags=['etl', 'worldbank', f'country-{country_code.lower()}']
#     )
    
#     # Country-specific extract task
#     extract_country_task = PythonOperator(
#         task_id=f'extract_economy_{country_code.lower()}',
#         python_callable=extract_worldbank_data,
#         params={
#             'extract_type': 'economy',
#             'economy_dataframe_id': country_code,
#             'economy_metadata_id': country_code
#         },
#         dag=country_dag
#     )
    
#     return country_dag

# # Example: Create Vietnam-specific DAG
# vietnam_dag = create_country_specific_dag('VNM', 'Vietnam')