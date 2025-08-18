class ETLPipelineOrchestrator:
    """Main ETL pipeline orchestrator that coordinates all services"""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_manager = get_config_manager(config_path)
        self.logger = StructuredLogger(__name__).get_logger()
        
        # Initialize all services
        self.quality_validator = DataQualityValidator(self.config_manager)
        self.transformation_service = TransformationService(self.config_manager)
        self.database_loader = DatabaseLoaderService(self.config_manager)
        self.kafka_publisher = KafkaPublisherService(self.config_manager)
        self.hdfs_storage = HDFSStorageService(self.config_manager)
    
    def execute_pipeline(self, context: ETLContext, extract_results: List[Dict[str, Any]]) -> Dict[str, ProcessingResult]:
        """Execute the complete ETL pipeline"""
        pipeline_results = {}
        
        try:
            self.logger.info(f"Starting ETL pipeline execution for {context.dag_id}")
            
            # Step 1: Validate data quality
            self.logger.info("Step 1: Validating data quality...")
            quality_result = self.quality_validator.process(context, extract_results)
            pipeline_results['data_quality'] = quality_result
            
            if not quality_result.success:
                self.logger.error("Data quality validation failed, stopping pipeline")
                return pipeline_results
            
            # Step 2: Transform data
            self.logger.info("Step 2: Transforming data...")
            transform_results = self.transformation_service.process(context, extract_results)
            
            if not transform_results:
                self.logger.error("Data transformation failed, stopping pipeline")
                return pipeline_results
            
            # Step 3: Load to database (primary storage)
            self.logger.info("Step 3: Loading data to database...")
            database_result = self.database_loader.process(context, transform_results)
            pipeline_results['database_load'] = database_result
            
            # Step 4: Publish to Kafka (parallel processing)
            self.logger.info("Step 4: Publishing to Kafka...")
            kafka_result = self.kafka_publisher.process(context, transform_results)
            pipeline_results['kafka_publish'] = kafka_result
            
            # Step 5: Store to HDFS (parallel processing)
            self.logger.info("Step 5: Storing to HDFS...")
            hdfs_result = self.hdfs_storage.process(context, transform_results)
            pipeline_results['hdfs_storage'] = hdfs_result
            
            # Log final results
            total_records = sum(result.records_processed for result in pipeline_results.values())
            self.logger.info(f"ETL pipeline completed successfully: {total_records} records processed")
            
            return pipeline_results
            
        except Exception as e:
            self.logger.error(f"ETL pipeline execution failed: {str(e)}")
            raise


# ==========================================
# AIRFLOW INTEGRATION FUNCTIONS
# ==========================================

def validate_data_quality(**context):
    """Airflow task function for data quality validation"""
    # Extract context information
    etl_context = ETLContext(
        execution_date=context['execution_date'],
        dag_id=context['dag'].dag_id,
        task_id=context['task'].task_id,
        run_id=context['run_id'],
        environment=os.getenv('ETL_ENV', 'development'),
        correlation_id=context.get('correlation_id', str(context['run_id']))
    )
    
    # Get extraction results from XCom
    extract_results = []
    task_ids = ['extract_economy', 'extract_series', 'extract_topic', 'extract_time',
                'extract_source', 'extract_region', 'extract_income', 'extract_lending']
    
    for task_id in task_ids:
        try:
            result = context['task_instance'].xcom_pull(task_ids=task_id)
            if result:
                extract_results.append(result)
        except:
            continue
    
    # Initialize validator and process
    config_manager = get_config_manager()
    validator = DataQualityValidator(config_manager)
    
    result = validator.process(etl_context, extract_results)
    
    if not result.success:
        raise ValueError(f"Data quality validation failed: {'; '.join(result.errors)}")
    
    return result.__dict__


def load_data_to_database(**context):
    """Airflow task function for database loading"""
    # Extract context information
    etl_context = ETLContext(
        execution_date=context['execution_date'],
        dag_id=context['dag'].dag_id,
        task_id=context['task'].task_id,
        run_id=context['run_id'],
        environment=os.getenv('ETL_ENV', 'development'),
        correlation_id=context.get('correlation_id', str(context['run_id']))
    )
    
    # Get transformation results from XCom
    transform_results = context['task_instance'].xcom_pull(task_ids='transform_data')
    
    if not transform_results:
        raise ValueError("No transformation results found in XCom")
    
    # Initialize loader and process
    config_manager = get_config_manager()
    loader = DatabaseLoaderService(config_manager)
    
    result = loader.process(etl_context, transform_results)
    
    if not result.success:
        raise ValueError(f"Database loading failed: {'; '.join(result.errors)}")
    
    return result.__dict__


def publish_to_kafka(**context):
    """Airflow task function for Kafka publishing"""
    # Extract context information
    etl_context = ETLContext(
        execution_date=context['execution_date'],
        dag_id=context['dag'].dag_id,
        task_id=context['task'].task_id,
        run_id=context['run_id'],
        environment=os.getenv('ETL_ENV', 'development'),
        correlation_id=context.get('correlation_id', str(context['run_id']))
    )
    
    # Get transformation results from XCom
    transform_results = context['task_instance'].xcom_pull(task_ids='transform_data')
    
    if not transform_results:
        return {
            'status': 'SKIPPED',
            'reason': 'No transformation results to publish'
        }
    
    # Initialize publisher and process
    config_manager = get_config_manager()
    publisher = KafkaPublisherService(config_manager)
    
    result = publisher.process(etl_context, transform_results)
    
    if not result.success:
        # Log errors but don't fail the task (Kafka publishing is not critical)
        logger = StructuredLogger(__name__).get_logger()
        logger.warning(f"Kafka publishing had errors: {'; '.join(result.errors)}")
    
    return result.__dict__


def store_to_hdfs(**context):
    """Airflow task function for HDFS storage"""
    # Extract context information
    etl_context = ETLContext(
        execution_date=context['execution_date'],
        dag_id=context['dag'].dag_id,
        task_id=context['task'].task_id,
        run_id=context['run_id'],
        environment=os.getenv('ETL_ENV', 'development'),
        correlation_id=context.get('correlation_id', str(context['run_id']))
    )
    
    # Get transformation results from XCom
    transform_results = context['task_instance'].xcom_pull(task_ids='transform_data')
    
    if not transform_results:
        return {
            'status': 'SKIPPED',
            'reason': 'No transformation results to store'
        }
    
    # Initialize storage service and process
    config_manager = get_config_manager()
    storage = HDFSStorageService(config_manager)
    
    result = storage.process(etl_context, transform_results)
    
    if not result.success:
        # Log errors but don't fail the task (HDFS storage is not critical for immediate processing)
        logger = StructuredLogger(__name__).get_logger()
        logger.warning(f"HDFS storage had errors: {'; '.join(result.errors)}")
    
    return result.__dict__


def cleanup_temp_data(**context):
    """Airflow task function for cleanup operations"""
    logger = StructuredLogger(__name__).get_logger()
    
    try:
        config_manager = get_config_manager()
        config = config_manager.load_config()
        
        # Database cleanup
        import pymysql
        conn = pymysql.connect(
            host=config.database.primary['host'],
            user=config.database.primary['username'],
            password=config.database.primary['password'],
            database=config.database.primary['database'],
            charset=config.database.primary.get('charset', 'utf8mb4')
        )
        
        cursor = conn.cursor()
        
        # Cleanup old audit logs and temporary data
        cleanup_queries = [
            "DELETE FROM audit_etl_runs WHERE created_at < DATE_SUB(NOW(), INTERVAL 30 DAY)",
            "DELETE FROM etl_performance_metrics WHERE created_at < DATE_SUB(NOW(), INTERVAL 7 DAY)",
            "OPTIMIZE TABLE economy_data, economy_metadata, series_data, series_metadata",
            "ANALYZE TABLE economy_data, economy_metadata, series_data, series_metadata"
        ]
        
        for query in cleanup_queries:
            try:
                cursor.execute(query)
                logger.info(f"Executed cleanup query: {query[:50]}...")
            except Exception as e:
                logger.warning(f"Cleanup query failed: {e}")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # File system cleanup (temporary files)
        import shutil
        temp_dirs = ['/tmp/worldbank_etl', '/tmp/spark-warehouse']
        cleaned_count = 0
        
        for temp_dir in temp_dirs:
            if os.path.exists(temp_dir):
                try:
                    shutil.rmtree(temp_dir)
                    cleaned_count += 1
                    logger.info(f"Cleaned temporary directory: {temp_dir}")
                except Exception as e:
                    logger.warning(f"Failed to clean {temp_dir}: {e}")
        
        return {
            'status': 'SUCCESS',
            'directories_cleaned': cleaned_count,
            'cleanup_completed_at': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Cleanup failed: {str(e)}")
        raise