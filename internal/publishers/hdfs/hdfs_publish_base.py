
import os
import logging
from pathlib import Path
from typing import Dict, Any

import pandas as pd
import pyarrow as pa
from pyarrow import parquet as pq
from hdfs import InsecureClient


class HDFSRepository:
    """Repository for HDFS operations"""
    
    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.client = None
        self._connect()
    
    def _connect(self):
        """Establish HDFS client connection"""
        try:
            namenode_url = f"http://{self.config['namenode']}:{self.config.get('webhdfs_port', 9870)}"
            self.client = InsecureClient(namenode_url)
            
            # Test connection
            self.client.list('/')
            
            self.logger.info(f"HDFS client connected to {namenode_url}")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to HDFS: {str(e)}")
            raise
    
    def write_parquet(self, hdfs_path: str, df: pd.DataFrame):
        """Write DataFrame as Parquet file to HDFS"""
        try:
            # Ensure directory exists
            directory = str(Path(hdfs_path).parent)
            self.client.makedirs(directory)
            
            # Convert to PyArrow table
            table = pa.Table.from_pandas(df)
            
            # Write to temporary local file first
            temp_file = f"/tmp/{Path(hdfs_path).name}"
            pq.write_table(table, temp_file, compression='snappy')
            
            # Upload to HDFS
            with open(temp_file, 'rb') as local_file:
                self.client.write(hdfs_path, local_file, overwrite=True)
            
            # Cleanup temp file
            os.remove(temp_file)
            
            self.logger.info(f"Successfully wrote {len(df)} records to HDFS: {hdfs_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to write Parquet to HDFS {hdfs_path}: {str(e)}")
            raise
    
    def read_parquet(self, hdfs_path: str) -> pd.DataFrame:
        """Read Parquet file from HDFS as DataFrame"""
        try:
            # Download to temporary local file
            temp_file = f"/tmp/{Path(hdfs_path).name}"
            
            with open(temp_file, 'wb') as local_file:
                self.client.read(hdfs_path, local_file)
            
            # Read Parquet file
            df = pd.read_parquet(temp_file)
            
            # Cleanup temp file
            os.remove(temp_file)
            
            self.logger.info(f"Successfully read {len(df)} records from HDFS: {hdfs_path}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read Parquet from HDFS {hdfs_path}: {str(e)}")
            raise
    
    def list_directory(self, hdfs_path: str) -> list:
        """List files in HDFS directory"""
        try:
            return self.client.list(hdfs_path)
        except Exception as e:
            self.logger.error(f"Failed to list HDFS directory {hdfs_path}: {str(e)}")
            return []
    
    def delete_path(self, hdfs_path: str, recursive: bool = False):
        """Delete path from HDFS"""
        try:
            self.client.delete(hdfs_path, recursive=recursive)
            self.logger.info(f"Deleted HDFS path: {hdfs_path}")
        except Exception as e:
            self.logger.error(f"Failed to delete HDFS path {hdfs_path}: {str(e)}")
            raise

class HDFSStorageService(BaseETLProcessor):
    """HDFS storage service for long-term data archival"""
    
    def __init__(self, config_manager: ConfigurationManager):
        super().__init__(config_manager)
        self.hdfs_client = None
    
    def _get_hdfs_client(self) -> HDFSRepository:
        """Get HDFS client instance"""
        if self.hdfs_client is None:
            hdfs_config = self.config_manager.get_hdfs_config()
            self.hdfs_client = HDFSRepository(hdfs_config, self.logger)
        return self.hdfs_client
    
    def _generate_hdfs_path(self, table_name: str, execution_date: datetime, layer: str = "processed") -> str:
        """Generate HDFS path with partitioning strategy"""
        year = execution_date.year
        month = execution_date.month
        day = execution_date.day
        
        # Path structure: /data/worldbank/{layer}/{domain}/{table}/year={year}/month={month}/day={day}/
        domain = table_name.split('_')[0]  # Extract domain from table name
        
        return f"/data/worldbank/{layer}/{domain}/{table_name}/year={year}/month={month:02d}/day={day:02d}/"
    
    def _convert_to_parquet_compatible(self, data: Any) -> pd.DataFrame:
        """Convert data to Parquet-compatible DataFrame"""
        if isinstance(data, pd.DataFrame):
            return data
        elif isinstance(data, list):
            return pd.DataFrame(data)
        elif isinstance(data, dict):
            return pd.DataFrame([data])
        else:
            self.logger.warning(f"Unsupported data type for Parquet conversion: {type(data)}")
            return pd.DataFrame()
    
    def process(self, context: ETLContext, transform_results: Dict[str, Any]) -> ProcessingResult:
        """Main processing method for HDFS storage"""
        self._validate_context(context)
        start_time = datetime.now()
        stored_paths = []
        errors = []
        total_records = 0
        
        try:
            if not transform_results:
                return ProcessingResult(
                    success=True,
                    records_processed=0,
                    execution_time_seconds=0,
                    warnings=["No transformation results to store"]
                )
            
            # Get HDFS client
            hdfs_client = self._get_hdfs_client()
            
            # Store each dataset
            for table_name, data in transform_results.items():
                if data is not None:
                    try:
                        # Generate HDFS path
                        hdfs_path = self._generate_hdfs_path(table_name, context.execution_date)
                        
                        # Convert data to DataFrame for Parquet storage
                        df = self._convert_to_parquet_compatible(data)
                        
                        if not df.empty:
                            # Add metadata columns
                            df['_execution_date'] = context.execution_date
                            df['_dag_id'] = context.dag_id
                            df['_task_id'] = context.task_id
                            df['_run_id'] = context.run_id
                            df['_loaded_at'] = datetime.now()
                            
                            # Store as Parquet file
                            file_path = f"{hdfs_path}data.parquet"
                            hdfs_client.write_parquet(file_path, df)
                            
                            stored_paths.append(file_path)
                            total_records += len(df)
                            
                            self.logger.info(f"Stored {len(df)} records for {table_name} to HDFS: {file_path}")
                        
                    except Exception as e:
                        error_msg = f"Failed to store {table_name} to HDFS: {str(e)}"
                        errors.append(error_msg)
                        self.logger.error(error_msg)
            
            # Calculate execution time
            duration = (datetime.now() - start_time).total_seconds()
            self._log_performance("hdfs_storage", duration, total_records)
            
            success = len(errors) == 0
            
            return ProcessingResult(
                success=success,
                records_processed=total_records,
                execution_time_seconds=duration,
                errors=errors,
                metadata={
                    'paths_stored': stored_paths,
                    'storage_format': 'parquet'
                }
            )
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            error_msg = f"HDFS storage failed: {str(e)}"
            self.logger.error(error_msg)
            
            return ProcessingResult(
                success=False,
                records_processed=total_records,
                execution_time_seconds=duration,
                errors=[error_msg]
            )