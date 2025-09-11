"""
Database Loader Service - ETL Pipeline Load Layer
Handles database operations, data loading, and table management
"""

import pymysql
from typing import Dict, Any, Optional, List
from src.utils import TableCreator
from internal.models.wbgapi_model.transform_models import (
    EconomyTransform, SeriesTransform, TopicTransform, TimeTransform,
    SourceTransform, RegionTransform, IncomeTransform, LendingTransform
)


class BaseDBLoader:
    """Base database loader with core CRUD operations"""
    
    def __init__(self, cursor, connection, pipeline_logger, pipeline_config):
        self.cursor = cursor
        self.connection = connection
        self.logger = pipeline_logger
        self.config = pipeline_config
        

    def execute(self, query: str, params: Optional[Dict] = None):
        """Execute SQL command"""
        try:
            self.cursor.execute(query, params or {})
            self.logger.info(f"Executed query: {query[:100]}...")
        except Exception as e:
            self.logger.error(f"Query failed: {e}")
            raise

    def executemany(self, query: str, param_list: List[Dict]):
        """Execute bulk operations"""
        try:
            self.cursor.executemany(query, param_list)
            self.logger.info(f"Executed bulk operation with {len(param_list)} records")
        except Exception as e:
            self.logger.error(f"Bulk operation failed: {e}")
            raise

    def commit(self):
        """Commit transaction"""
        try:
            self.connection.commit()
            self.logger.info("Transaction committed")
        except Exception as e:
            self.logger.error(f"Commit failed: {e}")
            raise

    def rollback(self):
        """Rollback transaction"""
        try:
            self.connection.rollback()
            self.logger.info("Transaction rolled back")
        except Exception as e:
            self.logger.error(f"Rollback failed: {e}")
            raise

    def create_database(self, db_name: str):
        """Create database if not exists"""
        self.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        self.logger.info(f"Database {db_name} created/verified")

    def create_table(self, create_table_sql: str):
        """Create table from SQL"""
        self.execute(create_table_sql)

    def insert_batch(self, table_name: str, data_dict: Dict[str, List]):
        """Insert batch data efficiently"""
        if not data_dict:
            return
            
        columns = list(data_dict.keys())
        placeholders = ','.join(['%s'] * len(columns))
        
        # Transpose data for bulk insert
        rows = list(zip(*data_dict.values()))
        
        insert_sql = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})"
        self.executemany(insert_sql, rows)
        self.commit()

    def truncate_table(self, table_name: str):
        """Truncate table"""
        self.execute(f"TRUNCATE TABLE {table_name}")
        self.commit()
        self.logger.info(f"Table {table_name} truncated")


class DatabaseLoaderService:
    """Main service for loading transformed data into database"""
    
    # Table mapping configuration
    TABLE_TRANSFORM_CONFIG = [
        (EconomyTransform, "ECONOMY", EconomyTransform.ECONOMY_DF_RULES),
        (SeriesTransform, "SERIES", SeriesTransform.SERIES_DF_RULES),
        (TopicTransform, "TOPIC", TopicTransform.TOPIC_DF_RULES),
        (TimeTransform, "TIME", TimeTransform.TIME_DF_RULES),
        (SourceTransform, "SOURCE", SourceTransform.SOURCE_DF_RULES),
        (RegionTransform, "REGION", RegionTransform.REGION_DF_RULES),
        (IncomeTransform, "INCOME", IncomeTransform.INCOME_DF_RULES),
        (LendingTransform, "LENDING", LendingTransform.LENDING_DF_RULES),
    ]
    
    def __init__(self, pipeline_config):
        self.config = pipeline_config
        self.conn = None
        self.cursor = None
        self.loader = None
        self.logger = pipeline_config.logger

    def __enter__(self):
        """Context manager entry - establish connection"""
        self._connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup connections"""
        self._disconnect()

    def _connect(self):
        """Establish database connection"""
        try:
            db_config = self.config.config['database']['primary']
            self.conn = pymysql.connect(
                host=db_config['host'],
                user=db_config['username'],
                password=db_config['password'],
                database=db_config['database'],
                charset=db_config.get('charset', 'utf8mb4'),
                cursorclass=pymysql.cursors.DictCursor
            )
            self.cursor = self.conn.cursor()
            self.loader = BaseDBLoader(self.cursor, self.conn, )
            self.logger.info("Database connection established")
        except Exception as e:
            self.logger.error(f"Database connection failed: {e}")
            raise

    def _disconnect(self):
        """Close database connections"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
            self.logger.info("Database connections closed")
        except Exception as e:
            self.logger.error(f"Error closing connections: {e}")

    def create_database(self):
        """Create database if not exists"""
        db_name = self.config.config['database']['primary']['database']
        self.loader.create_database(db_name)

    def _get_table_dataframe_mapping(self, transformed_data: Dict) -> Dict[str, Any]:
        """Map table names to their corresponding dataframes"""
        return {
            "economy_data": transformed_data.get("economy_data"),
            "economy_metadata": transformed_data.get("economy_metadata"),
            "series_data": transformed_data.get("series_data"),
            "series_metadata": transformed_data.get("series_metadata"),
            "topic_info": transformed_data.get("topic_info"),
            "topic_series": transformed_data.get("topic_series"),
            "topic_metadata": transformed_data.get("topic_metadata"),
            "time_series": transformed_data.get("time_series"),
            "source_info": transformed_data.get("source_info"),
            "source_series": transformed_data.get("source_series"),
            "region_info": transformed_data.get("region_info"),
            "income_series": transformed_data.get("income_series"),
            "lending_series": transformed_data.get("lending_series"),
        }

    def _create_table_from_rule(self, prefix: str, rule_name: str, rule_dict: Dict):
        """Create table based on transformation rules"""
        table_name = f"{prefix.lower()}_{rule_name.replace('transform_', '').replace('_dataframe', '').replace('_metadata', '')}"
        character_specific = ''.join([w.capitalize() for w in table_name.split('_')])
        
        creator = TableCreator(machine_id=1, character_specific=character_specific)
        create_sql = creator.generate_create_table_sql(table_name, rule_dict)
        
        self.loader.create_table(create_sql)
        self.logger.info(f"Table {table_name} created/verified")
        
        return table_name, creator

    def _load_dataframe_to_table(self, table_name: str, df, creator: TableCreator):
        """Load dataframe data into database table"""
        if df is None or df.empty:
            self.logger.warning(f"No data for table {table_name}, skipping")
            return

        # Add ID column and prepare data
        df_with_id = creator.add_id_column(df)
        
        # Convert dataframe to dict for batch insert
        data_dict = {col: df_with_id[col].tolist() for col in df_with_id.columns}
        
        # Truncate and load
        self.loader.truncate_table(table_name)
        self.loader.insert_batch(table_name, data_dict)
        
        self.logger.info(f"Loaded {len(df_with_id)} records into {table_name}")

    def load_transformed_data(self, transformed_data: Dict[str, Any]):
        """Main method to load all transformed data"""
        try:
            table_df_mapping = self._get_table_dataframe_mapping(transformed_data)
            
            for transform_cls, prefix, rules in self.TABLE_TRANSFORM_CONFIG:
                self.logger.info(f"Processing {prefix} tables")
                
                for rule_name, rule_dict in rules.items():
                    if not rule_name.startswith("transform_"):
                        continue
                    
                    # Create table
                    table_name, creator = self._create_table_from_rule(prefix, rule_name, rule_dict)
                    
                    # Load data
                    df = table_df_mapping.get(table_name)
                    self._load_dataframe_to_table(table_name, df, creator)
            
            self.logger.info("All data loaded successfully")
            
        except Exception as e:
            self.logger.error(f"Data loading failed: {e}")
            if self.loader:
                self.loader.rollback()
            raise


# Usage Example:
# with DatabaseLoaderService(pipeline_config) as loader:
#     loader.create_database()
#     loader.load_transformed_data(transformed_data)