
from datetime import datetime
from internal.dags.cophieu68_dag.load.mongodb.load_datalake_cophieu68 import *
from internal.dags.ETL_Orchestra.main_orchestra_etl import ETLPipelineConfig
from utils import TableCreator
from internal.models.cophieu68_model.transform_models import DATA_WAREHOUSE_SCHEMA as schema_dw
from internal.dags.cophieu68_dag.transform.postgres_sql_dw.cophieu68_metadata import *
from internal.models.cophieu68_model.extract_models import CRAWL_INDUSTRY_LIST_CONFIG
# config_path = "/mnt/c/Users/Admin/Downloads/Project/Github/ETL_Project/internal/config/web_craw_config/cophieu68_config.yaml"
# config = ETLPipelineConfig(config_path=config_path)
# mongo_config =config.config.get("storage", {}).get("mongodb", {})

class TransformDatawarehouse:
    def __init__(self,datawarehouse_config, datalake_config, datawarehouse_logger):
        self.datawarehouse_config = datawarehouse_config
        self.datawarehouse_logger = datawarehouse_logger
        self.datalake_config = datalake_config
        self.schema_dw = schema_dw
        self.loading_datalake = MongoLoader(
                username = datalake_config.get("username", ""),
                password = datalake_config.get("password", ""),
                host = datalake_config.get("host", "localhost"),
                authSource = datalake_config.get("authSource", "admin"),
                port = datalake_config.get("port", 27017),
                database = datalake_config.get("database", "ETL_Project"),
            )
        self.backend_mongo =MongoStorageBackend( self.loading_datalake)


    def __get_symbol_data(self):
        list_stock_collection = self.datalake_config.get("collections", {}).get("list_stock", "list_stock")
        symbol_data = self.backend_mongo.find_table(name = list_stock_collection)
        return symbol_data
    
    def __get_industry_info(self):
        list_industry_collection = self.datalake_config.get("collections", {}).get("industry_list", "industry_list")
        industry_data = self.backend_mongo.find_table(name = list_industry_collection)
        return industry_data
    
    def __get_schema_info(self, info, key_adjust, table_name):
        try:
            dim_market_type = self.schema_dw.get("dimensions", {}).get(info, {})
            rules_dict = dim_market_type.get("rules")
            table_creator = TableCreator(machine_id=1, character_specific=key_adjust)
            create_table_sql = table_creator.generate_create_table_sql(table_name, rules_dict)
            return table_creator, create_table_sql
        except Exception as e:
            self.datawarehouse_logger.error(f"Error: {e}")
        

    def dim_market_type(self):
        """
        Dim Market Type
            market_key BIGSERIAL PRIMARY KEY,
            market_type TEXT UNIQUE NOT NULL,       -- HOSE / HNX / UPCOM
            market_name TEXT,                       -- optional
            update_time TIMESTAMPTZ,                -- from Mongo
        """
        try:
            table_creator, create_table_sql = self.__get_schema_info(info = "dim_market_type", 
                                                    key_adjust = dim_market_type_info["market_key_char"], 
                                                    table_name = dim_market_type_info["table_name"])
            dim_market_type = self.__get_symbol_data()
            market_type = [item["market_type"].upper() for item in dim_market_type]
            market_name = [dim_market_type_info["market_name"].get(item, "") for item in market_type]
            update_time = datetime.now().isoformat()
            created_time = update_time
            _df = pd.DataFrame({ "market_type": market_type,
                                "market_name": market_name,
                                "update_time": [update_time for _ in range(len(market_type))],
                                "created_time": [created_time for _ in range(len(market_type))]})
            df_with_id = table_creator.add_id_column(df = _df, id_name=dim_market_type_info["primary_key"])
            return df_with_id, create_table_sql
        except Exception as e:
            self.datawarehouse_logger.error(f"Error: {e}")
    
    def dim_industry(self):
        try:
            table_creator, create_table_sql = self.__get_schema_info(info = "dim_industry", 
                                                    key_adjust = dim_industry_info["market_key_char"], 
                                                    table_name = dim_industry_info["table_name"])
            
            created_time = datetime.now().isoformat()
            update_time = created_time
            _df = pd.DataFrame({ "industry_code": [item for item in dim_industry_info.get("industry_mapping").values()],
                                "industry_name": [item for item in dim_industry_info.get("industry_mapping").keys()],
                                
                                "update_time": update_time,
                                "created_time": created_time}
            )
            df_with_id = table_creator.add_id_column(df = _df, id_name=dim_industry_info["primary_key"])
            return df_with_id, create_table_sql
        except Exception as e:
            self.datawarehouse_logger.error(f"Error: {e}")

    def 
    
