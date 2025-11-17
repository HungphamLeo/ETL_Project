

from internal.dags.cophieu68_dag.load.load_datalake_cophieu68 import *
from internal.dags.ETL_Orchestra.main_orchestra_etl import ETLPipelineConfig



config_path = "/mnt/c/Users/Admin/Downloads/Project/Github/ETL_Project/internal/config/web_craw_config/cophieu68_config.yaml"
config = ETLPipelineConfig(config_path=config_path)
mongo_config =config.config.get("storage", {}).get("mongodb", {})





class TransformDatawarehouse:
    def __init__(self,datawarehouse_config, datalake_config, datawarehouse_logger):
        self.datawarehouse_config = datawarehouse_config
        self.datawarehouse_logger = datawarehouse_logger
        self.datalake_config = datalake_config
        self.loading_datalake = MongoLoader(
                username = datalake_config.get("username", ""),
                password = datalake_config.get("password", ""),
                host = datalake_config.get("host", "localhost"),
                authSource = datalake_config.get("authSource", "admin"),
                port = datalake_config.get("port", 27017),
                database = datalake_config.get("database", "ETL_Project"),
            )
        self.backend_mongo =MongoStorageBackend( self.loading_datalake)


    def get_symbol_data(self):
        list_stock_collection = mongo_config.get("collections", {}).get("list_stock", "list_stock")
        symbol_data = self.backend_mongo.find_table(name = list_stock_collection)
        return symbol_data
    

    def dim_market_type(self):
        """
        Dim Market Type
            market_key BIGSERIAL PRIMARY KEY,
            market_type TEXT UNIQUE NOT NULL,       -- HOSE / HNX / UPCOM
            market_name TEXT,                       -- optional
            update_time TIMESTAMPTZ,                -- from Mongo
            symbols_json JSONB,                     -- raw array of symbols from MongoDB
            effective_from DATE DEFAULT CURRENT_DATE,
            effective_to DATE,
            is_current BOOLEAN DEFAULT TRUE
        """
        try:
            symbol_data = self.get_symbol_data()
            market_type = [item["market_type"] for item in symbol_data]
            market_key = [item["market_key"] for item in symbol_data["data"]]
            market_name = [item["market_name"] for item in symbol_data["data"]]
            symbols_json = [item["symbols"] for item in symbol_data["data"]]
            update_time = [item["update_time"] for item in symbol_data["data"]]
            dim_market_type = pd.DataFrame({"market_key": market_key, "market_type": market_type, "market_name": market_name, "symbols_json": symbols_json, "update_time": update_time})

            