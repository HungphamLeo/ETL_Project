
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
    """
    Base class – all shared components (Mongo, surrogate repo, table creator).
    """

    def __init__(self, datalake_config, datawarehouse_logger, postgres_client):
        self.datawarehouse_logger = datawarehouse_logger
        self.schema_dw = schema_dw

        # Mongo backend
        self.mongo = MongoStorageBackend(
            MongoLoader(
                username=datalake_config.get("username", ""),
                password=datalake_config.get("password", ""),
                host=datalake_config.get("host", "localhost"),
                authSource=datalake_config.get("authSource", "admin"),
                port=datalake_config.get("port", 27017),
                database=datalake_config.get("database", "ETL_Project"),
            )
        )

        # Shared ID generator
        self.table_creator = TableCreator(machine_id=1)

        # Shared surrogate key repository (maps natural → surrogate)
        self.repo = MetaSurrogateRepository(postgres_client)


class DimMarketTypeLoader(TransformDatawarehouse):

    def __init__(self, datalake_config, logger, postgres_client):
        super().__init__(datalake_config, logger, postgres_client)
        self.schema = self.schema_dw["dimensions"]["dim_market_type"]

    def load(self):
        raw_docs = self.mongo.find_table("market_list")

        market_types = [doc["market_type"].upper() for doc in raw_docs]

        rows = []
        for m in sorted(set(market_types)):
            surrogate_key = self.repo.get_or_create(m, "dim_market_type", self.table_creator)

            rows.append({
                "market_key": surrogate_key,
                "market_type": m,
                "market_name": dim_market_type_info["market_name"].get(m, ""),
                "update_time": datetime.utcnow(),
                "created_time": datetime.utcnow()
            })

        df = pd.DataFrame(rows)
        sql = self.table_creator.generate_create_table_sql(
            "dim_market_type", 
            self.schema["rules"]
        )

        return df, sql


class DimIndustryLoader(TransformDatawarehouse):

    def __init__(self, datalake_config, logger, postgres_client):
        super().__init__(datalake_config, logger, postgres_client)
        self.schema = self.schema_dw["dimensions"]["dim_industry"]

    def load(self):
        rows = []
        for name, code in dim_industry_info["industry_mapping"].items():
            surrogate_key = self.repo.get_or_create(code, "dim_industry", self.table_creator)

            rows.append({
                "industry_key": surrogate_key,
                "industry_code": code,
                "industry_name": name,
                "update_time": datetime.utcnow(),
                "created_time": datetime.utcnow()
            })

        df = pd.DataFrame(rows)
        sql = self.table_creator.generate_create_table_sql(
            "dim_industry",
            self.schema["rules"]
        )
        return df, sql


class DimCompanyLoader(TransformDatawarehouse):

    def __init__(self, datalake_config, logger, postgres_client):
        super().__init__(datalake_config, logger, postgres_client)
        self.schema = self.schema_dw["dimensions"]["dim_company"]

    def load(self):
        raw_docs = self.mongo.find_table("list_stock")

        rows = []
        for doc in raw_docs:
            symbol = doc["symbol"]

            company_key = self.repo.get_or_create(symbol, "dim_company", self.table_creator)

            rows.append({
                "company_key": company_key,
                "symbol": symbol,
                "company_name": doc.get("profile", {}).get("full_name", ""),
                "market_key": self.repo.get_or_create(doc["market_type"], "dim_market_type", self.table_creator),
                "industry_key": self.repo.get_or_create(doc["industry"], "dim_industry", self.table_creator),
                "profile_json": doc.get("profile", {}),
                "effective_from": datetime.utcnow(),
                "effective_to": None,
                "is_current": True
            })

        df = pd.DataFrame(rows)

        sql = self.table_creator.generate_create_table_sql(
            "dim_company",
            self.schema["columns"]    # note: your schema differs: columns vs rules
        )

        return df, sql



