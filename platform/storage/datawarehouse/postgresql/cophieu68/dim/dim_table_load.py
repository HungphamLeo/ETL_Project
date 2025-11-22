
from datetime import datetime
from internal.dags.cophieu68_dag.load.mongodb.load_datalake_cophieu68 import *
from internal.dags.cophieu68_dag.transform.base_transform import TransformDatawarehouse
from utils import TableCreator
from internal.dags.cophieu68_dag.transform.postgres_sql_dw.cophieu68_metadata import *
# config_path = "/mnt/c/Users/Admin/Downloads/Project/Github/ETL_Project/internal/config/web_craw_config/cophieu68_config.yaml"
# config = ETLPipelineConfig(config_path=config_path)
# mongo_config =config.config.get("storage", {}).get("mongodb", {})


class DimMarketTypeLoader(TransformDatawarehouse):

    def __init__(self, datalake_config, logger, postgres_client):
        super().__init__(datalake_config, logger, postgres_client)
        self.schema = self.schema_dw["dimensions"]["dim_market_type"]
        self.table_creator = TableCreator(machine_id=1, character_specific = dim_market_type_info["character_specific"]) 

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
        self.table_creator = TableCreator(machine_id=1, character_specific = dim_industry_mapping_info["character_specific"]) 

    def load(self):
        rows = []
        
        for name, code in dim_industry_mapping_info["industry_mapping"].items():
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

        self.schema_company = self.schema_dw["dimensions"]["dim_company"]
        self.schema_profile = self.schema_dw["dimensions"]["dim_company_profile"]

        # Company dùng character_specific riêng
        self.table_creator_company = TableCreator(
            machine_id=1,
            character_specific=dim_company_profile_info["character_specific"]
        )


    # -------------------------
    # LOAD FUNCTION
    # -------------------------
    def load(self):
        raw_docs = self.mongo.find_table("list_stock")

        rows_company = []
        rows_profile = []

        for doc in raw_docs:
            symbol = doc["symbol"]
            profile_raw = doc.get("profile", {})
            update_time = doc.get("update_time", datetime.utcnow())

            # 1) Generate surrogate key for COMPANY
            company_key = self.repo.get_or_create(symbol, "dim_company_profile", self.table_creator_company)

            # 2) Record into DIM_COMPANY (SCD2)
            rows_company.append({
                "company_key": company_key,
                "symbol": symbol,
                "company_name": profile_raw.get("full_name", ""),
                "market_key": self.repo.get_or_create(doc["market_type"], "dim_market_type", self.table_creator_company),
                "industry_key": self.repo.get_or_create(doc.get("industry", ""), "dim_industry", self.table_creator_company),
                "profile_json": profile_raw,
                "effective_from": datetime.utcnow(),
                "effective_to": None,
                "is_current": True
            })

            # 3) DIM_COMPANY_PROFILE (SCD1)
            rows_profile.append({
                "profile_key": self.table_creator_company.get_id(),
                "company_key": company_key,
                "full_name": profile_raw.get("full_name", ""),
                "english_name": profile_raw.get("english_name", ""),
                "short_name": profile_raw.get("short_name", ""),
                "address": profile_raw.get("address", ""),
                "phone": profile_raw.get("phone", ""),
                "fax": profile_raw.get("fax", ""),
                "website": profile_raw.get("website", ""),
                "email": profile_raw.get("email", ""),
                "established_date": profile_raw.get("established_date", ""),
                "listed_date": profile_raw.get("listed_date", ""),
                "chartered_capital": profile_raw.get("chartered_capital", ""),
                "business_license": profile_raw.get("business_license", ""),
                "tax_code": profile_raw.get("tax_code", ""),
                "update_time": update_time,
                "created_time": datetime.utcnow()
            })

        df_company = pd.DataFrame(rows_company)
        df_profile = pd.DataFrame(rows_profile)

        sql_company = self.table_creator_company.generate_create_table_sql(
            "dim_company",
            self.schema_company["columns"]
        )

        sql_profile = self.table_creator_company.get_id(
            "dim_company_profile",
            self.schema_profile["columns"]
        )

        return {
            "dim_company": (df_company, sql_company),
            "dim_company_profile": (df_profile, sql_profile)
        }




