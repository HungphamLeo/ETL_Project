# run_dw_load.py

# from prefect import task, flow
from typing import List
from platforms.processing.prefect.flows.prefect_orchestra_etl import PrefectETLPipelineConfig
from shared.logger.python_main_logger import FastLogger
from platforms.processing.base_processing import FileConfigLoader, DefaultLoggerFactory
from platforms.ingestion.cophieu68.load.load_datawarehouse_cophieu68 import (
    DimMarketTypeLoader, DimIndustryLoader, DimCompanyLoader, DimReportTypeLoader,
    FactTradeLoader, FactMatchDetailLoader, FactIncomeStatementLoader, 
    FactBalanceSheetLoader, FactBusinessPlanLoader, FactFinancialMetricsLoader, FactIndustryLoader
)
from platforms.ingestion.cophieu68.dto.extract_models import (
    CRAWL_MARKET_LIST_CONFIG,
    INDUSTRIAL_INFO_TYPE
)
from platforms.storage.datawarehouse.postgresql.datawarehouse_storage import PostgreSQLWriter, PostgreSQLStorageBackend
from platforms.storage.datalake.mongodb.data_lake_storage import MongoStorageBackend
from platforms.ingestion.cophieu68.load.load_datalake_cophieu68 import MongoLoader
from shared.utils.files.util_cophieu68 import TableCreator

config_path = "./platforms/processing/prefect/config/cophieu68_config.yaml"
config = PrefectETLPipelineConfig(config_path=config_path)
postgresql_config_etl_arg = config.config.get("storage", {}).get("postgreSQL", {}).get("reties_etl_flows", {})



def build_backend_mongo(config:PrefectETLPipelineConfig, logger):
    """
    Builds a backend for ETL pipeline based on given configuration.

    Parameters
    ----------
    config : dict
        Configuration for the ETL pipeline.

    Returns
    -------
    mongo_config : dict
        Configuration for MongoDB.
    loading_datalake : MongoLoader
        Object for loading data into MongoDB.
    backend_mongo : MongoStorageBackend
        Object for storing data in MongoDB.
    """
    try:
        mongo_config = config.get_mongo_config()
        mongo_storage_logger = logger
        loading_datalake = MongoLoader(
            username = mongo_config.get("username", ""),
            password = mongo_config.get("password", ""),
            host = mongo_config.get("host", "localhost"),
            authSource = mongo_config.get("authSource", "admin"),
            port = mongo_config.get("port", 27017),
            database = mongo_config.get("database", "ETL_Project"),
            logger = mongo_storage_logger
        )
        backend_mongo =MongoStorageBackend(mongo_writter = loading_datalake, pipeline_logger=mongo_storage_logger)

        return mongo_config,loading_datalake, backend_mongo
    except Exception as e:
        logger.error(f"Error building backend: {e}")
        raise


def build_backend_postgre(config:PrefectETLPipelineConfig, logger):
    """
    Builds a backend for ETL pipeline based on given configuration.

    Parameters
    ----------
    config : dict
        Configuration for the ETL pipeline.

    Returns
    -------
    mongo_config : dict
        Configuration for MongoDB.
    loading_datalake : MongoLoader
        Object for loading data into MongoDB.
    backend_mongo : MongoStorageBackend
        Object for storing data in MongoDB.
    """
    try:
        postgre_config = config.get_postgres_config()
        postgre_storage_logger = logger
        loading_datawarehouse = PostgreSQLWriter(
            username = postgre_config.get("username", ""),
            password = postgre_config.get("password", ""),
            host = postgre_config.get("host", "localhost"),
            port = postgre_config.get("port", 5432),
            database = postgre_config.get("database", "ETL_Project"),
            logger = postgre_storage_logger
        )
        backend_postgres = PostgreSQLStorageBackend(postgres_writter=loading_datawarehouse, pipeline_logger=postgre_storage_logger)

        return postgre_config, loading_datawarehouse, backend_postgres
    except Exception as e:
        logger.error(f"Error building backend: {e}")
        raise

# @task
def load_dim_market_type(datalake_config, table_creator, mongo_reader, logger, pg_client):
    loader = DimMarketTypeLoader(datalake_config, table_creator, mongo_reader, logger, pg_client)
    df, sql = loader.load()
    pg_client.execute(sql)
    pg_client.bulk_insert(f"{loader.schema_name}.{loader.dim_name.upper()}", df)
    logger.info(f"[{loader.dim_name}] Loaded {len(df)} rows")
    return True


# @task
def load_dim_industry(datalake_config, table_creator, mongo_reader, logger, pg_client):
    loader = DimIndustryLoader(datalake_config, table_creator, mongo_reader, logger, pg_client)
    df, sql = loader.load()
    pg_client.execute(sql)
    pg_client.bulk_insert(f"{loader.schema_name}.{loader.dim_name.upper()}", df)
    logger.info(f"[{loader.dim_name}] Loaded {len(df)} rows")
    return True


# @task
def load_dim_company(datalake_config, table_creator, mongo_reader, logger, pg_client):
    loader = DimCompanyLoader(datalake_config, table_creator, mongo_reader, logger, pg_client)
    df, sql = loader.load()
    pg_client.execute(sql)
    pg_client.bulk_insert(f"{loader.schema_name}.{loader.dim_name.upper()}", df)
    logger.info(f"[{loader.dim_name}] Loaded {len(df)} rows")
    return True

# @task
def load_dim_report_type(datalake_config, table_creator, mongo_reader, logger, pg_client):
    loader = DimReportTypeLoader(datalake_config, table_creator, mongo_reader, logger, pg_client)
    df, sql = loader.load()
    pg_client.execute(sql)
    pg_client.bulk_insert(df, loader.dim_name)
    logger.info(f"[{loader.dim_name}] Loaded {len(df)} rows")
    return True

# =========================
# FACT LOADING
# =========================
# @task
def load_fact_trade(datalake_config, table_creator, mongo_reader, logger, pg_client, dim_repo):
    loader = FactTradeLoader(datalake_config, table_creator, mongo_reader, dim_repo, logger, pg_client)
    df, sql = loader.load()
    pg_client.execute(sql)
    pg_client.bulk_insert(df, loader.fact_name)
    logger.info(f"[{loader.fact_name}] Loaded {len(df)} rows")
    return True


# @task
def load_fact_match(datalake_config, table_creator, mongo_reader, logger, pg_client, dim_repo):
    loader = FactMatchDetailLoader(datalake_config, table_creator, mongo_reader, dim_repo, logger, pg_client)
    df, sql = loader.load()
    pg_client.execute(sql)
    pg_client.bulk_insert(df, loader.fact_name)
    logger.info(f"[{loader.fact_name}] Loaded {len(df)} rows")
    return True


# @task
def load_fact_income(datalake_config, table_creator, mongo_reader, logger, pg_client, dim_repo):
    loader = FactIncomeStatementLoader(datalake_config, table_creator, mongo_reader, dim_repo, logger, pg_client)
    df_quarterly, sql_quarterly = loader.load_fact_income_statement_quarterly()
    pg_client.execute(sql_quarterly)
    pg_client.bulk_insert(f"{loader.schema_name}.{loader.fact_income_statement_quarterly.upper()}", df_quarterly)

    df_annual, sql_annual = loader.load_fact_income_statement_annually()
    pg_client.execute(sql_annual)
    pg_client.bulk_insert(f"{loader.schema_name}.{loader.fact_income_statement_annually.upper()}", df_annual)
    
    logger.info(f"[{loader.fact_income_statement_annually}] Loaded {len(df_annual)} rows")
    logger.info(f"[{loader.fact_income_statement_quarterly}] Loaded {len(df_quarterly)} rows")
    return True


# @task
def load_fact_balance(datalake_config, table_creator, mongo_reader, logger, pg_client, dim_repo):
    loader = FactBalanceSheetLoader(datalake_config, table_creator, mongo_reader, dim_repo, logger, pg_client)
    df_quarterly, sql_quarterly = loader.load_fact_balance_sheet_quarterly()
    pg_client.execute(sql_quarterly)
    pg_client.bulk_insert(f"{loader.schema_name}.{loader.fact_balance_sheet_quarterly.upper()}", df_quarterly)

    df_annual, sql_annual = loader.load_fact_balance_sheet_annually()
    pg_client.execute(sql_annual)
    pg_client.bulk_insert(f"{loader.schema_name}.{loader.fact_balance_sheet_annually.upper()}", df_annual)

    logger.info(f"[{loader.fact_balance_sheet_annually}] Loaded {len(df_annual)} rows")
    logger.info(f"[{loader.fact_balance_sheet_quarterly}] Loaded {len(df_quarterly)} rows")
    return True


# @task
def load_fact_business_plan(datalake_config, table_creator, mongo_reader, logger, pg_client, dim_repo):
    loader = FactBusinessPlanLoader(datalake_config, table_creator, mongo_reader, dim_repo, logger, pg_client)
    df, sql = loader.load()
    pg_client.execute(sql)
    pg_client.bulk_insert(f"{loader.schema_name}.{loader.fact_name.upper()}", df)
    logger.info(f"[{loader.fact_name}] Loaded {len(df)} rows")
    return True


# @task
def load_fact_financial_metrics(datalake_config, table_creator, mongo_reader, logger, pg_client, dim_repo):
    loader = FactFinancialMetricsLoader(datalake_config, table_creator, mongo_reader, dim_repo, logger, pg_client)
    df, sql = loader.load()
    pg_client.execute(sql)
    pg_client.bulk_insert(f"{loader.schema_name}.{loader.fact_name.upper()}", df)
    logger.info(f"[{loader.fact_name}] Loaded {len(df)} rows")
    return True


def load_fact_industry(datalake_config, table_creator, mongo_reader, logger, pg_client, dim_repo):
    loader = FactIndustryLoader(datalake_config, table_creator, mongo_reader, dim_repo, logger, pg_client)
    df, sql = loader.load()
    pg_client.execute(sql)
    pg_client.bulk_insert(f"{loader.schema_name}.{loader.fact_name.upper()}", df)
    logger.info(f"[{loader.fact_name}] Loaded {len(df)} rows")
    return True
#

# =========================
# MAIN PREFECT FLOW
# =========================
# @flow(name="DW-Full-Load")
def dw_full_load():
    pipeline_config = PrefectETLPipelineConfig(config_path=config_path, config_loader=FileConfigLoader(), logger_factory=DefaultLoggerFactory())
    mongodb_logger = pipeline_config.storage_mongodb
    postgre_logger = pipeline_config.storage_postgresql

    postgre_config, loading_datawarehouse, backend_postgres = build_backend_postgre(pipeline_config, postgre_logger)
    mongo_config, loading_datalake, backend_mongo = build_backend_mongo(pipeline_config, mongodb_logger)

    table_creator = TableCreator(machine_id=1, character_specific=None)

    load_dim_market_type(datalake_config=mongo_config, table_creator=table_creator, mongo_reader=backend_mongo, logger=postgre_logger, pg_client=backend_postgres)
    # load_dim_industry(datalake_config=mongo_config, table_creator=table_creator, mongo_reader=backend_mongo, logger=postgre_logger, pg_client=backend_postgres)
    # load_dim_company(datalake_config=mongo_config, table_creator=table_creator, mongo_reader=backend_mongo, logger=postgre_logger, pg_client=backend_postgres)
    # load_dim_report_type(datalake_config=mongo_config, table_creator=table_creator, mongo_reader=backend_mongo, logger=postgre_logger, pg_client=backend_postgres)
    

    # load_fact_trade(datalake_config=mongo_config, table_creator=table_creator, mongo_reader=backend_mongo, logger=postgre_logger, pg_client=backend_postgres)
    # load_fact_match(datalake_config=mongo_config, table_creator=table_creator, mongo_reader=backend_mongo, logger=postgre_logger, pg_client=backend_postgres)
    # load_fact_income(datalake_config=mongo_config, table_creator=table_creator, mongo_reader=backend_mongo, logger=postgre_logger, pg_client=backend_postgres)
    # load_fact_balance(datalake_config=mongo_config, table_creator=table_creator, mongo_reader=backend_mongo, logger=postgre_logger, pg_client=backend_postgres)
    # load_fact_business_plan(datalake_config=mongo_config, table_creator=table_creator, mongo_reader=backend_mongo, logger=postgre_logger, pg_client=backend_postgres)
    # load_fact_financial_metrics(datalake_config=mongo_config, table_creator=table_creator, mongo_reader=backend_mongo, logger=postgre_logger, pg_client=backend_postgres)
    # load_fact_industry(datalake_config=mongo_config, table_creator=table_creator, mongo_reader=backend_mongo, logger=postgre_logger, pg_client=backend_postgres)




if __name__ == "__main__":
    dw_full_load()
    # staging_load_postgre()
