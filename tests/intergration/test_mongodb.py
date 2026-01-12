import time
from typing import List
import sys

# from prefect import flow, task
from platforms.ingestion.cophieu68.extract.extract_cophieu68 import ExtractCophieu68
from platforms.processing.prefect.flows.prefect_orchestra_etl import PrefectETLPipelineConfig
from platforms.ingestion.cophieu68.load.load_datalake_cophieu68 import MongoLoader
from platforms.storage.datalake.mongodb.data_lake_storage import MongoStorageBackend
from platforms.ingestion.cophieu68.dto.extract_models import (
    CRAWL_MARKET_LIST_CONFIG,
    INDUSTRIAL_INFO_TYPE
)
from shared.logger.python_main_logger import FastLogger
from platforms.processing.base_processing import FileConfigLoader, DefaultLoggerFactory



config_path = "./platforms/processing/prefect/config/cophieu68_config.yaml"
config = PrefectETLPipelineConfig(config_path=config_path)
mongo_config_etl_arg =config.config.get("storage", {}).get("mongodb", {}).get("reties_etl_flows", {})

def build_crawler(config, logger=None):
    crawler = ExtractCophieu68(pipeline_config=config, pipeline_logger=logger)
    try:
        crawler.endpoint = crawler.crawler_cfg.get("endpoints", {})
    except:
        crawler.endpoint = {}
    return crawler


def build_backend(config:PrefectETLPipelineConfig, logger):
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

# @task(retries=mongo_config_etl_arg.get("retries", 3), retry_delay_seconds=mongo_config_etl_arg.get("backoff_seconds", 5))
def task_schedule_market_list(mongo_config, 
                              crawler: ExtractCophieu68, 
                              backend_mongo: MongoStorageBackend, 
                              loading_datalake: MongoLoader,
                              loading_pipeline_logger: None):
    """
    Schedules crawling of market list from website.

    Parameters
    ----------
    mongo_config : dict
        Configuration for MongoDB.
    crawler : ExtractCophieu68
        Object for crawling data from website.
    backend_mongo : MongoStorageBackend
        Object for storing data in MongoDB.
    loading_datalake : MongoLoader
        Object for loading data into MongoDB.

    Returns
    -------
    None
    """
    stock_lists = []
    for key in CRAWL_MARKET_LIST_CONFIG:
        stock_list_market_type = crawler.crawl_market_list(key)
        stock_lists.append(stock_list_market_type)
        time.sleep(mongo_config.get("delay_call", 0.25))
    list_stock_collection = mongo_config.get("collections", {}).get("list_stock", "list_stock")
    mongo_schema = mongo_config.get("documentation", {}).get("market_list", "market_list")
    try:
        backend_mongo.create_table(name=list_stock_collection, schema=mongo_schema)
        loading_datalake.load_market_list(
            collection_name=list_stock_collection,
            stock_list=stock_lists
        )
    except Exception as e:
        loading_pipeline_logger.error(f"Error in task_schedule_market_list: {e}")

# @task(retries=mongo_config_etl_arg.get("retries", 3), retry_delay_seconds=mongo_config_etl_arg.get("backoff_seconds", 5))
def task_schedule_industry_info(    mongo_config, 
                                    crawler: ExtractCophieu68, 
                                    backend_mongo: MongoStorageBackend, 
                                    loading_datalake: MongoLoader,
                                    loading_pipeline_logger: None):
    """
    Schedules crawling of industry information from website.

    Parameters
    ----------
    mongo_config : dict
        Configuration for MongoDB.
    crawler : ExtractCophieu68
        Object for crawling data from website.
    backend_mongo : MongoStorageBackend
        Object for storing data in MongoDB.
    loading_datalake : MongoLoader
        Object for loading data into MongoDB.

    Returns
    -------
    None
    """
    industry_list_collection = mongo_config.get("collections", {}).get("industry_list", "industry_list")
    mongo_schema = mongo_config.get("documentation", {}).get("industry_info", "industry_info")
    industry_info = {}
    for key in INDUSTRIAL_INFO_TYPE:
        industry_list = crawler.crawl_industry_info(type_info = key)
        industry_info[key] = industry_list
        time.sleep(mongo_config.get("delay_call", 0.25))
    
    try:
        backend_mongo.create_table(name=industry_list_collection, schema=mongo_schema)
        loading_datalake.load_crawl_industry_info(
            collection_name=industry_list_collection,
            industry_data=industry_info
        )
    except Exception as e:
        loading_pipeline_logger.error(f"Error in task_schedule_industry_info: {e}")

# @task(retries=mongo_config_etl_arg.get("retries", 3), retry_delay_seconds=mongo_config_etl_arg.get("backoff_seconds", 5))
def task_schedule_company_profile(  mongo_config, 
                                    crawler: ExtractCophieu68, 
                                    backend_mongo: MongoStorageBackend, 
                                    loading_datalake: MongoLoader,
                                    symbols_list: List,
                                    loading_pipeline_logger: None):
    company_profile_collection = mongo_config.get("collections", {}).get("stock_info", "company_profile")
    symbol_info = {}
    for symbol in symbols_list:
        company_profile = crawler.crawl_company_profile(symbol)
        symbol_info[symbol] = company_profile.__dict__
        time.sleep(mongo_config.get("delay_call", 0.25))
    mongo_schema = mongo_config.get("documentation", {}).get("company_info", "company_info")
    try:
        backend_mongo.create_table(name=company_profile_collection, schema=mongo_schema)
        loading_datalake.load_company_info(
            collection_name=company_profile_collection,
            company_profiles=symbol_info
        )
    except Exception as e:
        loading_pipeline_logger.error(f"Error in task_schedule_company_profile: {e}")

def task_schedule_match_details(mongo_config, 
                                crawler: ExtractCophieu68, 
                                backend_mongo: MongoStorageBackend, 
                                loading_datalake: MongoLoader,
                                symbols_list: List,
                                loading_pipeline_logger: None):
    match_details_collection = mongo_config.get("collections", {}).get("match_details", "match_details")
    symbol_info = {}
    for symbol in symbols_list:
        match_details_data = crawler.crawl_details_match(symbol)
        symbol_info[symbol] = match_details_data
        time.sleep(mongo_config.get("delay_call", 0.25))
    mongo_schema = mongo_config.get("documentation", {}).get("match_details", "match_details")
    try:
        backend_mongo.create_table(name=match_details_collection, schema=mongo_schema)
        loading_datalake.load_details_match(
            collection_name=match_details_collection,
            match_date=symbol_info
        )
    except Exception as e:
        loading_pipeline_logger.error(f"Error in task_schedule_company_profile: {e}")

# @task(retries=mongo_config_etl_arg.get("retries", 3), retry_delay_seconds=mongo_config_etl_arg.get("backoff_seconds", 5))
def task_schedule_financial_summary(mongo_config, 
                                    crawler: ExtractCophieu68, 
                                    backend_mongo: MongoStorageBackend, 
                                    loading_datalake: MongoLoader,
                                    symbols_list: List,
                                    loading_pipeline_logger):
    mongo_financial_summary = mongo_config.get("collections", {}).get("financial_report_summary", "financial_report_summary")
    mongo_schema = mongo_config.get("documentation", {}).get("financial_summary", "financial_summary")
    try:
        backend_mongo.create_table(name=mongo_financial_summary, schema=mongo_schema)
        for symbol in symbols_list:
            financial_summary_report = crawler.crawl_financial_report_summary(symbol = symbol)
            if financial_summary_report:
                loading_datalake.load_financial_report_summary(collection_name = mongo_financial_summary, 
                                                            report_data = financial_summary_report)
            time.sleep(mongo_config.get("delay_call", 0.25))
    except Exception as e:
        loading_pipeline_logger.error(f"Error in task_schedule_financial_summary: {e}")


# @task(retries=mongo_config_etl_arg.get("retries", 3), retry_delay_seconds=mongo_config_etl_arg.get("backoff_seconds", 5))
def task_schedule_business_plan(mongo_config, 
                                    crawler: ExtractCophieu68, 
                                    backend_mongo: MongoStorageBackend, 
                                    loading_datalake: MongoLoader,
                                    symbols_list: List,
                                    loading_pipeline_logger: None):
    business_plan_collection = mongo_config.get("collections", {}).get("business_plan", "business_plan")
    mongo_schema_business_plan = mongo_config.get("documentation", {}).get("business_plan", "business_plan")
    backend_mongo.create_table(name=business_plan_collection, schema=mongo_schema_business_plan)
    for symbol in symbols_list:
        business_plan_data = crawler.crawl_business_plan(symbol = symbol)
        if  business_plan_data:
            try:
                loading_datalake.load_business_plan(collection_name= business_plan_collection, business_plan_data= business_plan_data)
            except Exception as e:
                loading_pipeline_logger.error(f"Error loading business plan for symbol {symbol}: {e}")
        time.sleep(mongo_config.get("delay_call", 0.25)) 

# @task(retries=mongo_config_etl_arg.get("retries", 3), retry_delay_seconds=mongo_config_etl_arg.get("backoff_seconds", 5))
def task_schedule_details_financial_statement_quarterly(mongo_config, 
                                    crawler: ExtractCophieu68, 
                                    backend_mongo: MongoStorageBackend, 
                                    loading_datalake: MongoLoader,
                                    symbols_list: List,
                                    loading_pipeline_logger: None):
    income_statement_quarterly = mongo_config.get("collections", {}).get("income_statement_quarterly", "income_statement_quarterly")
    balance_sheet_quarterly = mongo_config.get("collections", {}).get("balance_sheet_quarterly", "balance_sheet_quarterly")
    mongo_schema_income_statement = mongo_config.get("documentation", {}).get("income_statement", "income_statement")
    mongo_schema_balance_sheet = mongo_config.get("documentation", {}).get("balance_sheet", ("balance_sheet"))
    backend_mongo.create_table(name=income_statement_quarterly, schema=mongo_schema_income_statement)
    backend_mongo.create_table(name=balance_sheet_quarterly, schema=mongo_schema_balance_sheet)
    for symbol in symbols_list:
        crawl_details_income_statement = crawler.crawl_details_income_statement(symbol=symbol, report_type="quarter")
        crawl_details_balance_sheet = crawler.crawl_details_balance_sheet(symbol=symbol, report_type="quarter")
        try:
            loading_datalake.load_detail_income_statement_quarterly(collection_name=income_statement_quarterly, income_data=crawl_details_income_statement)
        except Exception as e:
            loading_pipeline_logger.error(f"Error loading income statement quarterly for symbol {symbol}: {e}")
        time.sleep(mongo_config.get("delay_call", 0.25))
        try:
            loading_datalake.load_detail_balance_sheet_quarterly(collection_name=income_statement_quarterly, balance_data=crawl_details_balance_sheet)
        except Exception as e:
            loading_pipeline_logger.error(f"Error loading balance sheet quarterly for symbol {symbol}: {e}")

        time.sleep(mongo_config.get("delay_call", 0.25)) 

# @task(retries=mongo_config_etl_arg.get("retries", 3), retry_delay_seconds=mongo_config_etl_arg.get("backoff_seconds", 5))
def task_schedule_details_financial_statement_annually(mongo_config, 
                                    crawler: ExtractCophieu68, 
                                    backend_mongo: MongoStorageBackend, 
                                    loading_datalake: MongoLoader,
                                    symbols_list: List,
                                    loading_pipeline_logger: None):
    income_statement_annually = mongo_config.get("collections", {}).get("income_statement_annually", "income_statement_annually")
    balance_sheet_annually = mongo_config.get("collections", {}).get("balance_sheet_annually", "balance_sheet_annually")
    mongo_schema_income_statement = mongo_config.get("documentation", {}).get("income_statement", "income_statement")
    mongo_schema_balance_sheet = mongo_config.get("documentation", {}).get("balance_sheet", "balance_sheet")
    backend_mongo.create_table(name=income_statement_annually, schema=mongo_schema_income_statement)
    backend_mongo.create_table(name=balance_sheet_annually, schema=mongo_schema_balance_sheet)
    for symbol in symbols_list:
        crawl_details_income_statement = crawler.crawl_details_income_statement(symbol=symbol, report_type="year")
        crawl_details_balance_sheet = crawler.crawl_details_balance_sheet(symbol=symbol, report_type="year")
        try:
            loading_datalake.load_detail_income_statement_annually(collection_name=income_statement_annually, income_data=crawl_details_income_statement)
        except Exception as e:
            loading_pipeline_logger.error(f"Error loading income statement annually for symbol {symbol}: {e}")
        time.sleep(mongo_config.get("delay_call", 0.25))

        try:
            loading_datalake.load_detail_balance_sheet_annually(collection_name=balance_sheet_annually, balance_data=crawl_details_balance_sheet)
        except Exception as e:
            loading_pipeline_logger.error(f"Error loading balance sheet annually for symbol {symbol}: {e}")
        time.sleep(mongo_config.get("delay_call", 0.25)) 

# @task(retries=mongo_config_etl_arg.get("retries", 3), retry_delay_seconds=mongo_config_etl_arg.get("backoff_seconds", 5))
def task_schedule_details_financial_ratios(mongo_config, 
                                            crawler: ExtractCophieu68, 
                                            backend_mongo: MongoStorageBackend, 
                                            loading_datalake: MongoLoader,
                                            symbols_list: List,
                                            loading_pipeline_logger: None):
    financial_info_collection = mongo_config.get("collections", {}).get("financial_info", "financial_info")
    mongo_schema = mongo_config.get("documentation", {}).get("financial_info", "financial_info")
    backend_mongo.create_table(name=financial_info_collection, schema=mongo_schema)
    for symbol in symbols_list:
        financial_ratios = crawler.crawl_financial_ratios(symbol = symbol)
        try:
            loading_datalake.load_crawl_financial_info(
                collection_name = financial_info_collection,
                financial_data = financial_ratios
            )
        except Exception as e:
            loading_pipeline_logger.error(f"Error loading financial ratios for symbol {symbol}: {e}")
        time.sleep(mongo_config.get("delay_call", 0.25))  

# @task(retries=mongo_config_etl_arg.get("retries", 3), retry_delay_seconds=mongo_config_etl_arg.get("backoff_seconds", 5))
def task_schedule_details_trading_data(mongo_config, 
                                        crawler: ExtractCophieu68, 
                                        backend_mongo: MongoStorageBackend, 
                                        loading_datalake: MongoLoader,
                                        symbols_list: List,
                                        loading_pipeline_logger: None):
    trading_data_collection = mongo_config.get("collections", {}).get("trading_data", "trading_data")
    mongo_schema = mongo_config.get("documentation", {}).get("trading_data", "trading_data")
    backend_mongo.create_table(name=trading_data_collection, schema=mongo_schema)

    max_attempts = mongo_config.get("retries_loading", {}).get("max_attempts", 3)
    backoff_seconds = mongo_config.get("retries_loading", {}).get("backoff_seconds", 0.5)
    delay_call = mongo_config.get("delay_call", 0.25)

    for index, symbol in enumerate(symbols_list):
        retry_count = 0
        while retry_count <= max_attempts:
            try:
                crawl_trading_data = crawler.crawl_trading_data(symbol=symbol)
                break
            except Exception as e:
                retry_count += 1
                time.sleep(backoff_seconds)

        if retry_count > max_attempts:
            loading_pipeline_logger.error(f"Failed to crawl trading data for symbol {symbol}, index {index} after {max_attempts} attempts. Error: {e}")
            continue
        try:
            loading_datalake.load_trading_data(collection_name=trading_data_collection, trading_data=crawl_trading_data)
        except Exception as e:
            loading_pipeline_logger.error(f"Failed to load trading data for symbol {symbol}, index {index} Error: {e}")
        time.sleep(delay_call)

# @task(retries=mongo_config_etl_arg.get("retries", 3), retry_delay_seconds=mongo_config_etl_arg.get("backoff_seconds", 5))
def get_symbol_list(config , logger):
    mongo_config, _, backend_mongo = build_backend(config, logger)
    list_stock_collection = mongo_config.get("collections", {}).get("list_stock", "list_stock")
    symbol_data = backend_mongo.find_table(name = list_stock_collection)
    symbol_list = [item["symbols"] for item in symbol_data["data"] if item["market_type"] == "VNINDEX"]
    return symbol_list[0]



# @flow(name=mongo_config_etl_arg.get("etl_name", "cophieu68_etl_flow"))
def cophieu68_etl_flow(config_path):
    pipeline_config = PrefectETLPipelineConfig(config_path=config_path, config_loader=FileConfigLoader(), logger_factory=DefaultLoggerFactory())
    extract_pipeline_logger = pipeline_config.cophieu68_extract_logger
    loading_pipeline_logger = pipeline_config.cophieu68_load_logger
    mongo_config, loading_datalake, backend_mongo = build_backend(pipeline_config, loading_pipeline_logger)
    config_info= pipeline_config._config
    crawler=build_crawler(config_info, extract_pipeline_logger)
    # task_schedule_market_list(  
    #                             mongo_config = mongo_config,
    #                             crawler=crawler,
    #                             backend_mongo=backend_mongo,
    #                             loading_datalake=loading_datalake,
    #                             loading_pipeline_logger=loading_pipeline_logger
    #                           )
    # print("Market List done")
    # time.sleep(mongo_config.get("delay_call", 0.25))
    

    # task_schedule_industry_info(    
    #                             mongo_config = mongo_config,
    #                             crawler=crawler,
    #                             backend_mongo=backend_mongo,
    #                             loading_datalake=loading_datalake,
    #                             loading_pipeline_logger=loading_pipeline_logger
    #                             )
    # print("Industry info done")
    # time.sleep(mongo_config.get("delay_call", 0.25))

    
    symbol_list = get_symbol_list(pipeline_config, loading_pipeline_logger)
    time.sleep(mongo_config.get("delay_call", 0.25))

    # task_schedule_details_financial_ratios(
    #                                     mongo_config = mongo_config,
    #                                     crawler=crawler,
    #                                     backend_mongo=backend_mongo,
    #                                     loading_datalake=loading_datalake,
    #                                     symbols_list = symbol_list,
    #                                     loading_pipeline_logger = loading_pipeline_logger
    # )
    # print("financial ratios done")
    # time.sleep(mongo_config.get("delay_call", 0.25))

    
    # task_schedule_company_profile(
    #                                 mongo_config = mongo_config,
    #                                 crawler=crawler,
    #                                 backend_mongo=backend_mongo,
    #                                 loading_datalake=loading_datalake,
    #                                 symbols_list=symbol_list,
    #                                 loading_pipeline_logger = loading_pipeline_logger
    #                             )
    # print("company profile done")
    # time.sleep(mongo_config.get("delay_call", 0.25))
    

    # task_schedule_details_financial_statement_annually(
    #                                 mongo_config = mongo_config,
    #                                 crawler=crawler,
    #                                 backend_mongo=backend_mongo,
    #                                 loading_datalake=loading_datalake,
    #                                 symbols_list=symbol_list,
    #                                 loading_pipeline_logger = loading_pipeline_logger
    #                                 )
    # print("financial statement annually done")
    # time.sleep(mongo_config.get("delay_call", 0.25))

    task_schedule_details_financial_statement_quarterly(
                                    mongo_config = mongo_config,
                                    crawler=crawler,
                                    backend_mongo=backend_mongo,
                                    loading_datalake=loading_datalake,
                                    symbols_list=symbol_list,
                                    loading_pipeline_logger = loading_pipeline_logger
                                    )
    print("financial statement quarterly done")
    time.sleep(mongo_config.get("delay_call", 0.25))

    # task_schedule_business_plan(
    #                                 mongo_config = mongo_config,
    #                                 crawler=crawler,
    #                                 backend_mongo=backend_mongo,
    #                                 loading_datalake=loading_datalake,
    #                                 symbols_list=symbol_list,
    #                                 loading_pipeline_logger = loading_pipeline_logger
    #                                 )
    # print("business plan done")
    # time.sleep(mongo_config.get("delay_call", 0.25))

    
    # task_schedule_match_details(
    #                                 mongo_config = mongo_config,
    #                                 crawler=crawler,
    #                                 backend_mongo=backend_mongo,
    #                                 loading_datalake=loading_datalake,
    #                                 symbols_list=symbol_list,
    #                                 loading_pipeline_logger = loading_pipeline_logger
    #                                 )
    # print("match details done")
    # time.sleep(mongo_config.get("delay_call", 0.25))

    # task_schedule_details_trading_data(
    #                                 mongo_config = mongo_config,
    #                                 crawler=crawler,
    #                                 backend_mongo=backend_mongo,
    #                                 loading_datalake=loading_datalake,
    #                                 symbols_list=symbol_list,
    #                                 loading_pipeline_logger = loading_pipeline_logger
    #                                 )
    # print("trading data done")
    # time.sleep(mongo_config.get("delay_call", 0.25))

    print("ETL data done")
if __name__ == "__main__":
    cophieu68_etl_flow(config_path = config_path)