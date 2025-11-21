# run_dw_load.py

from prefect import task, flow
from datetime import datetime
import logging
import traceback
from internal.dags.cophieu68_dag.transform.postgres_sql_dw.dim_table_load import (
    DimMarketTypeLoader, DimIndustryLoader, DimCompanyLoader
)
from internal.dags.cophieu68_dag.transform.postgres_sql_dw.fact_table_load import (
    FactTradeLoader, FactMatchDetailLoader,
    FactIncomeStatementLoader, FactBalanceSheetLoader,
    FactBusinessPlanLoader, FactFinancialMetricsLoader
)

from internal.storage.postgres_client import PostgresClient

LOGGER = logging.getLogger("DW_LOAD")


@task
def load_dim_market_type(datalake_config, pg_client):
    loader = DimMarketTypeLoader(
        datalake_config=datalake_config,
        logger=LOGGER,
        postgres_client=pg_client
    )
    return run_loader(loader, "dim_market_type", pg_client)


@task
def load_dim_industry(datalake_config, pg_client):
    loader = DimIndustryLoader(
        datalake_config,
        logger=LOGGER,
        postgres_client=pg_client
    )
    return run_loader(loader, "dim_industry", pg_client)


@task
def load_dim_company(datalake_config, pg_client):
    loader = DimCompanyLoader(
        datalake_config,
        logger=LOGGER,
        postgres_client=pg_client
    )
    dim_dict = loader.load()

    for tbl, (df, sql) in dim_dict.items():
        pg_client.execute(sql)
        pg_client.bulk_insert(df, tbl)
        LOGGER.info(f"[{tbl}] Loaded {len(df)} rows")

    return True


# =========================
# FACT LOADING
# =========================
@task
def load_fact_trade(datalake_config, pg_client, dim_repo):
    loader = FactTradeLoader(datalake_config, LOGGER, pg_client, dim_repo)
    return run_loader(loader, "fact_trade", pg_client)


@task
def load_fact_match(datalake_config, pg_client, dim_repo):
    loader = FactMatchDetailLoader(datalake_config, LOGGER, pg_client, dim_repo)
    return run_loader(loader, "fact_match_detail", pg_client)


@task
def load_fact_income(datalake_config, pg_client, dim_repo):
    loader = FactIncomeStatementLoader(datalake_config, LOGGER, pg_client, dim_repo)
    return run_loader(loader, "fact_income_statement", pg_client)


@task
def load_fact_balance(datalake_config, pg_client, dim_repo):
    loader = FactBalanceSheetLoader(datalake_config, LOGGER, pg_client, dim_repo)
    return run_loader(loader, "fact_balance_sheet", pg_client)


@task
def load_fact_business_plan(datalake_config, pg_client, dim_repo):
    loader = FactBusinessPlanLoader(datalake_config, LOGGER, pg_client, dim_repo)
    return run_loader(loader, "fact_business_plan", pg_client)


@task
def load_fact_financial_metrics(datalake_config, pg_client, dim_repo):
    loader = FactFinancialMetricsLoader(datalake_config, LOGGER, pg_client, dim_repo)
    return run_loader(loader, "fact_financial_metrics", pg_client)


# =========================
# MAIN PREFECT FLOW
# =========================
@flow(name="DW-Full-Load")
def dw_full_load():
    LOGGER.info("Initializing PostgreSQL client...")
    pg_client = PostgresClient()  # Should include connection config

    datalake_config = {
        "username": "root",
        "password": "password",
        "host": "localhost",
        "port": 27017,
        "database": "ETL_Project",
        "authSource": "admin"
    }

    # ===================================================
    # 1️⃣ LOAD DIMENSIONS (surrogate keys first)
    # ===================================================
    LOGGER.info("===== LOADING DIMENSIONS =====")

    load_dim_market_type(datalake_config, pg_client)
    load_dim_industry(datalake_config, pg_client)
    load_dim_company(datalake_config, pg_client)

    # The dim_repo now uses the pg_client inside loaders
    dim_repo = None  # meta repo is inside loader → no passing needed actually

    # ===================================================
    # 2️⃣ LOAD FACT TABLES
    # ===================================================
    LOGGER.info("===== LOADING FACTS =====")

    load_fact_trade(datalake_config, pg_client, dim_repo)
    load_fact_match(datalake_config, pg_client, dim_repo)
    load_fact_income(datalake_config, pg_client, dim_repo)
    load_fact_balance(datalake_config, pg_client, dim_repo)
    load_fact_business_plan(datalake_config, pg_client, dim_repo)
    load_fact_financial_metrics(datalake_config, pg_client, dim_repo)

    LOGGER.info("===== DATA WAREHOUSE LOAD COMPLETED =====")


if __name__ == "__main__":
    dw_full_load()
