# fact_table_load.py (refactored)
import pandas as pd
from datetime import datetime
from utils import TableCreator
from internal.dags.cophieu68_dag.transform.base_transform import TransformDatawarehouse
from internal.dags.cophieu68_dag.transform.postgres_sql_dw.cophieu68_metadata import *
import logging

logger = logging.getLogger(__name__)

# -------------------------
# Base FactLoader
# -------------------------
class FactLoader(TransformDatawarehouse):
    """
    Base class for FACT loaders.
    NOTE: TransformDatawarehouse.__init__ expects (datalake_config, datawarehouse_logger, postgres_client)
    """

    def __init__(self, datalake_config, datawarehouse_logger, postgres_client, dim_repo, table_creator: TableCreator):
        # call base to initialize self.mongo, self.repo (meta repo) etc.
        super().__init__(datalake_config, datawarehouse_logger, postgres_client)
        self.dim_repo = dim_repo
        self.table_creator = table_creator
        self.datawarehouse_logger = datawarehouse_logger

    def create_fact_table_sql(self, fact_name: str) -> str:
        table_info = self.schema_dw["facts"][fact_name]["columns"]
        # table_info values may already be dict-like or strings; normalize if needed
        norm = {}
        for col, meta in table_info.items():
            if isinstance(meta, dict) and "type" in meta:
                norm[col] = meta
            else:
                # meta is a string like "NUMERIC" or "NUMERIC NOT NULL"
                t = str(meta)
                # split first token as type and remainder as constraints
                parts = t.split(None, 1)
                typ = parts[0]
                cons = parts[1] if len(parts) > 1 else ""
                norm[col] = {"type": typ, "constraints": cons}
        return self.table_creator.generate_create_table_sql(fact_name, norm)

    # Mapping DIM surrogate key (must pass generator)
    def get_company_key(self, symbol: str) -> str:
        return self.dim_repo.get_or_create(symbol, "dim_company", self.table_creator)

    def get_report_type_key(self, report_type: str) -> str:
        return self.dim_repo.get_or_create(report_type, "dim_report_type", self.table_creator)

    def get_date_key(self, date_str: str) -> str:
        """Convert date 'YYYY-MM-DD' → surrogate key (stores natural key in meta table)"""
        return self.dim_repo.get_or_create(date_str, "dim_date", self.table_creator)


# -------------------------
# FactTradeLoader
# -------------------------
class FactTradeLoader(FactLoader):

    def __init__(self, datalake_config, datawarehouse_logger, postgres_client, dim_repo):
        # create a table_creator with proper character_specific for trade
        table_creator = TableCreator(machine_id=1, character_specific=dim_trade_info.get("character_specific"))
        super().__init__(datalake_config, datawarehouse_logger, postgres_client, dim_repo, table_creator)

    def load(self):
        raw = list(self.mongo.find_table("trading_data") or [])
        rows = []
        for doc in raw:
            symbol = doc.get("symbol")
            if not symbol:
                self.datawarehouse_logger.warning("trading_data doc without symbol: %s", doc)
                continue
            company_key = self.get_company_key(symbol)
            records = doc.get("records", [])
            for r in records:
                trade_date_key = self.get_date_key(r.get("date"))
                rows.append({
                    "trade_key": self.table_creator.get_id(),
                    "trade_datetime": r.get("date"),
                    "trade_date_key": trade_date_key,
                    "company_key": company_key,
                    "price": r.get("close_price"),
                    "volume": r.get("volume"),
                    "value": (r.get("close_price") or 0) * (r.get("volume") or 0),
                    "side": r.get("side", "NA"),
                    "source_json": r
                })

        df = pd.DataFrame(rows)
        sql = self.create_fact_table_sql("fact_trade")
        return df, sql


# -------------------------
# FactMatchDetailLoader
# -------------------------
class FactMatchDetailLoader(FactLoader):

    def __init__(self, datalake_config, datawarehouse_logger, postgres_client, dim_repo):
        table_creator = TableCreator(machine_id=1, character_specific=dim_match_info.get("character_specific"))
        super().__init__(datalake_config, datawarehouse_logger, postgres_client, dim_repo, table_creator)

    def load(self):
        raw = list(self.mongo.find_table("match_details") or [])
        rows = []
        for doc in raw:
            symbol = doc.get("symbol")
            if not symbol:
                self.datawarehouse_logger.warning("match_details doc without symbol: %s", doc)
                continue
            company_key = self.get_company_key(symbol)
            for r in doc.get("data", []):
                rows.append({
                    "match_key": self.table_creator.get_id(),
                    "company_key": company_key,
                    "match_datetime": r.get("Time_match"),
                    "price": r.get("Price_match"),
                    "volume": r.get("Volume"),
                    "broker": r.get("Broker"),
                    "source_json": r
                })
        df = pd.DataFrame(rows)
        sql = self.create_fact_table_sql("fact_match_detail")
        return df, sql


# -------------------------
# FactIncomeStatementLoader
# -------------------------
class FactIncomeStatementLoader(FactLoader):

    def __init__(self, datalake_config, datawarehouse_logger, postgres_client, dim_repo):
        table_creator = TableCreator(machine_id=1, character_specific=dim_income_info.get("character_specific"))
        super().__init__(datalake_config, datawarehouse_logger, postgres_client, dim_repo, table_creator)

    def load(self):
        raw = []
        raw.extend(list(self.mongo.find_table("income_statement_yearly") or []))
        raw.extend(list(self.mongo.find_table("income_statement_quarterly") or []))
        rows = []
        for doc in raw:
            symbol = doc.get("symbol")
            if not symbol:
                self.datawarehouse_logger.warning("income_statement doc without symbol: %s", doc)
                continue
            company_key = self.get_company_key(symbol)
            report_type_key = self.get_report_type_key(doc.get("report_type"))
            for r in doc.get("data", []):
                period_date_key = self.get_date_key(r.get("period"))
                rows.append({
                    "income_key": self.table_creator.get_id(),
                    "company_key": company_key,
                    "report_type_key": report_type_key,
                    "period_date_key": period_date_key,
                    "revenue": r.get("revenue"),
                    "operating_profit": r.get("operating_profit"),
                    "net_income": r.get("net_income"),
                    "eps": r.get("eps"),
                    "source_json": r
                })
        df = pd.DataFrame(rows)
        sql = self.create_fact_table_sql("fact_income_statement")
        return df, sql


# -------------------------
# FactBalanceSheetLoader
# -------------------------
class FactBalanceSheetLoader(FactLoader):

    def __init__(self, datalake_config, datawarehouse_logger, postgres_client, dim_repo):
        table_creator = TableCreator(machine_id=1, character_specific=dim_balance_info.get("character_specific"))
        super().__init__(datalake_config, datawarehouse_logger, postgres_client, dim_repo, table_creator)

    def load(self):
        raw = []
        raw.extend(list(self.mongo.find_table("balance_sheet_yearly") or []))
        raw.extend(list(self.mongo.find_table("balance_sheet_quarterly") or []))
        rows = []
        for doc in raw:
            symbol = doc.get("symbol")
            if not symbol:
                self.datawarehouse_logger.warning("balance_sheet doc without symbol: %s", doc)
                continue
            company_key = self.get_company_key(symbol)
            report_type_key = self.get_report_type_key(doc.get("report_type"))
            for r in doc.get("data", []):
                period_date_key = self.get_date_key(r.get("period"))
                rows.append({
                    "bs_key": self.table_creator.get_id(),
                    "company_key": company_key,
                    "report_type_key": report_type_key,
                    "period_date_key": period_date_key,
                    "total_assets": r.get("total_assets"),
                    "total_liabilities": r.get("total_liabilities"),
                    "shareholder_equity": r.get("shareholder_equity"),
                    "cash": r.get("cash"),
                    "inventory": r.get("inventory"),
                    "source_json": r
                })
        df = pd.DataFrame(rows)
        sql = self.create_fact_table_sql("fact_balance_sheet")
        return df, sql


# -------------------------
# FactBusinessPlanLoader
# -------------------------
class FactBusinessPlanLoader(FactLoader):
    COLLECTION = "business_plan"
    FACT_NAME = "fact_business_plan"

    def __init__(self, datalake_config, datawarehouse_logger, postgres_client, dim_repo):
        table_creator = TableCreator(machine_id=1, character_specific=fact_business_plan_info.get("character_specific"))
        super().__init__(datalake_config, datawarehouse_logger, postgres_client, dim_repo, table_creator)

    def load(self):
        try:
            raw_docs = list(self.mongo.find_table(self.COLLECTION) or [])
        except Exception as e:
            self.datawarehouse_logger.exception("Failed to read business_plan from mongo: %s", e)
            raw_docs = []

        rows = []
        for doc in raw_docs:
            symbol = doc.get("symbol")
            if not symbol:
                self.datawarehouse_logger.warning("skip business_plan doc without symbol: %s", doc)
                continue

            company_key = self.get_company_key(symbol)
            data_list = doc.get("data") or []
            if isinstance(data_list, dict):
                data_list = [data_list]

            for r in data_list:
                year_raw = r.get("Year") or r.get("year") or r.get("period")
                try:
                    year_int = int(str(year_raw)[:4])
                    period_date = datetime(year_int, 12, 31).date()
                    period_date_key = self.dim_repo.get_or_create(period_date.isoformat(), "dim_date", self.table_creator)
                except Exception:
                    period_date_key = self.dim_repo.get_or_create(str(year_raw), "dim_date", self.table_creator)

                rows.append({
                    "plan_key": self.table_creator.get_id(),
                    "company_key": company_key,
                    "year_key": period_date_key,
                    "target_revenue": r.get("Plan_revenue") or r.get("target_revenue"),
                    "target_profit": r.get("Plan_profit") or r.get("target_profit"),
                    "capex_plan": r.get("Capex") or r.get("capex_plan") or None,
                    "source_json": r
                })

        df = pd.DataFrame(rows)
        sql = self.create_fact_table_sql(self.FACT_NAME)
        return df, sql


# -------------------------
# FactFinancialMetricsLoader
# -------------------------
class FactFinancialMetricsLoader(FactLoader):
    COLLECTION = "financial_info"
    FACT_NAME = "fact_financial_metrics"

    def __init__(self, datalake_config, datawarehouse_logger, postgres_client, dim_repo):
        table_creator = TableCreator(machine_id=1, character_specific=fact_financial_metrics_info.get("character_specific"))
        super().__init__(datalake_config, datawarehouse_logger, postgres_client, dim_repo, table_creator)

    def load(self):
        try:
            raw_docs = list(self.mongo.find_table(self.COLLECTION) or [])
        except Exception as e:
            self.datawarehouse_logger.exception("Failed to read financial_info from mongo: %s", e)
            raw_docs = []

        rows = []
        for doc in raw_docs:
            symbol = doc.get("symbol")
            if not symbol:
                self.datawarehouse_logger.warning("skip financial_info doc without symbol: %s", doc)
                continue

            company_key = self.get_company_key(symbol)
            data_list = doc.get("data") or doc.get("metrics") or []
            if isinstance(data_list, dict):
                data_list = [data_list]

            for r in data_list:
                period_raw = r.get("period") or r.get("report_date") or r.get("date")
                if period_raw:
                    period_dt = pd.to_datetime(period_raw, errors="coerce")
                    if pd.isna(period_dt):
                        period_key = self.dim_repo.get_or_create(str(period_raw), "dim_date", self.table_creator)
                    else:
                        period_key = self.dim_repo.get_or_create(period_dt.date().isoformat(), "dim_date", self.table_creator)
                else:
                    period_key = self.dim_repo.get_or_create("latest", "dim_date", self.table_creator)

                rows.append({
                    "metric_key": self.table_creator.get_id(),
                    "company_key": company_key,
                    "period_date_key": period_key,
                    "pe": r.get("pe"),
                    "roe": r.get("roe"),
                    "roa": r.get("roa"),
                    "debt_equity": r.get("debt_equity") or r.get("debtToEquity") or r.get("debt_to_equity"),
                    "market_cap": r.get("market_cap") or r.get("marketCap"),
                    "source_json": r
                })

        df = pd.DataFrame(rows)
        sql = self.create_fact_table_sql(self.FACT_NAME)
        return df, sql
