import pandas as pd
from internal.models.cophieu68_model.transform_models import DATA_WAREHOUSE_SCHEMA as schema_dw
from internal.dags.cophieu68_dag.transform.postgres_sql_dw.cophieu68_metadata import *

class FactLoader:
    """
    Base class dùng cho tất cả FACT tables.
    - Nhận raw Mongo JSON
    - Lookup surrogate keys
    - Convert thành bảng FACT theo DW schema
    - Trả về (DataFrame, create_table_sql)
    """

    def __init__(self, mongo_backend, dim_repo, table_creator=None):
        self.mongo = mongo_backend
        self.dim_repo = dim_repo
        self.table_creator = table_creator or TableCreator()

    def create_fact_table_sql(self, fact_name: str) -> str:
        table_info = schema_dw["facts"][fact_name]["columns"]
        return self.table_creator.generate_create_table_sql(
            fact_name,
            {col: {"type": col_type} for col, col_type in table_info.items()}
        )

    # Mapping DIM surrogate key
    def get_company_key(self, symbol: str) -> str:
        return self.dim_repo.get_surrogate(symbol, "dim_company")

    def get_report_type_key(self, report_type: str) -> str:
        return self.dim_repo.get_surrogate(report_type, "dim_report_type")

    def get_date_key(self, date_str: str) -> str:
        """Convert date 'YYYY-MM-DD' → surrogate key"""
        return self.dim_repo.get_surrogate(date_str, "dim_date")

class FactTradeLoader(FactLoader):

    def load(self):
        raw = self.mongo.find_table("trading_data")

        rows = []
        for doc in raw:
            company_key = self.get_company_key(doc["symbol"])
            for r in doc["records"]:
                trade_date_key = self.get_date_key(r["date"])

                rows.append({
                    "trade_key": self.table_creator.get_id(),
                    "trade_datetime": r["date"],
                    "trade_date_key": trade_date_key,
                    "company_key": company_key,
                    "price": r["close_price"],
                    "volume": r["volume"],
                    "value": r["close_price"] * r["volume"],
                    "side": "NA",
                    "source_json": r
                })

        df = pd.DataFrame(rows)
        sql = self.create_fact_table_sql("fact_trade")
        return df, sql
class FactMatchDetailLoader(FactLoader):

    def load(self):
        raw = self.mongo.find_table("match_details")

        rows = []
        for doc in raw:
            company_key = self.get_company_key(doc["symbol"])
            for r in doc["data"]:
                rows.append({
                    "match_key": self.table_creator.get_id(),
                    "company_key": company_key,
                    "match_datetime": r["Time_match"],
                    "price": r["Price_match"],
                    "volume": r["Volume"],
                    "broker": r.get("Broker", None),
                    "source_json": r
                })

        df = pd.DataFrame(rows)
        sql = self.create_fact_table_sql("fact_match_detail")
        return df, sql

class FactIncomeStatementLoader(FactLoader):

    def load(self):
        raw = []
        raw.extend(self.mongo.find_table("income_statement_yearly"))
        raw.extend(self.mongo.find_table("income_statement_quarterly"))

        rows = []
        for doc in raw:
            symbol = doc["symbol"]
            company_key = self.get_company_key(symbol)
            report_type_key = self.get_report_type_key(doc["report_type"])

            for r in doc["data"]:
                period_date_key = self.get_date_key(r["period"])

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

class FactBalanceSheetLoader(FactLoader):

    def load(self):
        raw = []
        raw.extend(self.mongo.find_table("balance_sheet_yearly"))
        raw.extend(self.mongo.find_table("balance_sheet_quarterly"))

        rows = []
        for doc in raw:
            symbol = doc["symbol"]
            company_key = self.get_company_key(symbol)
            report_type_key = self.get_report_type_key(doc["report_type"])

            for r in doc["data"]:
                period_date_key = self.get_date_key(r["period"])

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


class FactBusinessPlanLoader(FactLoader):
    """
    Load fact_business_plan from Mongo collection 'business_plan'.
    Grain: company × year
    Expected Mongo doc example:
      {
        "symbol": "AAA",
        "data": [
           {"Year": "2023", "Plan_revenue": 100000, "Pass_revenue": 95000, "Plan_profit": 5000, "Pass_profit": 4800},
           ...
        ],
        "update_time": "..."
      }
    """

    COLLECTION = "business_plan"
    FACT_NAME = "fact_business_plan"

    def load(self) -> (pd.DataFrame, str):
        try:
            raw_docs = list(self.mongo.find_table(self.COLLECTION))
        except Exception as e:
            logger.exception("Failed to read business_plan from mongo: %s", e)
            raw_docs = []

        rows = []
        for doc in raw_docs:
            symbol = doc.get("symbol")
            if not symbol:
                logger.warning("skip business_plan doc without symbol: %s", doc)
                continue

            company_key = self.get_company_key(symbol)
            # Expect doc["data"] to be list-of-rows; if DataFrame serialized, adapt accordingly
            data_list = doc.get("data") or []
            if isinstance(data_list, dict):
                # maybe stored as object, try to convert
                data_list = [data_list]

            for r in data_list:
                year_raw = r.get("Year") or r.get("year") or r.get("period")
                # Normalize year -> use dim_date key (e.g., year-end date 'YYYY-12-31')
                try:
                    year_int = int(str(year_raw)[:4])
                    period_date = datetime(year_int, 12, 31).date()
                    period_date_key = self.dim_repo.get_or_create(
                        period_date.isoformat(), "dim_date", self.table_creator
                    )
                except Exception:
                    # fallback to using raw string as natural key for dim_date
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
        # create SQL from schema (DATA_WAREHOUSE_SCHEMA)
        fact_schema = DATA_WAREHOUSE_SCHEMA["facts"][self.FACT_NAME]["columns"]
        sql = self.table_creator.generate_create_table_sql(self.FACT_NAME, {
            k: {"type": v.split()[0] if isinstance(v, str) else "TEXT", "constraints": ""} for k, v in fact_schema.items()
        })
        return df, sql


class FactFinancialMetricsLoader(FactLoader):
    """
    Load fact_financial_metrics from Mongo collection 'financial_info' (or 'financial_metrics').
    Expected mongo doc example:
      {
         "symbol": "AAA",
         "data": [
            {"period": "2023-12-31", "pe": 12.3, "roe": 0.15, "roa": 0.08, "debt_equity": 0.6, "market_cap": 1_000_000},
            ...
         ],
         "update_time": "..."
      }
    """

    COLLECTION = "financial_info"
    FACT_NAME = "fact_financial_metrics"

    def load(self) -> (pd.DataFrame, str):
        try:
            raw_docs = list(self.mongo.find_table(self.COLLECTION))
        except Exception as e:
            logger.exception("Failed to read financial_info from mongo: %s", e)
            raw_docs = []

        rows = []
        for doc in raw_docs:
            symbol = doc.get("symbol")
            if not symbol:
                logger.warning("skip financial_info doc without symbol: %s", doc)
                continue

            company_key = self.get_company_key(symbol)
            # raw metrics could be under doc["data"] (list) or doc["metrics"] (single)
            data_list = doc.get("data") or doc.get("metrics") or []
            if isinstance(data_list, dict):
                data_list = [data_list]

            for r in data_list:
                # period -> dim_date key
                period_raw = r.get("period") or r.get("report_date") or r.get("date")
                period_key = None
                if period_raw:
                    try:
                        # expect yyyy-mm-dd or yyyy
                        period_dt = pd.to_datetime(period_raw, errors="coerce")
                        if pd.isna(period_dt):
                            period_key = self.dim_repo.get_or_create(str(period_raw), "dim_date", self.table_creator)
                        else:
                            period_key = self.dim_repo.get_or_create(period_dt.date().isoformat(), "dim_date", self.table_creator)
                    except Exception:
                        period_key = self.dim_repo.get_or_create(str(period_raw), "dim_date", self.table_creator)
                else:
                    # if no period, use 'latest' natural key
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
        fact_schema = DATA_WAREHOUSE_SCHEMA["facts"][self.FACT_NAME]["columns"]
        sql = self.table_creator.generate_create_table_sql(self.FACT_NAME, {
            k: {"type": v.split()[0] if isinstance(v, str) else "TEXT", "constraints": ""} for k, v in fact_schema.items()
        })
        return df, sql