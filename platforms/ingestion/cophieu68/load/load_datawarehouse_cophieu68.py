# ...existing code...
from datetime import datetime
import logging
from typing import Optional, Dict, Any
import pandas as pd
from shared.utils.files.util_cophieu68 import TableCreator
from platforms.storage.datalake.mongodb.data_lake_storage import MongoStorageBackend
from platforms.ingestion.cophieu68.dto.load_models import BaseDoc
from platforms.ingestion.cophieu68.dto import transform_models
from platforms.ingestion.cophieu68.dto.load_models import (
    TradingDataDoc,
    MatchDetailsDoc,
    IncomeStatementDoc,
    BalanceSheetDoc,
    BusinessPlanDoc,
    FinancialInfoDoc
)
from platforms.storage.datawarehouse.postgresql.datawarehouse_storage import PostgreSQLWriter


class BaseLoader:
    def __init__(self, 
                 datalake_config: Dict[str, Any], 
                 table_creator: TableCreator, 
                 mongo_reader: MongoStorageBackend, 
                 datawarehouse_logger: Optional[logging.Logger] = None,
                 postgresql_client: Optional[PostgreSQLWriter] = None):
        self.datalake_config = datalake_config or {}
        self.table_creator = table_creator
        self.mongo = mongo_reader
        self.postgresql_client = postgresql_client
        self.schema_dw = transform_models.DATA_WAREHOUSE_SCHEMA or {}
        self.datawarehouse_logger = datawarehouse_logger
        self.schema_name = self.schema_dw.get("schema_name", "public")
        # map of collection names from YAML (storage.mongodb.collections.*)
        self.collection_map = (
            self.datalake_config.get("storage", {})
            .get("mongodb", {})
            .get("collections", {})
            or {}
        )

    def _get_collection(self, key: str) -> str:
        return self.collection_map.get(key, key)

    def _create_table_sql(self, name: str) -> str:
        # determine if dim or fact
        if name.startswith("dim"):
            sections = self.schema_dw.get("dimensions", {})
        else:
            sections = self.schema_dw.get("facts", {})
        cols = sections.get(name, {}).get("columns", {})
        norm = {}
        for col, meta in cols.items():
            if isinstance(meta, dict) and "type" in meta:
                norm[col] = meta
            else:
                parts = str(meta).split(None, 1)
                typ = parts[0]
                cons = parts[1] if len(parts) > 1 else ""
                norm[col] = {"type": typ, "constraints": cons}
        schema_name = self.schema_dw.get("schema_name", "public")
        return self.table_creator.generate_create_table_sql(name, norm, schema_name)



class DimLoader(BaseLoader):
    def __init__(self, 
                 datalake_config: Dict[str, Any], 
                 table_creator: TableCreator, 
                 mongo_reader: MongoStorageBackend, 
                 datawarehouse_logger: Optional[logging.Logger] = None,
                 postgresql_client: Optional[PostgreSQLWriter] = None):
        super().__init__(datalake_config, table_creator, mongo_reader, datawarehouse_logger, postgresql_client)
        self.dim_postgresql_client = postgresql_client


class FactLoader(BaseLoader):
    def __init__(
        self,
        datalake_config: dict,
        table_creator: TableCreator,
        mongo_reader: MongoStorageBackend,
        dim_repo,
        datawarehouse_logger: Optional[logging.Logger] = None,
        postgres_client: Optional[PostgreSQLWriter] = None,
    ):
        super().__init__(datalake_config, table_creator, mongo_reader, datawarehouse_logger, postgres_client)
        self.fact_postgresql_client = postgres_client
        self.dim_repo = dim_repo

    def _get_fact_name(self, hint: str, fallback: str) -> str:
        # if explicit hint exists in schema, return it; else try to find by substring; fallback otherwise
        facts = self.schema_dw.get("facts", {}) or {}
        if hint in facts:
            return hint
        # find key that contains hint fragment (e.g., 'match' -> 'fact_match_detail')
        hint_fragment = hint.replace("fact_", "").replace("fact", "")
        found = next((k for k in facts.keys() if hint_fragment and hint_fragment in k), None)
        if found:
            return found
        return fallback

    def create_fact_table_sql(self, fact_name: str) -> str:
        table_info = self.schema_dw["facts"][fact_name]["columns"]
        norm = {}
        for col, meta in table_info.items():
            if isinstance(meta, dict) and "type" in meta:
                norm[col] = meta
            else:
                parts = str(meta).split(None, 1)
                typ = parts[0]
                cons = parts[1] if len(parts) > 1 else ""
                norm[col] = {"type": typ, "constraints": cons}
        return self.table_creator.generate_create_table_sql(fact_name, norm)

    def get_company_key(self, symbol: str) -> str:
        return self.dim_repo.get_or_create(symbol, "dim_company", self.table_creator)
    
    def get_industry_key(self, industry: str) -> str:
        return self.dim_repo.get_or_create(industry, "dim_industry", self.table_creator)

    def get_report_type_key(self, report_type: str) -> str:
        return self.dim_repo.get_or_create(report_type, "dim_report_type", self.table_creator)

    def get_date_key(self, date_str: str) -> str:
        return self.dim_repo.get_or_create(date_str, "dim_date", self.table_creator)

# dim_market_type
class DimMarketTypeLoader(DimLoader):
    def __init__(self, datalake_config: Dict[str, Any], 
                 table_creator: TableCreator, 
                 mongo_reader: MongoStorageBackend, 
                 datawarehouse_logger: Optional[logging.Logger] = None,
                 postgresql_client: Optional[PostgreSQLWriter] = None):
        super().__init__(datalake_config, table_creator, mongo_reader, datawarehouse_logger, postgresql_client)
        # YAML key is 'list_stock' per config
        self.collection_name = self._get_collection("list_stock")
        self.dim_name = "DIM_MARKET_TYPE"

    def load(self):
        raw = self.mongo.find_table(self.collection_name)
        rows = []
        from platforms.ingestion.cophieu68.dto.load_models import index_information
        for document in raw["data"]:
            b = BaseDoc.from_extract(document)
            market_type = document.get("market_type") if isinstance(document, dict) else b.symbol
            rows.append({
                "market_key": market_type.lower() if market_type else None,
                "market_type": market_type,
                "market_name": index_information.get(market_type),
                "update_time": document.get("update_time") or datetime.utcnow().isoformat(),
                "created_time": datetime.utcnow().isoformat()
            })
        df = pd.DataFrame(rows)
        sql = self._create_table_sql(self.dim_name)
        return df, sql


# dim_industry
class DimIndustryLoader(DimLoader):
    def __init__(self, datalake_config, table_creator, mongo_reader, datawarehouse_logger=None, postgresql_client=None):
        super().__init__(datalake_config, table_creator, mongo_reader, datawarehouse_logger, postgresql_client)
        self.collection_name = self._get_collection("industry_list")
        self.dim_name = "DIM_INDUSTRY"

    def load(self):
        raw = self.mongo.find_table(self.collection_name)
        rows = []
        for doc in raw.get("data"):
            industry_metric = doc.get("industry_metric")
            existing_record = self.postgresql_client.query(
                f"SELECT * FROM {self.schema_name}.{self.dim_name} WHERE industry_metric = %s AND is_current = TRUE",
                (industry_metric,)
            )

            if existing_record:
                # Check if there are changes
                if existing_record["industry_code"] != doc.get("industry_code") or \
                   existing_record["industry_craw_url"] != doc.get("industry_craw_url"):
                    # Update the existing record's end_date and is_current
                    self.postgresql_client.execute(
                        f"UPDATE {self.schema_name}.{self.dim_name} SET end_date = %s, is_current = FALSE WHERE industry_metric = %s AND is_current = TRUE",
                        (datetime.utcnow().isoformat(), industry_metric)
                    )

            # Insert the new record
            rows.append({
                "industry_metric": industry_metric,
                "industry_code": doc.get("industry_code"),
                "industry_code_replace": doc.get("industry_code_replace"),
                "industry_craw_url": doc.get("industry_craw_url"),
                "effective_date": datetime.utcnow().isoformat(),
                "end_date": None,
                "is_current": True,
                "update_time": datetime.utcnow().isoformat(),
                "created_time": datetime.utcnow().isoformat(),
            })

        df = pd.DataFrame(rows)
        sql = self._create_table_sql(self.dim_name)
        return df, sql


# dim_company (SCD2 simplified snapshot)
class DimCompanyLoader(DimLoader):
    def __init__(self, datalake_config, table_creator, mongo_reader, datawarehouse_logger=None, postgresql_client=None):
        super().__init__(datalake_config, table_creator, mongo_reader, datawarehouse_logger, postgresql_client)
        # YAML key for company profiles: use 'stock_info' from config
        self.collection_name = self._get_collection("company_profile")
        self.dim_name = "dim_company"

    def load(self):
        raw = self.mongo.find_table(self.collection_name)
        rows = []
        for doc in raw.get("data"):
            company_key = doc.get("symbol")
            existing_record = self.postgresql_client.query(
                f"SELECT * FROM {self.schema_name}.{self.dim_name} WHERE company_key = %s AND is_current = TRUE",
                (company_key,)
            )

            if existing_record:
                # Check if there are changes
                if existing_record["company_name"] != doc.get("company_name") or \
                   existing_record["full_name"] != doc.get("full_name"):
                    # Update the existing record's end_date and is_current
                    self.postgresql_client.execute(
                        f"UPDATE {self.schema_name}.{self.dim_name} SET end_date = %s, is_current = FALSE WHERE company_key = %s AND is_current = TRUE",
                        (datetime.utcnow().isoformat(), company_key)
                    )

            # Insert the new record
            rows.append({
                "company_key": company_key,
                "symbol": doc.get("symbol"),
                "company_name": doc.get("company_name"),
                "full_name": doc.get("full_name"),
                "effective_date": datetime.utcnow().isoformat(),
                "end_date": None,
                "is_current": True,
                "update_time": datetime.utcnow().isoformat(),
                "created_time": datetime.utcnow().isoformat(),
            })

        df = pd.DataFrame(rows)
        sql = self._create_table_sql(self.dim_name)
        return df, sql


# dim_report_type
class DimReportTypeLoader(DimLoader):
    def __init__(self, datalake_config, table_creator, mongo_reader, datawarehouse_logger=None, postgresql_client=None):
        super().__init__(datalake_config, table_creator, mongo_reader, datawarehouse_logger, postgresql_client)
        # report types are static; no collection required
        self.dim_name = "dim_report_type"

    def load(self):
        mapping = [
            {"report_type_key": "Y", "report_type_code": "Y", "description": "Yearly"},
            {"report_type_key": "Q", "report_type_code": "Q", "description": "Quarterly"},
        ]
        df = pd.DataFrame(mapping)
        sql = self._create_table_sql(self.dim_name)
        return df, sql




# FactTradeLoader using TradingDataDoc
class FactTradeLoader(FactLoader):
    def __init__(self, datalake_config, datawarehouse_logger, postgres_client, dim_repo, mongo_reader):
        table_creator = TableCreator(machine_id=1, character_specific=None)
        super().__init__(datalake_config, table_creator, mongo_reader, dim_repo, datawarehouse_logger, postgres_client)
        self.collection_name = self._get_collection("trading_data")
        self.fact_name = self._get_fact_name("fact_trade", "fact_trade")

    def load(self):
        raw_docs = self.mongo.find_table(self.collection_name)
        rows = []
        for doc in raw_docs:
            tdoc = TradingDataDoc.from_extract(doc)
            if not tdoc.symbol:
                self.datawarehouse_logger.warning("trading_data doc without symbol: %s", doc)
                continue
            company_key = self.get_company_key(tdoc.symbol)
            for r in tdoc.to_fact_rows():
                trade_dt = r.get("trade_datetime")
                rows.append({
                    "trade_key": self.table_creator.get_id(),
                    "trade_date": trade_dt,
                    "company_key": company_key,
                    "close_price": r.get("price"),
                    "open_price": r.get("open_price"),
                    "high_price": r.get("high_price"),
                    "low_price": r.get("low_price"),
                    "volume": r.get("volume"),
                    "foreign_buy": r.get("foreign_buy"),
                    "foreign_sell": r.get("foreign_sell"),
                    "foreign_net_value": r.get("foreign_net_value"),
                    "update_time": r.get("update_time") or datetime.utcnow().isoformat(),
                    
                })
        df = pd.DataFrame(rows)
        sql = self.create_fact_table_sql(self.fact_name)
        return df, sql


# FactMatchDetailLoader
class FactMatchDetailLoader(FactLoader):
    def __init__(self, datalake_config, datawarehouse_logger, postgres_client, dim_repo, mongo_reader):
        table_creator = TableCreator(machine_id=1, character_specific=None)
        super().__init__(datalake_config, table_creator, mongo_reader, dim_repo, datawarehouse_logger, postgres_client)
        self.collection_name = self._get_collection("match_details")
        self.fact_name = self._get_fact_name("fact_match_detail", "fact_match_detail")

    def load(self):
        raw = list(self.mongo.find_table(self.collection_name) or [])
        rows = []
        for doc in raw:
            mdoc = MatchDetailsDoc.from_extract(doc)
            if not mdoc.symbol:
                self.datawarehouse_logger.warning("match_details doc without symbol: %s", doc)
                continue
            company_key = self.get_company_key(mdoc.symbol)
            for r in mdoc.to_fact_rows():
                rows.append({
                    "match_key": self.table_creator.get_id(),
                    "company_key": company_key,
                    "match_datetime": r.get("match_datetime"),
                    "price": r.get("price"),
                    "volume": r.get("volume"),
                    "fluctuation_range": r.get("fluctuation_range"),
                    "accum_volume": r.get("accum_volume"),
                    "update_time": r.get("update_time")
                   
                })
        df = pd.DataFrame(rows)
        sql = self.create_fact_table_sql(self.fact_name)
        return df, sql


# FactIncomeStatementLoader (IncomeStatementDoc)
class FactIncomeStatementLoader(FactLoader):
    def __init__(self, datalake_config, datawarehouse_logger, postgres_client, dim_repo, mongo_reader):
        table_creator = TableCreator(machine_id=1, character_specific=None)
        super().__init__(datalake_config, table_creator, mongo_reader, dim_repo, datawarehouse_logger, postgres_client)
        # can map both yearly/quarterly collections
        self.collection_income_statement_yearly = self._get_collection("income_statement_yearly")
        self.collection_income_statement_quarterly = self._get_collection("income_statement_quarterly")
        self.fact_income_statement_quarterly = self._get_fact_name("fact_income_statement_quarterly", "fact_income_statement_quarterly")
        self.fact_income_statement_yearly = self._get_fact_name("fact_income_statement_yearly", "fact_income_statement_yearly")

    def load_fact_income_statement_quarterly(self):
        raw = self.mongo.find_table(self.collection_income_statement_quarterly)
        rows = []
        for doc in raw:
            idoc = IncomeStatementDoc.from_extract(doc)
            if not idoc.symbol:
                self.datawarehouse_logger.warning("income_statement doc without symbol: %s", doc)
                continue
            company_key = self.get_company_key(idoc.symbol)
            report_type_key = self.get_report_type_key(report_type = "quarterly")
            period_key = self.get_date_key(r.get("period"))
            rows.append({
                "income_key": self.table_creator.get_id(),
                "company_key": company_key,
                "report_type_key": report_type_key,
                "period_date_key": period_key,
                "revenue": r.get("revenue"),
                "operating_profit": r.get("operating_profit"),
                "net_income": r.get("net_income"),
                "eps": r.get("eps")
                
            })
        df = pd.DataFrame(rows)
        sql = self.create_fact_table_sql(self.fact_income_statement_quarterly)
        return df, sql

    def load_fact_income_statement_yearly(self):
        raw = self.mongo.find_table(self.collection_income_statement_yearly)
        rows = []
        for doc in raw:
            idoc = IncomeStatementDoc.from_extract(doc)
            if not idoc.symbol:
                self.datawarehouse_logger.warning("income_statement doc without symbol: %s", doc)
                continue
            company_key = self.get_company_key(idoc.symbol)
            report_type_key = self.get_report_type_key(report_type = "yearly")
            period_key = self.get_date_key(r.get("period"))
            rows.append({
                "income_key": self.table_creator.get_id(),
                "company_key": company_key,
                "report_type_key": report_type_key,
                "period_date_key": period_key,
                "revenue": r.get("revenue"),
                "operating_profit": r.get("operating_profit"),
                "net_income": r.get("net_income"),
                "eps": r.get("eps")
                
            })
        df = pd.DataFrame(rows)
        sql = self.create_fact_table_sql(self.fact_income_statement_yearly)
        return df, sql

# FactBalanceSheetLoader (BalanceSheetDoc)
class FactBalanceSheetLoader(FactLoader):
    def __init__(self, datalake_config, datawarehouse_logger, postgres_client, dim_repo, mongo_reader):
        table_creator = TableCreator(machine_id=1, character_specific=None)
        super().__init__(datalake_config, table_creator, mongo_reader, dim_repo, datawarehouse_logger, postgres_client)
        # can map both yearly/quarterly collections
        self.collection_balance_sheet_yearly = self._get_collection("balance_sheet_yearly")
        self.collection_balance_sheet_quarterly = self._get_collection("balance_sheet_quarterly")
        self.fact_balance_sheet_quarterly = self._get_fact_name("fact_balance_sheet_quarterly", "fact_balance_sheet_quarterly")
        self.fact_balance_sheet_yearly = self._get_fact_name("fact_balance_sheet_yearly", "fact_balance_sheet_yearly")

    def load_fact_balance_sheet_quarterly(self):
        raw = self.mongo.find_table(self.collection_balance_sheet_quarterly)
        rows = []
        for doc in raw:
            bdoc = BalanceSheetDoc.from_extract(doc)
            if not bdoc.symbol:
                self.datawarehouse_logger.warning("balance_sheet doc without symbol: %s", doc)
                continue
            company_key = self.get_company_key(bdoc.symbol)
            report_type_key = self.get_report_type_key(report_type = "quarterly")
            for r in bdoc.to_fact_rows():
                period_key = self.get_date_key(r.get("period"))
                rows.append({
                    "bs_key": self.table_creator.get_id(),
                    "company_key": company_key,
                    "report_type_key": report_type_key,
                    "period_date_key": period_key,
                    "total_assets": r.get("total_assets"),
                    "total_liabilities": r.get("total_liabilities"),
                    "shareholder_equity": r.get("shareholder_equity"),
                    "cash": r.get("cash"),
                    "inventory": r.get("inventory")
                })
        df = pd.DataFrame(rows)
        sql = self.create_fact_table_sql(self.fact_balance_sheet_quarterly)
        return df, sql
    
    def load_fact_balance_sheet_yearly(self):
        raw = self.mongo.find_table(self.collection_balance_sheet_yearly)
        rows = []
        for doc in raw:
            bdoc = BalanceSheetDoc.from_extract(doc)
            if not bdoc.symbol:
                self.datawarehouse_logger.warning("balance_sheet doc without symbol: %s", doc)
                continue
            company_key = self.get_company_key(bdoc.symbol)
            report_type_key = self.get_report_type_key(report_type = "yearly")
            for r in bdoc.to_fact_rows():
                period_key = self.get_date_key(r.get("period"))
                rows.append({
                    "bs_key": self.table_creator.get_id(),
                    "company_key": company_key,
                    "report_type_key": report_type_key,
                    "period_date_key": period_key,
                    "total_assets": r.get("total_assets"),
                    "total_liabilities": r.get("total_liabilities"),
                    "shareholder_equity": r.get("shareholder_equity"),
                    "cash": r.get("cash"),
                    "inventory": r.get("inventory")
                })
        df = pd.DataFrame(rows)
        sql = self.create_fact_table_sql(self.fact_balance_sheet_yearly)
        return df, sql


# FactBusinessPlanLoader
class FactBusinessPlanLoader(FactLoader):
    def __init__(self, datalake_config, datawarehouse_logger, postgres_client, dim_repo, mongo_reader):
        table_creator = TableCreator(machine_id=1, character_specific=None)
        super().__init__(datalake_config, table_creator, mongo_reader, dim_repo, datawarehouse_logger, postgres_client)
        self.collection_name = self._get_collection("business_plan")
        self.fact_name = self._get_fact_name("fact_business_plan", "fact_business_plan")

    def load(self):
        raw = self.mongo.find_table(self.collection_name)
        rows = []
        for doc in raw:
            pdoc = BusinessPlanDoc.from_extract(doc)
            if not pdoc.symbol:
                self.datawarehouse_logger.warning("business_plan doc without symbol: %s", doc)
                continue
            company_key = self.get_company_key(pdoc.symbol)
            for r in pdoc.to_fact_rows():
                year = r.get("year")
                try:
                    period_key = self.get_date_key(str(year))
                except Exception:
                    period_key = self.get_date_key("latest")
                rows.append({
                    "plan_key": self.table_creator.get_id(),
                    "company_key": company_key,
                    "year_key": period_key,
                    "target_revenue": r.get("target_revenue"),
                    "target_profit": r.get("target_profit"),
                    "capex_plan": r.get("capex_plan"),
                    "source_json": r.get("source_json")
                })
        df = pd.DataFrame(rows)
        sql = self.create_fact_table_sql(self.fact_name)
        return df, sql

class FactFinancialMetricsLoader(FactLoader):
    def __init__(self, datalake_config, datawarehouse_logger, postgres_client, dim_repo, mongo_reader):
        table_creator = TableCreator(machine_id=1, character_specific=None)
        super().__init__(datalake_config, table_creator, mongo_reader, dim_repo, datawarehouse_logger, postgres_client)
        self.collection_name = self._get_collection("financial_info")
        self.fact_name = self._get_fact_name("fact_financial_metrics", "fact_financial_metrics")
    
    def load(self):
        raw = self.mongo.find_table(self.collection_name)
        rows = []
        for doc in raw:
            fdoc = FinancialInfoDoc.from_extract(doc)
            if not fdoc.symbol:
                self.datawarehouse_logger.warning("financial_metrics doc without symbol: %s", doc)
                continue
            company_key = self.get_company_key(fdoc.symbol)
            for r in fdoc.to_fact_rows():
                # period_key = self.get_date_key(r.get("period"))
                rows.append({
                    "financial_ratio_key": self.table_creator.get_id(),
                    "company_key":company_key,
                    "reference_price":  r.get("reference_price"),
                    "company_name": r.get("company_name"),
                    "open_price": r.get("open_price"),
                    "high_price": r.get("high_price"),
                    "low_price": r.get("low_price"),
                    "volume": r.get("volume"),
                    "book_value": r.get("book_value"),
                    "earning_per_share(EPS)": r.get("eps"),
                    "price_on_earning(P/E)": r.get("pe"),
                    "price_on_book_value(P/B)": r.get("pb"),
                    "return_on_equity(ROE)": r.get("roe"),
                    "return_on_assets(ROA)": r.get("roa"),
                    "beta": r.get("beta"),
                    "market_cap": r.get("market_cap"),
                    "listed_volume": r.get("listed_volume"),
                    "average_volume_52_weeks": r.get("avg_volume_52w"),
                    "high_low_52_weeks": r.get("high_low_52w"),
                    "debt": r.get("debt"),
                    "equity": r.get("equity"),
                    "debt_to_equity": r.get("debt_to_equity"),
                    "equity_to_assets": r.get("equity_to_assets"),
                    "cash": r.get("cash"),
                    "eps_power": r.get("eps_power"),
                    "roe_power": r.get("roe_power"),
                    "invest_efficiency": r.get("invest_efficiency"),
                    "pb_power": r.get("pb_power"),
                    "price_growth_power": r.get("price_growth_power"),
                    "update_time": r.get("update_time")
                })
        df = pd.DataFrame(rows)
        sql = self.create_fact_table_sql(self.fact_name)
        return df, sql

class FactIndustryLoader(FactLoader):
    def __init__(self, datalake_config, datawarehouse_logger, postgres_client, dim_repo, mongo_reader):
        table_creator = TableCreator(machine_id=1, character_specific=None)
        super().__init__(datalake_config, table_creator, mongo_reader, dim_repo, datawarehouse_logger, postgres_client)
        self.collection_name = self._get_collection("industry_list")
        self.fact_name = self._get_fact_name("fact_industry_summary", "fact_industry_summary")
    
    def load(self):
        raw = self.mongo.find_table(self.collection_name)
        data = raw.get("data")
        for documentation in data:
           industry_metric = documentation.get("industry_metric")
           industry_key = get_industry_key = self.get_industry_key(industry_metric)
           row = []

           for info in documentation.get("data"):
                for infor_keys in list(info.keys()):
                    row.append({
                        "industry_metric": industry_metric,
                        "industry_code": str(infor_keys).split("_")[1],
                        "industry_code_replace": str(infor_keys).split("_")[2],
                        "industry_craw_url": str(infor_keys).split("_")[3],
                        "update_time": info.get("update_time") or datetime.utcnow().isoformat(),
                        "created_time": info.get("update_time") or datetime.utcnow().isoformat(),
                    })

