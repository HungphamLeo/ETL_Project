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
    BusinessPlanDoc
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
        data = raw.get("data")
        for documentation in data:
           industry_metric = documentation.get("industry_metric")
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
        df = pd.DataFrame(row)
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
            
            profile = doc.get("profile_json")
            # some payloads store profile under 'profile' or raw data under 'data'
            if not profile and isinstance(doc, dict):
                profile = doc.get("profile") or doc.get("raw") or doc.get("data") or {}
            symbol = profile.get("symbol")

            rows.append({
                "company_key": symbol,
                "symbol": symbol,
                "company_name": profile.get("company_name") or None,
                "full_name": profile.get("full_name") or None,
                "english_name": profile.get("english_name") or None,
                "short_name": profile.get("short_name") or None,
                "address": profile.get("address") or None,
                "phone": profile.get("phone") or None,
                "fax": profile.get("fax") or None,
                "website": profile.get("website") or None,
                "email": profile.get("email") or None,
                "established_date": profile.get("established_date"),
                "listed_date": profile.get("listed_date"),
                "chartered_capital": profile.get("chartered_capital") or None,
                "business_license": profile.get("business_license") or None,
                "tax_code": profile.get("tax_code") or None,
                # "market_key": profile.get("market_type") or None,
                # "industry_key": profile.get("industry_code") or None,
                "effective_from": profile.get("listed_date") or None,
                "effective_to": None,
                "is_current": True,
                "created_time": datetime.now()
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
        raw_docs = list(self.mongo.find_table(self.collection_name) or [])
        rows = []
        for doc in raw_docs:
            tdoc = TradingDataDoc.from_extract(doc)
            if not tdoc.symbol:
                self.datawarehouse_logger.warning("trading_data doc without symbol: %s", doc)
                continue
            company_key = self.get_company_key(tdoc.symbol)
            for r in tdoc.to_fact_rows():
                trade_dt = r.get("trade_datetime")
                trade_date_key = self.get_date_key(trade_dt) if trade_dt else self.get_date_key("latest")
                rows.append({
                    "trade_key": self.table_creator.get_id(),
                    "trade_datetime": trade_dt,
                    "trade_date_key": trade_date_key,
                    "company_key": company_key,
                    "price": r.get("price"),
                    "volume": r.get("volume"),
                    "value": r.get("value"),
                    "side": r.get("side") or "NA",
                    "source_json": r.get("source_json")
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
                    "broker": r.get("broker"),
                    "source_json": r.get("source_json")
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
        self.collection_names = [
            self._get_collection("income_statement_yearly"),
            self._get_collection("income_statement_quarterly"),
        ]
        self.fact_name = self._get_fact_name("fact_income_statement", "fact_income_statement")

    def load(self):
        raw = []
        for c in self.collection_names:
            raw.extend(list(self.mongo.find_table(c) or []))
        rows = []
        for doc in raw:
            idoc = IncomeStatementDoc.from_extract(doc)
            if not idoc.symbol:
                self.datawarehouse_logger.warning("income_statement doc without symbol: %s", doc)
                continue
            company_key = self.get_company_key(idoc.symbol)
            report_type_key = self.get_report_type_key(idoc.report_type or doc.get("report_type"))
            for r in idoc.to_fact_rows():
                period_key = self.get_date_key(r.get("period"))
                rows.append({
                    "income_key": self.table_creator.get_id(),
                    "company_key": company_key,
                    "report_type_key": report_type_key,
                    "period_date_key": period_key,
                    "revenue": r.get("revenue"),
                    "operating_profit": r.get("operating_profit"),
                    "net_income": r.get("net_income"),
                    "eps": r.get("eps"),
                    "source_json": r.get("source_json")
                })
        df = pd.DataFrame(rows)
        sql = self.create_fact_table_sql(self.fact_name)
        return df, sql


# FactBalanceSheetLoader (BalanceSheetDoc)
class FactBalanceSheetLoader(FactLoader):
    def __init__(self, datalake_config, datawarehouse_logger, postgres_client, dim_repo, mongo_reader):
        table_creator = TableCreator(machine_id=1, character_specific=None)
        super().__init__(datalake_config, table_creator, mongo_reader, dim_repo, datawarehouse_logger, postgres_client)
        self.collection_names = [
            self._get_collection("balance_sheet_yearly"),
            self._get_collection("balance_sheet_quarterly"),
        ]
        self.fact_name = self._get_fact_name("fact_balance_sheet", "fact_balance_sheet")

    def load(self):
        raw = []
        for c in self.collection_names:
            raw.extend(list(self.mongo.find_table(c) or []))
        rows = []
        for doc in raw:
            bdoc = BalanceSheetDoc.from_extract(doc)
            if not bdoc.symbol:
                self.datawarehouse_logger.warning("balance_sheet doc without symbol: %s", doc)
                continue
            company_key = self.get_company_key(bdoc.symbol)
            report_type_key = self.get_report_type_key(bdoc.report_type or doc.get("report_type"))
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
                    "inventory": r.get("inventory"),
                    "source_json": r.get("source_json")
                })
        df = pd.DataFrame(rows)
        sql = self.create_fact_table_sql(self.fact_name)
        return df, sql


# FactBusinessPlanLoader
class FactBusinessPlanLoader(FactLoader):
    def __init__(self, datalake_config, datawarehouse_logger, postgres_client, dim_repo, mongo_reader):
        table_creator = TableCreator(machine_id=1, character_specific=None)
        super().__init__(datalake_config, table_creator, mongo_reader, dim_repo, datawarehouse_logger, postgres_client)
        self.collection_name = self._get_collection("business_plan")
        self.fact_name = self._get_fact_name("fact_business_plan", "fact_business_plan")

    def load(self):
        raw = list(self.mongo.find_table(self.collection_name) or [])
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
