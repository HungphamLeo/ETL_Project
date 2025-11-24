# ...existing code...
import pandas as pd
from datetime import datetime
from shared.utils.util_cophieu68 import TableCreator
from shared.common_models.cophieu68_model import transform_models
from shared.common_models.cophieu68_model.load_models import (
    TradingDataDoc,
    MatchDetailsDoc,
    IncomeStatementDoc,
    BalanceSheetDoc,
    BusinessPlanDoc
)
from platforms.storage.datalake.mongodb.data_lake_storage import MongoStorageBackend  # assume exists
import logging
class FactLoader:
    def __init__(
        self,
        datalake_config: dict,
        datawarehouse_logger,
        postgres_client,
        dim_repo,
        table_creator: TableCreator,
        mongo_reader: MongoStorageBackend,
    ):
        self.datalake_config = datalake_config or {}
        self.schema_dw = transform_models.DATA_WAREHOUSE_SCHEMA
        self.table_creator = table_creator
        self.mongo = mongo_reader
        self.dim_repo = dim_repo
        self.datawarehouse_logger = datawarehouse_logger
        self.postgres_client = postgres_client

        # map of collection names from YAML (keys like 'trading_data', 'match_details', ...)
        self.collection_map = (
            self.datalake_config.get("storage", {})
            .get("mongodb", {})
            .get("collections", {})
            or {}
        )

    def _get_collection(self, key: str) -> str:
        # return mapped collection name or fallback to key itself
        return self.collection_map.get(key, key)

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


# FactTradeLoader using TradingDataDoc
class FactTradeLoader(FactLoader):
    def __init__(self, datalake_config, datawarehouse_logger, postgres_client, dim_repo, mongo_reader):
        table_creator = TableCreator(machine_id=1, character_specific=None)
        super().__init__(datalake_config, datawarehouse_logger, postgres_client, dim_repo, table_creator, mongo_reader)
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
        super().__init__(datalake_config, datawarehouse_logger, postgres_client, dim_repo, table_creator, mongo_reader)
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
        super().__init__(datalake_config, datawarehouse_logger, postgres_client, dim_repo, table_creator, mongo_reader)
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
        super().__init__(datalake_config, datawarehouse_logger, postgres_client, dim_repo, table_creator, mongo_reader)
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
        super().__init__(datalake_config, datawarehouse_logger, postgres_client, dim_repo, table_creator, mongo_reader)
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
# ...existing code...