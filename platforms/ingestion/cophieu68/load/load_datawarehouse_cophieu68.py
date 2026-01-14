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
    FinancialInfoDoc,
    Pattern_IncomeStatementStandardLoadToDW,
    Pattern_BalanceSheetStandardLoadToDW
)
from platforms.storage.datawarehouse.postgresql.datawarehouse_storage import PostgreSQLWriter


class BaseLoader:
    def __init__(self, 
                 datalake_config: Dict[str, Any], 
                 mongo_reader: MongoStorageBackend, 
                 datawarehouse_logger: Optional[logging.Logger] = None,
                 postgresql_client: Optional[PostgreSQLWriter] = None):
        self.datalake_config = datalake_config or {}
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

    def _create_table_sql(self, name: str, table_creator:TableCreator) -> str:
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
        return table_creator.generate_create_table_sql(name, norm, schema_name)



class DimLoader(BaseLoader):
    def __init__(self, 
                 datalake_config: Dict[str, Any], 
                 mongo_reader: MongoStorageBackend, 
                 datawarehouse_logger: Optional[logging.Logger] = None,
                 postgresql_client: Optional[PostgreSQLWriter] = None):
        super().__init__(datalake_config, mongo_reader, datawarehouse_logger, postgresql_client)
        self.dim_postgresql_client = postgresql_client
    

class FactLoader(BaseLoader):
    def __init__(
        self,
        datalake_config: dict,
        mongo_reader: MongoStorageBackend,
        dim_repo,
        datawarehouse_logger: Optional[logging.Logger] = None,
        postgres_client: Optional[PostgreSQLWriter] = None,
    ):
        super().__init__(datalake_config, mongo_reader, datawarehouse_logger, postgres_client)
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

    def create_fact_table_sql(self, fact_name: str, table_creator: TableCreator) -> str:
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
        schema_name = self.schema_dw.get("schema_name", "public")
        return table_creator.generate_create_table_sql(fact_name, norm, schema_name)

    def get_company_key(self, symbol: str, table_creator: TableCreator) -> str:
        return self.dim_repo.get_or_create(symbol, "dim_company", table_creator)
    
    def get_market_key(self, market_type: str, table_creator:TableCreator) ->str:
        return self.dim_repo.get_or_create(market_type, "dim_market_type", table_creator)
    def get_industry_key(self, industry: str, table_creator:TableCreator) -> str:
        return self.dim_repo.get_or_create(industry, "dim_industry", table_creator)

    def get_report_type_key(self, report_type: str, table_creator:TableCreator) -> str:
        return self.dim_repo.get_or_create(report_type, "dim_report_type", table_creator)

    def get_date_key(self, date_str: str, table_creator:TableCreator) -> str:
        return self.dim_repo.get_or_create(date_str, "dim_date", table_creator)

# dim_market_type
class DimMarketTypeLoader(DimLoader):
    def __init__(self, datalake_config: Dict[str, Any], 
                 mongo_reader: MongoStorageBackend, 
                 datawarehouse_logger: Optional[logging.Logger] = None,
                 postgresql_client: Optional[PostgreSQLWriter] = None):
       
        super().__init__(datalake_config, mongo_reader, datawarehouse_logger, postgresql_client)
        # YAML key is 'list_stock' per config
        self.collection_name = self._get_collection("list_stock")
        self.dim_name = "dim_market_type"
        self.table_creator = TableCreator(machine_id=1, character_specific=self.dim_name)
        
        

    def load(self):
        raw = self.mongo.find_table(self.collection_name)
        rows = []
        from platforms.ingestion.cophieu68.dto.load_models import index_information
        for document in raw["data"]:
            b = BaseDoc.from_extract(document)
            market_type = document.get("market_type") if isinstance(document, dict) else b.symbol
            rows.append({
                "market_key": self.table_creator.get_id(),
                "market_type": market_type,
                "market_name": index_information.get(market_type),
                "update_time": document.get("update_time") or datetime.utcnow().isoformat(),
                "created_time": datetime.utcnow().isoformat()
            })
        df = pd.DataFrame(rows)
        sql = self._create_table_sql(self.dim_name, self.table_creator)
        df.drop_duplicates()
        return df, sql


# dim_industry
class DimIndustryLoader(DimLoader):
    def __init__(self, datalake_config, mongo_reader, datawarehouse_logger=None, postgresql_client=None):
        super().__init__(datalake_config, mongo_reader, datawarehouse_logger, postgresql_client)
        self.collection_name = self._get_collection("industry_list")
        self.dim_name = "dim_industry"
        self.table_creator = TableCreator(machine_id=1, character_specific=self.dim_name)

    def load(self):
        raw = self.mongo.find_table(self.collection_name)
        rows = []
        for doc in raw.get("data"):
            industry_metric = doc.get("industry_metric")
            
            
            for info in doc.get("data"):
                for info_keys in list(info.keys()):
                    rows.append({
                        "industry_sk": self.table_creator.get_id(),
                        "industry_metric": industry_metric,
                        "industry_code": str(info_keys).split("_")[1],
                        "industry_code_replace": str(info_keys).split("_")[2],
                        "industry_craw_url": str(info_keys).split("_")[3],
                        "effective_date": datetime.utcnow().isoformat(),
                        "end_date": None,
                        "is_current": True,
                        "update_time": info.get("update_time") or datetime.utcnow().isoformat(),
                        "created_time": info.get("update_time") or datetime.utcnow().isoformat()
                    })
        df = pd.DataFrame(rows)
        sql = self._create_table_sql(self.dim_name, self.table_creator)
        df.drop_duplicates()
        return df, sql

    

# dim_company (SCD2 simplified snapshot)
class DimCompanyLoader(DimLoader):
    def __init__(self, datalake_config, mongo_reader, datawarehouse_logger=None, postgresql_client=None):
        super().__init__(datalake_config, mongo_reader, datawarehouse_logger, postgresql_client)
        # YAML key for company profiles: use 'stock_info' from config
        self.collection_name = self._get_collection("company_profile")
        self.dim_name = "dim_company"
        self.table_creator = TableCreator(machine_id=1, character_specific=self.dim_name)

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
                "company_key": self.table_creator.get_id(),
                "symbol": symbol,
                "company_name": profile.get("full_name") or profile.get("company_name") or None,
                "market_key": self.get_market_key(profile.get("market_type")),
                "industry_sk": self.get_industry_sk(profile.get("industry_code")),
                "full_name": profile.get("full_name") or None,
                "english_name": profile.get("english_name") or None,
                "short_name": profile.get("short_name") or None,
                "address": profile.get("address") or None,
                "phone": profile.get("phone") or None,
                "fax": profile.get("fax") or None,
                "website": profile.get("website") or None,
                "email": profile.get("email") or None,
                "listed_date": profile.get("listed_date"),
                "chartered_capital": profile.get("chartered_capital") or None,
                "business_license": profile.get("business_license") or None,
                "tax_code": profile.get("tax_code") or None,
                "established_date": profile.get("established_date") or None,
                "end_date": None,
                "is_current": True,
                "update_time": doc.get("update_time") or datetime.utcnow().isoformat(),
                "created_time": doc.get("update_time") or datetime.utcnow().isoformat(),

            })

        df = pd.DataFrame(rows)
        sql = self._create_table_sql(self.dim_name, self.table_creator)
        df.drop_duplicates()
        return df, sql

    def get_market_key(self, market_type):
        if not market_type:
            return None
        result = self.postgresql_client.query(
            f"SELECT market_key FROM {self.schema_name}.dim_market_type WHERE market_type = %s",
            (market_type,)
        )
        rows = result.get("results", []) if result.get("ok") else []
        if rows:
            return rows[0]['market_key']
        else:
            # If not found, insert new record
            market_key = self.table_creator.get_id()
            market_name = ""  # You can add logic to get market_name if available
            self.postgresql_client.execute(
                f"INSERT INTO {self.schema_name}.dim_market_type (market_key, market_type, market_name, update_time, created_time) VALUES (%s, %s, %s, %s, %s)",
                (market_key, market_type, market_name, datetime.utcnow().isoformat(), datetime.utcnow().isoformat())
            )
            return market_key

    def get_industry_sk(self, industry_code):
        if not industry_code:
            return None
        # Lookup industry_sk by industry_code, assuming unique for current
        result = self.postgresql_client.query(
            f"SELECT industry_sk FROM {self.schema_name}.dim_industry WHERE industry_code = %s AND is_current = TRUE",
            (industry_code,)
        )
        rows = result.get("results", []) if result.get("ok") else []
        if rows:
            return rows[0]['industry_sk']
        else:
            # If not found, insert new record
            industry_sk = self.table_creator.get_id()
            industry_key = f"default_{industry_code}"  # Adjust
            industry_metric = "default"
            self.postgresql_client.execute(
                f"INSERT INTO {self.schema_name}.dim_industry (industry_sk, industry_metric, industry_code, effective_date, is_current, update_time, created_time) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (industry_sk, industry_metric, industry_code, datetime.utcnow().isoformat(), True, datetime.utcnow().isoformat(), datetime.utcnow().isoformat())
            )
            return industry_sk


# dim_report_type
class DimReportTypeLoader(DimLoader):
    def __init__(self, datalake_config, mongo_reader, datawarehouse_logger=None, postgresql_client=None):
        super().__init__(datalake_config, mongo_reader, datawarehouse_logger, postgresql_client)
        # report types are static; no collection required
        self.dim_name = "dim_report_type"
        self.table_creator = TableCreator(machine_id=1, character_specific=self.dim_name)

    def load(self):
        mapping = [
            {"report_type_key": "Y", "report_type_code": "Y", "description": "annually"},
            {"report_type_key": "Q", "report_type_code": "Q", "description": "quarterly"},
        ]
        df = pd.DataFrame(mapping)
        sql = self._create_table_sql(self.dim_name, self.table_creator)
        df.drop_duplicates()
        return df, sql



# FactTradeLoader using TradingDataDoc
class FactTradeLoader(FactLoader):
    def __init__(self, datalake_config, datawarehouse_logger, postgres_client, dim_repo, mongo_reader):
        super().__init__(datalake_config, datawarehouse_logger, postgres_client, dim_repo, mongo_reader )
        self.collection_name = self._get_collection("trading_data")
        self.fact_name = self._get_fact_name("fact_trade", "fact_trade")
        self.table_creator = TableCreator(machine_id=1, character_specific=self.fact_name)

    def load(self):
        raw_docs = self.mongo.find_table(self.collection_name)
        rows = []
        for doc in raw_docs.get("data"):
            company_key = self.get_company_key(doc.get("symbol"),self.table_creator)

            trade_dt = doc.get("trade_datetime")
            rows.append({
                "trade_key": self.table_creator.get_id(),
                "trade_date": trade_dt,
                "company_key": company_key,
                "close_price": doc.get("price"),
                "open_price": doc.get("open_price"),
                "high_price": doc.get("high_price"),
                "low_price": doc.get("low_price"),
                "volume": doc.get("volume"),
                "foreign_buy": doc.get("foreign_buy"),
                "foreign_sell": doc.get("foreign_sell"),
                "foreign_net_value": doc.get("foreign_net_value"),
                "update_time": doc.get("update_time") or datetime.utcnow().isoformat()
                
            })
        df = pd.DataFrame(rows)
        sql = self.create_fact_table_sql(self.fact_name, self.table_creator)
        df.drop_duplicates()
        return df, sql


# FactMatchDetailLoader
class FactMatchDetailLoader(FactLoader):
    def __init__(self, datalake_config, datawarehouse_logger, postgres_client, dim_repo, mongo_reader):
        
        super().__init__(datalake_config, mongo_reader, dim_repo, datawarehouse_logger, postgres_client)
        self.collection_name = self._get_collection("match_details")
        self.fact_name = self._get_fact_name("fact_match_detail", "fact_match_detail")
        self.table_creator = TableCreator(machine_id=1, character_specific=self.fact_name)

    def load(self):
        raw = list(self.mongo.find_table(self.collection_name) or [])
        rows = []
        for doc in raw.get("data"):
            company_key = self.get_company_key(doc.get("symbol"),self.table_creator)
            rows.append({
                "match_key": self.table_creator.get_id(),
                "company_key": company_key,
                "match_datetime": doc.get("match_datetime"),
                "price": doc.get("price"),
                "volume": doc.get("volume"),
                "fluctuation_range": doc.get("fluctuation_range"),
                "accum_volume": doc.get("accum_volume"),
                "update_time": doc.get("update_time") or datetime.utcnow().isoformat()
                
            })
        df = pd.DataFrame(rows)
        sql = self.create_fact_table_sql(self.fact_name, self.table_creator)
        df.drop_duplicates()
        return df, sql


# FactIncomeStatementLoader (IncomeStatementDoc)
class FactIncomeStatementLoader(FactLoader):
    def __init__(self, datalake_config, datawarehouse_logger, postgres_client, dim_repo, mongo_reader):
        super().__init__(datalake_config, mongo_reader, dim_repo, datawarehouse_logger, postgres_client)
        # can map both annually/quarterly collections
        self.collection_income_statement_annually = self._get_collection("income_statement_annually")
        self.collection_income_statement_quarterly = self._get_collection("income_statement_quarterly")
        self.fact_income_statement_quarterly = self._get_fact_name("fact_income_statement_quarterly", "fact_income_statement_quarterly")
        self.fact_income_statement_annually = self._get_fact_name("fact_income_statement_annually", "fact_income_statement_annually")
        self.table_creator_quaterly = TableCreator(machine_id=1, character_specific=self.fact_income_statement_quarterly)
        self.table_creator_annually = TableCreator(machine_id=1, character_specific=self.fact_income_statement_annually)

    def transform_income_statement_quarterly(self, doc):
        data = doc.get("data")
        symbol = doc.get("symbol")
        report_type = doc.get("report_type")
        update_time = doc.get("update_time")

        period_columns = [c for c in df.columns if c != "Chỉ tiêu"]
        company_key = self.get_company_key(doc.symbol)
        time_report_type_key = self.get_report_type_key(report_type = "quarterly")

        rows = []
        for _, r in df.iterrows():
            metric_vi = r["Chỉ tiêu"]

            mapping = Pattern_IncomeStatementStandardLoadToDW.METRIC_MAPPING.get(metric_vi)
            if not mapping:
                continue  # hoặc log warning

            for col in period_columns:
                if pd.isna(r[col]):
                    continue

                quarter, year = col.split("_")

                rows.append({
                    "income_key": self.table_creator_quaterly.get_id(),
                    "company_key": company_key,
                    "time_report_type_key": time_report_type_key ,
                    "symbol": symbol,
                    "time_report_type": report_type,
                    "financial_report_type": "income_statement",
                    "year": year,
                    "period": Pattern_IncomeStatementStandardLoadToDW.METRIC_MAPPING["Quý"].get(quarter),  # Quarter_1
                    "metric_code": mapping["metric_code"],
                    "metric_name_en": mapping["metric_name_en"],
                    "metric_group": mapping["metric_group"],
                    "metric_value": float(r[col]),
                    "update_time": update_time
                })

        return rows
    def transform_income_statement_annually(self, doc):
        data = doc.get("data")
        symbol = doc.get("symbol")
        report_type = doc.get("report_type")
        update_time = doc.get("update_time")
        if not isinstance(data, pd.DataFrame):
            return []

        df = data.copy()
        period_columns = [c for c in df.columns if c != "Chỉ tiêu"]
        company_key = self.get_company_key(doc.symbol)
        time_report_type_key = self.get_report_type_key(report_type = "annually")

        rows = []
        for _, r in df.iterrows():
            metric_vi = r["Chỉ tiêu"]

            mapping = Pattern_IncomeStatementStandardLoadToDW.METRIC_MAPPING.get(metric_vi)
            if not mapping:
                continue  # hoặc log warning

            for col in period_columns:
                _, year = col.split(" ")
                if pd.isna(r[col]):
                    continue

                rows.append({
                    "income_key": self.table_creator_annually.get_id(),
                    "company_key": company_key,
                    "time_report_type_key": time_report_type_key ,
                    "symbol": symbol,
                    "time_report_type": report_type,
                    "financial_report_type": "income_statement",
                    "year": year,
                    "metric_code": mapping["metric_code"],
                    "metric_name_en": mapping["metric_name_en"],
                    "metric_group": mapping["metric_group"],
                    "metric_value": float(r[col]),
                    "update_time": update_time
                })

        return rows


    def load_fact_income_statement_quarterly(self):
        raw = self.mongo.find_table(self.collection_income_statement_quarterly)
        rows = []
        for doc in raw:
            record = self.transform_income_statement_quarterly(doc)
            rows = list(set(rows.extend(record)))
        df = pd.DataFrame(rows)
        df["currency"] = Pattern_IncomeStatementStandardLoadToDW.currency
        df["unit"] =Pattern_IncomeStatementStandardLoadToDW.unit
        sql = self.create_fact_table_sql(self.fact_income_statement_quarterly, self.table_creator_quaterly)
        df.drop_duplicates()
        return df, sql

    def load_fact_income_statement_annually(self):
        raw = self.mongo.find_table(self.collection_income_statement_annually)
        rows = []
        for doc in raw:
            record = self.transform_income_statement_annually(doc)
            rows = list(set(rows.extend(record)))
        df = pd.DataFrame(rows)
        df["currency"] = Pattern_IncomeStatementStandardLoadToDW.currency
        df["unit"] =Pattern_IncomeStatementStandardLoadToDW.unit
        sql = self.create_fact_table_sql(self.fact_income_statement_annually, self.table_creator_annually)
        df.drop_duplicates()
        return df, sql

# FactBalanceSheetLoader (BalanceSheetDoc)
class FactBalanceSheetLoader(FactLoader):
    def __init__(self, datalake_config, datawarehouse_logger, postgres_client, dim_repo, mongo_reader):
        super().__init__(datalake_config, mongo_reader, dim_repo, datawarehouse_logger, postgres_client)
        # can map both annually/quarterly collections
        self.collection_balance_sheet_annually = self._get_collection("balance_sheet_annually")
        self.collection_balance_sheet_quarterly = self._get_collection("balance_sheet_quarterly")
        self.fact_balance_sheet_quarterly = self._get_fact_name("fact_balance_sheet_quarterly", "fact_balance_sheet_quarterly")
        self.fact_balance_sheet_annually = self._get_fact_name("fact_balance_sheet_annually", "fact_balance_sheet_annually")
        self.table_creator_quarterly = TableCreator(machine_id=1, character_specific=self.fact_balance_sheet_quarterly)
        self.table_creator_annually = TableCreator(machine_id=1, character_specific=self.fact_balance_sheet_annually)
        self.logger = datawarehouse_logger
    def transform_balance_sheet_quarterly(self, doc):
        """
        Transform data from MongoDB to PostgreSQL fact table format
        """
        data = doc.get("data")
        symbol = doc.get("symbol")
        report_type = doc.get("report_type")
        update_time = doc.get("update_time")

        company_key = self.get_company_key(symbol, self.table_creator_quarterly)
        time_report_type_key = self.get_report_type_key(report_type, self.table_creator_quarterly)

        rows = []
        for info in data:
            period_columns = [c for c in list(info.keys()) if c != "Chỉ tiêu"]

            metric_vi = info.get("Chỉ tiêu")
            # if not metric_vi:
            #     raise ValueError("Metric_vi is empty")

            mapping = Pattern_BalanceSheetStandardLoadToDW.METRIC_MAPPING.get(metric_vi)
            if not mapping:
                # self.logger.warning(f"Metric_vi {metric_vi} is not supported")
                continue

            for col in period_columns:
                _, quarter, year = col.split(" ")
                if not quarter or not year:
                    raise ValueError(f"Invalid period column {col}")

                metric_value = info.get(col)
                if not metric_value:
                    self.logger.warning(f"Metric value for column {col} is empty")
                    continue

                try:
                    metric_value = float(metric_value)
                except ValueError:
                    # self.logger.error(f"Invalid metric value for column {col}: {metric_value}")
                    continue

                rows.append({
                    "balance_key": self.table_creator_quarterly.get_id(),
                    "company_key": company_key,
                    "time_report_type_key": time_report_type_key ,
                    "symbol": symbol,
                    "time_report_type": report_type,
                    "financial_report_type": "balance_sheet",
                    "year": year,
                    "period": quarter,
                    "metric_code": mapping["metric_code"],
                    "metric_name_en": mapping["metric_name_en"],
                    "metric_group": mapping["metric_group"],
                    "metric_value": metric_value,
                    "update_time": update_time
                })
        return rows
    
    def transform_balance_sheet_annually(self, doc):
        data = doc.get("data")
        symbol = doc.get("symbol")
        report_type = doc.get("report_type")
        update_time = doc.get("update_time")


        company_key = self.get_company_key(symbol, self.table_creator_annually)
        time_report_type_key = self.get_report_type_key(report_type = report_type, table_creator=self.table_creator_annually)

        rows = []
        for info in data:
            period_columns = [c for c in list(info.keys()) if c != "Chỉ tiêu"]
            metric_vi = info.get("Chỉ tiêu")
            if not metric_vi:
                raise ValueError("Metric_vi is empty")

            mapping = Pattern_BalanceSheetStandardLoadToDW.METRIC_MAPPING.get(metric_vi)
            if not mapping:
                # self.logger.warning(f"Metric_vi {metric_vi} is not supported")
                continue

            for col in period_columns:
                _, year = col.split(" ")
                if not year:
                    raise ValueError(f"Invalid period column {col}")

                metric_value = info.get(col)
                if not metric_value:
                    # self.logger.warning(f"Metric value for column {col} is empty")
                    continue

                try:
                    metric_value = float(metric_value)
                except ValueError:
                    # self.logger.error(f"Invalid metric value for column {col}: {metric_value}")
                    continue

                rows.append({
                    "balance_key": self.table_creator_annually.get_id(),
                    "company_key": company_key,
                    "time_report_type_key": time_report_type_key ,
                    "symbol": symbol,
                    "time_report_type": report_type,
                    "financial_report_type": "balance_sheet",
                    "year": year,
                    "metric_code": mapping["metric_code"],
                    "metric_name_en": mapping["metric_name_en"],
                    "metric_group": mapping["metric_group"],
                    "metric_value": metric_value,
                    "update_time": update_time
                })
        return rows
    def load_fact_balance_sheet_quarterly(self):
        raw = self.mongo.find_table(self.collection_balance_sheet_quarterly)
        rows = []

        for doc in raw.get("data"):
            record = self.transform_balance_sheet_quarterly(doc)
            rows.extend(record)
        df = pd.DataFrame(rows)
        df["currency"] = Pattern_BalanceSheetStandardLoadToDW.currency
        df["unit"] = Pattern_BalanceSheetStandardLoadToDW.unit
        sql = self.create_fact_table_sql(self.fact_balance_sheet_quarterly,self.table_creator_quarterly)
        df.drop_duplicates()
        return df, sql
    
    def load_fact_balance_sheet_annually(self):
        raw = self.mongo.find_table(self.collection_balance_sheet_annually)
        rows = []
        for doc in raw.get("data"):
            record = self.transform_balance_sheet_annually(doc)
            rows.extend(record)
        df = pd.DataFrame(rows)
        df["currency"] = Pattern_BalanceSheetStandardLoadToDW.currency
        df["unit"] = Pattern_BalanceSheetStandardLoadToDW.unit
        sql = self.create_fact_table_sql(self.fact_balance_sheet_annually,self.table_creator_annually)
        df.drop_duplicates()
        return df, sql
    


# FactBusinessPlanLoader
class FactBusinessPlanLoader(FactLoader):
    def __init__(self, datalake_config, datawarehouse_logger, postgres_client, dim_repo, mongo_reader):
        super().__init__(datalake_config, mongo_reader, dim_repo, datawarehouse_logger, postgres_client)
        self.collection_name = self._get_collection("business_plan")
        self.fact_name = self._get_fact_name("fact_business_plan", "fact_business_plan")
        self.table_creator = TableCreator(machine_id=1, character_specific=self.fact_name)

    def load(self):
        raw = self.mongo.find_table(self.collection_name)
        rows = []
        for doc in raw.get("data"):
            company_key = self.get_company_key(doc.get("symbol"),self.table_creator)
            rows.append({
                "plan_key": self.table_creator.get_id(),
                "company_key": company_key,
                "symbol": doc.get("symbol"),
                "year": doc.get("Year"),
                "plan_revenue": doc.get("Plan_revenue"),
                "revenue_achived": doc.get("Pass_revenue"),
                "plan_profit": doc.get("Plan_profit"),
                "profit_achived": doc.get("Pass_profit"),
                "update_time": doc.get("update_time")

            })
        df = pd.DataFrame(rows)
        df.drop_duplicates()
        sql = self.create_fact_table_sql(self.fact_name,self.table_creator)
        return df, sql

class FactFinancialMetricsLoader(FactLoader):
    def __init__(self, datalake_config, datawarehouse_logger, postgres_client, dim_repo, mongo_reader):
        
        super().__init__(datalake_config,  mongo_reader, dim_repo, datawarehouse_logger, postgres_client)
        self.collection_name = self._get_collection("financial_info")
        self.fact_name = self._get_fact_name("fact_financial_metrics", "fact_financial_metrics")
        self.table_creator = TableCreator(machine_id=1, character_specific=self.fact_name)
    
    def load(self):
        raw = self.mongo.find_table(self.collection_name)
        rows = []
    
        for doc in raw.get("data"):
            # fdoc = FinancialInfoDoc.from_extract(doc)

            company_key = self.get_company_key(doc.get("symbol"),self.table_creator)
                # period_key = self.get_date_key(r.get("period"))
            rows.append({
                "financial_ratio_key": self.table_creator.get_id(),
                "company_key":company_key,
                "reference_price":  doc.get("reference_price"),
                "company_name": doc.get("company_name"),
                "open_price": doc.get("open_price"),
                "high_price": doc.get("high_price"),
                "low_price": doc.get("low_price"),
                "volume": doc.get("volume"),
                "book_value": doc.get("book_value"),
                "earning_per_share_eps": doc.get("eps"),
                "price_on_earning_pe": doc.get("pe"),
                "price_on_book_value_pb": doc.get("pb"),
                "return_on_equity_roe": doc.get("roe"),
                "return_on_assets_roa": doc.get("roa"),
                "beta": doc.get("beta"),
                "market_cap": doc.get("market_cap"),
                "listed_volume": doc.get("listed_volume"),
                "average_volume_52_weeks": doc.get("avg_volume_52w"),
                "high_low_52_weeks": doc.get("high_low_52w"),
                "debt": doc.get("debt"),
                "equity": doc.get("equity"),
                "debt_to_equity": doc.get("debt_to_equity"),
                "equity_to_assets": doc.get("equity_to_assets"),
                "cash": doc.get("cash"),
                "eps_power": doc.get("eps_power"),
                "roe_power": doc.get("roe_power"),
                "invest_efficiency": doc.get("invest_efficiency"),
                "pb_power": doc.get("pb_power"),
                "price_growth_power": doc.get("price_growth_power"),
                "update_time": doc.get("update_time")
            })
        df = pd.DataFrame(rows)
        df.drop_duplicates()
        sql = self.create_fact_table_sql(self.fact_name,self.table_creator)
        return df, sql

class FactIndustryLoader(FactLoader):
    def __init__(self, datalake_config, datawarehouse_logger, postgres_client, dim_repo, mongo_reader):
        super().__init__(datalake_config,  mongo_reader, dim_repo, datawarehouse_logger, postgres_client)
        self.collection_name = self._get_collection("industry_list")
        self.fact_name = self._get_fact_name("fact_industry_summary", "fact_industry_summary")
        self.table_creator = TableCreator(machine_id=1, character_specific=self.fact_name)
    
    def parse_number(self,val):
        if val is None:
            return None

        if isinstance(val, (int, float)):
            return float(val)

        s = str(val).strip()

        if s in ("", "-", "—", "N/A", "NA"):
            return None

        s = s.replace(",", "")
        s = s.replace("%", "")

        try:
            return float(s)
        except ValueError:
            return None


    def load(self):
        raw = self.mongo.find_table(self.collection_name)
        rows = []
        data_reconcile = {}
        exist_keys = set()
        for documentation in raw.get("data"):
            for keys, values in documentation.get("data")[0].items():
                parts = str(keys).split("_")
                industry_code = parts[1]
                industry_name = parts[2]
                new_keys = (industry_code, industry_name)
                values["industry_code"] = industry_code
                values["industry_name"] = industry_name
                if new_keys not in exist_keys:
                    exist_keys.add(new_keys)
                    data_reconcile[new_keys] = values
                else:
                    dict_1 = data_reconcile[new_keys]
                    dict_2 = values
                    dict_3 = {**dict_1, **dict_2}
                    data_reconcile[new_keys] = dict_3
        for info in data_reconcile:
            value_dict = data_reconcile[info]
            rows.append({
                "industry_sk": self.get_industry_key(info[0], self.table_creator),
                "industry_code": value_dict.get("industry_code"),
                "industry_name": value_dict.get("industry_name"),
                "industry_index": self.parse_number(value_dict.get("index")),
                "percentage_change": self.parse_number(value_dict.get("change")),
                "liquidity": self.parse_number(value_dict.get("liquidity")),
                "total_capital": self.parse_number(value_dict.get("capital")),
                "average_price": self.parse_number(value_dict.get("avg_price")),
                "book_value": self.parse_number(value_dict.get("book_value")),
                "earning_per_share_eps": self.parse_number(value_dict.get("eps")),
                "price_on_earning_pe": self.parse_number(value_dict.get("pe")),
                "return_on_asset_roa": self.parse_number(value_dict.get("roa")),
                "return_on_equity_roe": self.parse_number(value_dict.get("roe")),
                "supply_volumn": self.parse_number(value_dict.get("supply_volumn")),
                "total_asset": self.parse_number(value_dict.get("total_asset")),
                "total_equity": self.parse_number(value_dict.get("total_equity")),
                "total_liabilities": self.parse_number(value_dict.get("total_liabilities")),
                "percentage_debt_on_equity": self.parse_number(value_dict.get("percentage_debt_on_equity")),
                "percentage_equity_on_assets": self.parse_number(value_dict.get("percentage_equity_on_assets")),
                "revenue": self.parse_number(value_dict.get("revenue")),
                "profit_before_tax": self.parse_number(value_dict.get("profit_before_tax")),
                "created_time": datetime.utcnow(),
                "updated_time": documentation.get("update_time") or datetime.utcnow()
            })

        df = pd.DataFrame(rows)
        df.drop_duplicates()
        # pd.set_option("display.max_columns", None)
        # pd.set_option("display.max_rows", None)
        # print(df)
        sql = self.create_fact_table_sql(self.fact_name,self.table_creator)
        return df, sql
                   
