# ...existing code...
from datetime import datetime
import pandas as pd
from typing import Dict, Any, Optional

from shared.utils.files.util_cophieu68 import TableCreator
from platforms.storage.datalake.mongodb.data_lake_storage import MongoStorageBackend
from platforms.ingestion.cophieu68.dto.load_models import BaseDoc
from platforms.ingestion.cophieu68.dto import transform_models


class DimLoader:
    def __init__(self, datalake_config: Dict[str, Any], table_creator: TableCreator, mongo_reader: MongoStorageBackend):
        self.datalake_config = datalake_config or {}
        self.table_creator = table_creator
        self.mongo = mongo_reader
        self.schema_dw = transform_models.DATA_WAREHOUSE_SCHEMA or {}

        # map of collection names from YAML (storage.mongodb.collections.*)
        self.collection_map = (
            self.datalake_config.get("storage", {})
            .get("mongodb", {})
            .get("collections", {})
            or {}
        )

    def _get_collection(self, key: str) -> str:
        return self.collection_map.get(key, key)

    def _create_table_sql(self, dim_name: str) -> str:
        dims = self.schema_dw.get("dimensions", {})
        cols = dims.get(dim_name, {}).get("columns", {})
        norm = {}
        for col, meta in cols.items():
            if isinstance(meta, dict) and "type" in meta:
                norm[col] = meta
            else:
                parts = str(meta).split(None, 1)
                typ = parts[0]
                cons = parts[1] if len(parts) > 1 else ""
                norm[col] = {"type": typ, "constraints": cons}
        return self.table_creator.generate_create_table_sql(dim_name, norm)


# dim_market_type
class DimMarketTypeLoader(DimLoader):
    def __init__(self, datalake_config, table_creator, mongo_reader):
        super().__init__(datalake_config, table_creator, mongo_reader)
        # YAML key is 'list_stock' per config
        self.collection_name = self._get_collection("list_stock")
        self.dim_name = "dim_market_type"

    def load(self):
        raw = list(self.mongo.find_table(self.collection_name) or [])
        rows = []
        for doc in raw:
            b = BaseDoc.from_extract(doc)
            market_type = (doc.get("market_type") or b.symbol) if isinstance(doc, dict) else b.symbol
            rows.append({
                "market_key": market_type.lower() if market_type else None,
                "market_type": market_type,
                "market_name": doc.get("market_name") if isinstance(doc, dict) else None,
                "update_time": doc.get("update_time") or datetime.utcnow().isoformat(),
                "created_time": None
            })
        df = pd.DataFrame(rows)
        sql = self._create_table_sql(self.dim_name)
        return df, sql


# dim_industry
class DimIndustryLoader(DimLoader):
    def __init__(self, datalake_config, table_creator, mongo_reader):
        super().__init__(datalake_config, table_creator, mongo_reader)
        self.collection_name = self._get_collection("industry_info")
        self.dim_name = "dim_industry"

    def load(self):
        raw = list(self.mongo.find_table(self.collection_name) or [])
        rows = []
        for doc in raw:
            b = BaseDoc.from_extract(doc)
            key = b.symbol or (doc.get("industry_metric") if isinstance(doc, dict) else None)
            rows.append({
                "industry_key": key,
                "industry_code": key,
                "industry_name": doc.get("industry_name") if isinstance(doc, dict) else None,
                "update_time": doc.get("update_time") or datetime.utcnow().isoformat(),
                "created_time": None
            })
        df = pd.DataFrame(rows)
        sql = self._create_table_sql(self.dim_name)
        return df, sql


# dim_company (SCD2 simplified snapshot)
class DimCompanyLoader(DimLoader):
    def __init__(self, datalake_config, table_creator, mongo_reader):
        super().__init__(datalake_config, table_creator, mongo_reader)
        # YAML key for company profiles: use 'stock_info' from config
        self.collection_name = self._get_collection("stock_info")
        self.dim_name = "dim_company"

    def load(self):
        raw = list(self.mongo.find_table(self.collection_name) or [])
        rows = []
        for doc in raw:
            b = BaseDoc.from_extract(doc)
            profile = (doc.get("profile_json") if isinstance(doc, dict) else None) or {}
            # some payloads store profile under 'profile' or raw data under 'data'
            if not profile and isinstance(doc, dict):
                profile = doc.get("profile") or doc.get("raw") or doc.get("data") or {}
            symbol = b.symbol or profile.get("symbol") or profile.get("code")
            rows.append({
                "company_key": symbol,
                "symbol": symbol,
                "company_name": profile.get("full_name") or profile.get("company_name") or None,
                "market_key": profile.get("market_type") or None,
                "industry_key": profile.get("industry_code") or None,
                "profile_json": profile,
                "effective_from": None,
                "effective_to": None,
                "is_current": True,
                "created_time": None
            })
        df = pd.DataFrame(rows)
        sql = self._create_table_sql(self.dim_name)
        return df, sql


# dim_report_type
class DimReportTypeLoader(DimLoader):
    def __init__(self, datalake_config, table_creator, mongo_reader):
        super().__init__(datalake_config, table_creator, mongo_reader)
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