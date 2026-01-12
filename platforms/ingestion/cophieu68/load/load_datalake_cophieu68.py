from pymongo import MongoClient
import time
from datetime import datetime
from typing import List, Dict, Any, Optional, Iterable
import logging
from platforms.storage.datalake.mongodb.data_lake_storage import MongoWriter
from platforms.storage.base_storage import to_primitive
from platforms.ingestion.cophieu68.dto.load_models import (
    BaseDoc,
    TradingDataDoc,
    FinancialInfoDoc,
    IncomeStatementDoc,
    BalanceSheetDoc,
    MatchDetailsDoc,
    BusinessPlanDoc,
    doc_from_extract
)


class MongoLoader(MongoWriter):
    """
    Lightweight Mongo writer. Use dataclasses in shared.common_models to normalize payloads
    before writing. Short-lived client per call to ensure resource cleanup.
    """

    def __init__(
        self,
        username: str,
        password: str,
        authSource: str,
        database: str,
        host: str,
        port: int,
        logger: Optional[logging.Logger] = None,
        **client_kwargs,
    ):
        self.database = database
        self.uri = f"mongodb://{username}:{password}@{host}:{port}/?authSource={authSource}"
        self.logger = logger or logging.getLogger(__name__)
        self.client_kwargs = client_kwargs or {}

    # --- internal helpers ---
    def _now_iso(self) -> str:
        return datetime.utcnow().isoformat()

    def _upsert(self, coll, doc: Dict[str, Any], key_fields: Optional[Iterable[str]] = None) -> None:
        if not doc:
            return
        # build filter from provided key fields or fallback to symbol
        key_fields = list(key_fields) if key_fields else []
        filter_q = {}
        for k in key_fields:
            v = doc.get(k)
            if v is not None:
                filter_q[k] = v
        if not filter_q and doc.get("symbol"):
            filter_q = {"symbol": doc.get("symbol")}
        if not filter_q:
            # no reliable key -> insert as new document with generated _ingested_at
            doc.setdefault("_ingested_at", self._now_iso())
            coll.insert_one(doc)
            return
        coll.update_one(filter_q, {"$set": doc}, upsert=True)

    def _ensure_client(self):
        return MongoClient(self.uri, **self.client_kwargs)

    # --- loader methods (normalize using dataclasses) ---
    def load_company_info(self, collection_name: str, company_profiles: Dict[str, Any]) -> None:
        client = self._ensure_client()
        try:
            db = client[self.database]
            coll = db[collection_name]
            now = self._now_iso()
            for symbol, profile in company_profiles.items():
                # profile may be dataclass or dict; store under profile_json and normalized 'data' empty
                doc = BaseDoc.from_extract({"symbol": symbol, "data": []}).to_mongo_dict()
                # keep original profile under profile_json
                if profile is not None:
                    doc["profile_json"] = profile if isinstance(profile, dict) else getattr(profile, "__dict__", profile)
                doc["update_time"] = now
                self._upsert(coll, doc, key_fields=["symbol"])
        finally:
            client.close()

    def load_crawl_industry_info(self, collection_name: str, industry_data: Dict[str, Any]) -> None:
        client = self._ensure_client()
        try:
            db = client[self.database]
            coll = db[collection_name]
            now = self._now_iso()
            for metric, payload in industry_data.items():
                doc = BaseDoc.from_extract({"industry_metric": metric, "data": payload}).to_mongo_dict()
                doc["industry_metric"] = metric
                doc["update_time"] = now
                self._upsert(coll, doc, key_fields=["industry_metric"])
        finally:
            client.close()

    def load_market_list(self, collection_name: str, stock_list: Any) -> None:
        """
        Accepts list[dict] or dict mapping market->list. Stores each market doc with market_type and symbols.
        """
        client = self._ensure_client()
        try:
            db = client[self.database]
            coll = db[collection_name]
            now = self._now_iso()
            docs: List[Dict[str, Any]] = []
            if isinstance(stock_list, dict):
                # flatten mapping market_type -> list
                for market, lst in stock_list.items():
                    if isinstance(lst, list):
                        symbols = []
                        for it in lst:
                            if isinstance(it, dict):
                                if "symbol" in it:
                                    symbols.append(it["symbol"])
                                elif "symbol_list" in it:
                                    symbols.extend(it["symbol_list"])
                            elif isinstance(it, str):
                                symbols.append(it)
                        docs.append({"market_type": market, "symbols": symbols, "raw": lst})
                    else:
                        docs.append({"market_type": market, "symbols": [], "raw": lst})
            elif isinstance(stock_list, list):
                for item in stock_list:
                    if isinstance(item, dict):
                        docs.append({"market_type": item.get("market_type"), "symbols": item.get("symbols") or item.get("symbol_list") or [], "raw": item})
                    else:
                        docs.append({"market_type": None, "symbols": [item], "raw": item})
            else:
                docs.append({"market_type": None, "symbols": stock_list, "raw": stock_list})

            for d in docs:
                d["update_time"] = now
                self._upsert(coll, d, key_fields=["market_type"])
        finally:
            client.close()

    def load_crawl_stock_info(self, collection_name: str, stock_data: Any) -> None:
        """
        Accepts:
          - list of dicts
          - dict mapping market->list
          - single dict/dataclass
        Upserts per symbol.
        """
        client = self._ensure_client()
        try:
            db = client[self.database]
            coll = db[collection_name]
            now = self._now_iso()
            docs: List[Dict[str, Any]] = []
            if isinstance(stock_data, dict) and not all(isinstance(k, int) for k in stock_data.keys()):
                # if dict is mapping market->list or symbol->profile
                # detect if values are lists
                for k, v in stock_data.items():
                    if isinstance(v, list):
                        for it in v:
                            if isinstance(it, dict):
                                docs.append(it)
                            else:
                                docs.append({"symbol": it, "market_type": k})
                    elif isinstance(v, dict):
                        # treat as profile
                        docs.append({"symbol": k, **v})
                    else:
                        docs.append({"symbol": v, "market_type": k})
            elif isinstance(stock_data, list):
                for it in stock_data:
                    if isinstance(it, dict):
                        docs.append(it)
                    else:
                        docs.append({"symbol": it})
            else:
                # single item
                docs.append(stock_data if isinstance(stock_data, dict) else {"symbol": stock_data})

            for d in docs:
                # normalize with BaseDoc to ensure data field exists
                doc = BaseDoc.from_extract(d).to_mongo_dict()
                # preserve market_type and symbol_list/symbols if present
                if isinstance(d, dict):
                    if d.get("market_type"):
                        doc["market_type"] = d.get("market_type")
                    if d.get("symbols"):
                        doc["symbols"] = d.get("symbols")
                    if d.get("symbol_list"):
                        doc["symbols"] = d.get("symbol_list")
                doc["update_time"] = now
                symbol = doc.get("symbol") or doc.get("code") or doc.get("ticker")
                if symbol:
                    self._upsert(coll, doc, key_fields=["symbol"])
                else:
                    # fallback insert
                    self._upsert(coll, doc, key_fields=None)
        finally:
            client.close()

    def load_crawl_financial_info(self, collection_name: str, financial_data: Any) -> None:
        client = self._ensure_client()
        try:
            db = client[self.database]
            coll = db[collection_name]
            now = self._now_iso()
            financial_data["update_time"] = now
            self._upsert(coll, financial_data, key_fields=["symbol"])
        finally:
            client.close()

    def load_trading_data(self, collection_name: str, trading_data: Any) -> None:
        client = self._ensure_client()
        try:
            db = client[self.database]
            coll = db[collection_name]
            now = self._now_iso()
            doc = TradingDataDoc.from_extract(trading_data).to_mongo_dict()
            doc["update_time"] = now
            return
            self._upsert(coll, doc, key_fields=["symbol"])
        finally:
            client.close()

    def load_detail_income_statement_annually(self, collection_name: str, income_data: Any) -> None:
        client = self._ensure_client()
        try:
            db = client[self.database]
            coll = db[collection_name]
            now = self._now_iso()
            if income_data and "data" in income_data:
                income_data["data"] = to_primitive(income_data["data"])
            doc = IncomeStatementDoc.from_extract(income_data).to_mongo_dict()
            doc["update_time"] = now
            self._upsert(coll, doc, key_fields=["symbol", "report_type"])
        finally:
            client.close()

    def load_detail_income_statement_quarterly(self, collection_name: str, income_data: Any) -> None:
        client = self._ensure_client()
        try:
            db = client[self.database]
            coll = db[collection_name]
            now = self._now_iso()
            if income_data and "data" in income_data:
                income_data["data"] = to_primitive(income_data["data"])
            doc = IncomeStatementDoc.from_extract(income_data).to_mongo_dict()
            doc["update_time"] = now
            self._upsert(coll, doc, key_fields=["symbol", "report_type"])
        finally:
            client.close()
        

    def load_detail_balance_sheet_annually(self, collection_name: str, balance_data: Any) -> None:
        client = self._ensure_client()
        try:
            db = client[self.database]
            coll = db[collection_name]
            now = self._now_iso()
            if balance_data and "data" in balance_data:
                balance_data["data"] = to_primitive(balance_data["data"])
            doc = BalanceSheetDoc.from_extract(balance_data).to_mongo_dict()
            doc["update_time"] = now
            self._upsert(coll, doc, key_fields=["symbol", "report_type"])
        finally:
            client.close()

    def load_detail_balance_sheet_quarterly(self, collection_name: str, balance_data: Any) -> None:
        client = self._ensure_client()
        try:
            db = client[self.database]
            coll = db[collection_name]
            now = self._now_iso()
            if balance_data and "data" in balance_data:
                balance_data["data"] = to_primitive(balance_data["data"])
            doc = BalanceSheetDoc.from_extract(balance_data).to_mongo_dict()
            doc["update_time"] = now
            self._upsert(coll, doc, key_fields=["symbol", "report_type"])
        finally:
            client.close()
        

    def load_details_match(self, collection_name: str, match_data: Any) -> None:
        client = self._ensure_client()
        try:
            db = client[self.database]
            coll = db[collection_name]
            now = self._now_iso()
            doc = MatchDetailsDoc.from_extract(match_data).to_mongo_dict()
            doc["update_time"] = now
            # upsert per symbol; match rows stored under data list
            self._upsert(coll, doc, key_fields=["symbol"])
        finally:
            client.close()

    def load_business_plan(self, collection_name: str, business_plan_data: Any) -> None:
        client = self._ensure_client()
        try:
            db = client[self.database]
            coll = db[collection_name]
            now = self._now_iso()
            doc = BusinessPlanDoc.from_extract(business_plan_data).to_mongo_dict()
            doc["update_time"] = now
            self._upsert(coll, doc, key_fields=["symbol"])
        finally:
            client.close()

    def load_financial_report_summary(self, collection_name: str, report_data: Any) -> None:
        client = self._ensure_client()
        try:
            db = client[self.database]
            coll = db[collection_name]
            now = self._now_iso()
            doc = BaseDoc.from_extract(report_data).to_mongo_dict()
            # copy table_index/report_type if present in raw payload
            if isinstance(report_data, dict):
                if "table_index" in report_data:
                    doc["table_index"] = report_data["table_index"]
                if "report_type" in report_data:
                    doc["report_type"] = report_data["report_type"]
            doc["update_time"] = now
            self._upsert(coll, doc, key_fields=["symbol", "table_index", "report_type"])
        finally:
            client.close()