from pymongo import MongoClient
from datetime import datetime
from internal.dags.cophieu68_dag.load.base_loading import *


class MongoLoader(MongoWriter):
    """
    Lightweight Mongo writer. Uses a short-lived client per call to ensure resource cleanup.
    """

    def __init__(self, username: str, 
                        password: str, 
                        authSource: str, 
                        database: str, 
                        host: str,
                        port: int,
                        logger: Optional[logging.Logger] = None, **client_kwargs):
        

        self.database = database
        self.uri = f"mongodb://{username}:{password}@{host}:{port}/?authSource={authSource}"
        self.logger = logger or logging.getLogger(__name__)
        self.client_kwargs = client_kwargs
        
    

    def load_company_info(self, collection_name: str, company_profiles: Dict) -> None:
        """
        Load list of symbols into MongoDB.
        Performs upsert to avoid duplicate entries.
        """
        client = MongoClient(self.uri, **self.client_kwargs)
        try:
            db = client[self.database]
            collection = db[collection_name]
            time_update = datetime.now().isoformat()
            for symbol, profile in company_profiles.items():
                filter_query = {"symbol": symbol, "profile": profile}
                update_query = {"$set": {"symbol": symbol, "profile": profile}}
                collection.update_one(filter_query, update_query, upsert=True)
        except Exception as e:
            raise Exception(f"Error loading market list into MongoDB: {e}") from e
        finally:
            client.close()
    
    def load_crawl_industry_info(self, collection_name: str, industry_data: Dict[str, Any]) -> None:
        """
        Load industry information into MongoDB.
        Performs upsert to avoid duplicate entries.
        """
        client = MongoClient(self.uri, **self.client_kwargs)
        try:
            database = client[self.database]
            collection = database[collection_name]
            time_update = datetime.now().isoformat()
            for metric, data in industry_data.items():
                data["update_time"] = time_update
                filter_query = {"industry_metric": metric, "data": data}
                update_query = {"$set": data}
                collection.update_one(filter_query, update_query, upsert=True)
        finally:
            client.close()
    
    def load_market_list(self, collection_name: str, stock_info: List) -> None:
        """
        Load stock information into MongoDB.
        Performs upsert to avoid duplicate entries.
        """
        client = MongoClient(self.uri, **self.client_kwargs)
        try:
            db = client[self.database]
            collection = db[collection_name]
            time_update = datetime.now().isoformat()
            for data in stock_info:
                filter_query = {"market_type": data["market_type"],"symbol_list": data["symbols"]}
                data["update_time"] = time_update
                update_query = {"$set": data}
                collection.update_one(filter_query, update_query, upsert=True)
        except Exception as e:
            raise Exception(f"Error loading stock info into MongoDB: {e}") from e
        finally:
            client.close()
    
    def load_crawl_financial_info(self, collection_name: str,financial_data: List[Dict[str, Any]]) -> None:
        """
        Load financial information into MongoDB.
        Performs upsert to avoid duplicate entries.
        """
        client = MongoClient(self.uri, **self.client_kwargs)
        try:
            db = client[self.database]
            collection = db[collection_name]
            time_update = datetime.now().isoformat()
            financial_data["update_time"] = time_update
            filter_query = {"symbol": financial_data["symbol"]}
            update_query = {"$set": financial_data}
            collection.update_one(filter_query, update_query, upsert=True)
        except Exception as e:
            raise Exception(f"Error loading financial info into MongoDB: {e}") from e
        finally:
            client.close()
    
    def load_trading_data(self, collection: str, trading_data: Dict) -> None:
        """
        Load trading data into MongoDB.
        Performs upsert to avoid duplicate entries.
        """
        client = MongoClient(self.uri, **self.client_kwargs)
        try:
            db = client[self.database]
            collection = db[collection]
            time_update = datetime.now().isoformat()
            trading_data["update_time"] = time_update
            filter_query = {"symbol": trading_data["symbol"], "data": trading_data["records"]}
            update_query = {"$set": trading_data}
            collection.update_one(filter_query, update_query, upsert=True)
        except Exception as e:
            raise Exception(f"Error loading trading data into MongoDB: {e}") from e
        finally:
            client.close()
    
    def load_detail_income_statement_yearly(self, collection_name: str,income_data: Dict[str, Any]) -> None:
        """
        Load detailed income statement data into MongoDB.
        Performs upsert to avoid duplicate entries.
        """
        client = MongoClient(self.uri, **self.client_kwargs)
        try:
            db = client[self.database]
            collection = db[collection_name]
            time_update = datetime.now().isoformat()
            income_data["update_time"] = time_update
            filter_query = {"symbol": income_data["symbol"], "report_type": income_data["report_type"], "data": income_data["data"]}
            update_query = {"$set": income_data}
            collection.update_one(filter_query, update_query, upsert=True)
        except Exception as e:
            raise Exception(f"Error loading income statement data into MongoDB: {e}") from e
        finally:
            client.close()
    
    def load_detail_income_statement_quarterly(self, collection_name: str, income_data: List[Dict[str, Any]]) -> None:
        """
        Load detailed quarterly income statement data into MongoDB.
        Performs upsert to avoid duplicate entries.
        """
        client = MongoClient(self.uri, **self.client_kwargs)
        try:
            db = client[self.database]
            collection = db[collection_name]
            time_update = datetime.now().isoformat()
            income_data["update_time"] = time_update
            filter_query = {"symbol": income_data["symbol"], "report_type": income_data["report_type"], "data": income_data["data"]}
            update_query = {"$set": income_data}
            collection.update_one(filter_query, update_query, upsert=True)
        except Exception as e:
            raise Exception(f"Error loading quarterly income statement data into MongoDB: {e}") from e
        finally:
            client.close()
    
    def load_detail_balance_sheet_quarterly(self, collection_name: str, balance_data: Dict[str, Any]) -> None:
        """
        Load detailed balance sheet data into MongoDB.
        Performs upsert to avoid duplicate entries.
        """
        client = MongoClient(self.uri, **self.client_kwargs)
        try:
            db = client[self.database]
            collection = db[collection_name]
            time_update = datetime.now().isoformat()
            balance_data["update_time"] = time_update
            filter_query = {"symbol": balance_data["symbol"], "report_type": balance_data["report_type"], "data": balance_data["data"]}
            update_query = {"$set": balance_data}
            collection.update_one(filter_query, update_query, upsert=True)
        except Exception as e:
            raise Exception(f"Error loading balance sheet data into MongoDB: {e}") from e
        finally:
            client.close()
    
    def load_detail_balance_sheet_yearly(self, collection_name: str, balance_data: Dict[str, Any]) -> None:
        """
        Load detailed balance sheet data into MongoDB.
        Performs upsert to avoid duplicate entries.
        """
        client = MongoClient(self.uri, **self.client_kwargs)
        try:
            db = client[self.database]
            collection = db[collection_name]
            time_update = datetime.now().isoformat()
            balance_data["update_time"] = time_update
            filter_query = {"symbol": balance_data["symbol"], "report_type": balance_data["report_type"], "data": balance_data["data"]}
            update_query = {"$set": balance_data}
            collection.update_one(filter_query, update_query, upsert=True)
        except Exception as e:
            raise Exception(f"Error loading balance sheet data into MongoDB: {e}") from e
        finally:
            client.close()

    def load_details_match(self,collection_name: str, match_data: Dict[str, Any]) -> None:
        """
        Load match details into MongoDB.
        Performs upsert to avoid duplicate entries.
        """
        client = MongoClient(self.uri, **self.client_kwargs)
        try:
            db = client[self.database]
            collection = db[collection_name]
            time_update = datetime.now().isoformat()
            match_data["update_time"] = time_update
            filter_query = {"symbol": match_data["symbol"], "data": match_data["data"]}
            update_query = {"$set": match_data}
            collection.update_one(filter_query, update_query, upsert=True)
        except Exception as e:
            raise Exception(f"Error loading match details into MongoDB: {e}") from e
        finally:
            client.close()
    
    def load_business_plan(self, collection_name: str, business_plan_data: Dict[str, Any]) -> None:
        """
        Load business plan data into MongoDB.
        Performs upsert to avoid duplicate entries.
        """
        client = MongoClient(self.uri, **self.client_kwargs)
        try:
            db = client[self.database]
            collection = db[collection_name]
            time_update = datetime.now().isoformat()
            business_plan_data["update_time"] = time_update
            filter_query = {"symbol": business_plan_data["symbol"], "data": business_plan_data["data"]}
            update_query = {"$set": business_plan_data}
            collection.update_one(filter_query, update_query, upsert=True)
        except Exception as e:
            raise Exception(f"Error loading business plan data into MongoDB: {e}") from e
        finally:
            client.close()

    def load_financial_report_summary(self, collection: str, report_data: List[Dict[str, Any]]) -> None:
        """
        Load financial report summary data into MongoDB.
        Performs upsert to avoid duplicate entries.
        """
        client = MongoClient(self.uri, **self.client_kwargs)
        try:
            db = client[self.database]
            collection = db[collection]
            time_update = datetime.now().isoformat()
            for report in report_data:
                filter_query = {"symbol": report["symbol"], "report_date": report["report_date"]}
                update_query = {"$set": report}
                collection.update_one(filter_query, update_query, upsert=True)
        except Exception as e:
            raise Exception(f"Error loading financial report summary into MongoDB: {e}") from e
        finally:
            client.close()