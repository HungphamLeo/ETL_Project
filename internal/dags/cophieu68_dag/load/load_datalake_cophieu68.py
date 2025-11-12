from pymongo import MongoClient
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
    

    def load_market_list(self, collection: str, market_type: str, market_data: List[Dict[str, Any]]) -> None:
        """
        Load list of symbols into MongoDB.
        Performs upsert to avoid duplicate entries.
        """
        client = MongoClient(self.uri, **self.client_kwargs)
        try:
            db = client[self.database]
            collection = db[collection]
            for symbol in market_data:
                filter_query = {"market_type": market_type, "symbol": symbol["symbol"]}
                update_query = {"$set": {"market_type": market_type, "symbol": symbol["symbol"]}}
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
            for metric, data in industry_data.items():
                filter_query = {"industry_metric": metric, "data": data}
                update_query = {"$set": data}
                collection.update_one(filter_query, update_query, upsert=True)
        finally:
            client.close()
    
    def load_crawl_stock_info(self, collection_name: str, stock_info: Dict[str, List[str]]) -> None:
        """
        Load stock information into MongoDB.
        Performs upsert to avoid duplicate entries.
        """
        client = MongoClient(self.uri, **self.client_kwargs)
        try:
            db = client[self.database]
            collection = db[collection_name]
            for market_type, symbol_list in stock_info.items():
                filter_query = {"symbol_list": symbol_list}
                update_query = {"$set": {"market_type": market_type, "symbol_list": symbol_list}}
                collection.update_one(filter_query, update_query, upsert=True)
        except Exception as e:
            raise Exception(f"Error loading stock info into MongoDB: {e}") from e
        finally:
            client.close()
    
    def load_crawl_financial_info(self, collection: str,financial_data: List[Dict[str, Any]]) -> None:
        """
        Load financial information into MongoDB.
        Performs upsert to avoid duplicate entries.
        """
        client = MongoClient(self.uri, **self.client_kwargs)
        try:
            db = client[self.database]
            collection = db[collection]
            for financial in financial_data:
                filter_query = {"symbol": financial["symbol"]}
                update_query = {"$set": financial}
                collection.update_one(filter_query, update_query, upsert=True)
        except Exception as e:
            raise Exception(f"Error loading financial info into MongoDB: {e}") from e
        finally:
            client.close()
    
    def load_trading_data(self, collection: str, trading_data: List[Dict[str, Any]]) -> None:
        """
        Load trading data into MongoDB.
        Performs upsert to avoid duplicate entries.
        """
        client = MongoClient(self.uri, **self.client_kwargs)
        try:
            db = client[self.database]
            collection = db[collection]
            for trading in trading_data:
                filter_query = {"symbol": trading["symbol"], "date": trading["date"]}
                update_query = {"$set": trading}
                collection.update_one(filter_query, update_query, upsert=True)
        except Exception as e:
            raise Exception(f"Error loading trading data into MongoDB: {e}") from e
        finally:
            client.close()
    
    def load_detail_income_statement_yearly(self, collection: str,income_data: List[Dict[str, Any]]) -> None:
        """
        Load detailed income statement data into MongoDB.
        Performs upsert to avoid duplicate entries.
        """
        client = MongoClient(self.uri, **self.client_kwargs)
        try:
            db = client[self.database]
            collection = db[collection]
            for income in income_data:
                filter_query = {"symbol": income["symbol"], "report_date": income["report_date"]}
                update_query = {"$set": income}
                collection.update_one(filter_query, update_query, upsert=True)
        except Exception as e:
            raise Exception(f"Error loading income statement data into MongoDB: {e}") from e
        finally:
            client.close()
    
    def load_detail_income_statement_quarterly(self, collection: str, income_data: List[Dict[str, Any]]) -> None:
        """
        Load detailed quarterly income statement data into MongoDB.
        Performs upsert to avoid duplicate entries.
        """
        client = MongoClient(self.uri, **self.client_kwargs)
        try:
            db = client[self.database]
            collection = db[collection]
            for income in income_data:
                filter_query = {"symbol": income["symbol"], "report_date": income["report_date"]}
                update_query = {"$set": income}
                collection.update_one(filter_query, update_query, upsert=True)
        except Exception as e:
            raise Exception(f"Error loading quarterly income statement data into MongoDB: {e}") from e
        finally:
            client.close()
    
    def load_detail_balance_sheet_quarterly(self, collection: str, balance_data: List[Dict[str, Any]]) -> None:
        """
        Load detailed balance sheet data into MongoDB.
        Performs upsert to avoid duplicate entries.
        """
        client = MongoClient(self.uri, **self.client_kwargs)
        try:
            db = client[self.database]
            collection = db[collection]
            for balance in balance_data:
                filter_query = {"symbol": balance["symbol"], "report_date": balance["report_date"]}
                update_query = {"$set": balance}
                collection.update_one(filter_query, update_query, upsert=True)
        except Exception as e:
            raise Exception(f"Error loading balance sheet data into MongoDB: {e}") from e
        finally:
            client.close()
    
    def load_detail_balance_sheet_yearly(self, collection: str, balance_data: List[Dict[str, Any]]) -> None:
        """
        Load detailed balance sheet data into MongoDB.
        Performs upsert to avoid duplicate entries.
        """
        client = MongoClient(self.uri, **self.client_kwargs)
        try:
            db = client[self.database]
            collection = db[collection]
            for balance in balance_data:
                filter_query = {"symbol": balance["symbol"], "report_date": balance["report_date"]}
                update_query = {"$set": balance}
                collection.update_one(filter_query, update_query, upsert=True)
        except Exception as e:
            raise Exception(f"Error loading balance sheet data into MongoDB: {e}") from e
        finally:
            client.close()

    def load_details_match(self,collection: str, match_data: List[Dict[str, Any]]) -> None:
        """
        Load match details into MongoDB.
        Performs upsert to avoid duplicate entries.
        """
        client = MongoClient(self.uri, **self.client_kwargs)
        try:
            db = client[self.database]
            collection = db[collection]
            for match in match_data:
                filter_query = {"symbol": match["symbol"], "time": match["Time_match"], "price": match["Price_match"]}
                update_query = {"$set": match}
                collection.update_one(filter_query, update_query, upsert=True)
        except Exception as e:
            raise Exception(f"Error loading match details into MongoDB: {e}") from e
        finally:
            client.close()
    
    def load_business_plan(self, collection: str, business_data: List[Dict[str, Any]]) -> None:
        """
        Load business plan data into MongoDB.
        Performs upsert to avoid duplicate entries.
        """
        client = MongoClient(self.uri, **self.client_kwargs)
        try:
            db = client[self.database]
            collection = db[collection]
            for plan in business_data:
                filter_query = {"symbol": plan["symbol"]}
                update_query = {"$set": plan}
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
            for report in report_data:
                filter_query = {"symbol": report["symbol"], "report_date": report["report_date"]}
                update_query = {"$set": report}
                collection.update_one(filter_query, update_query, upsert=True)
        except Exception as e:
            raise Exception(f"Error loading financial report summary into MongoDB: {e}") from e
        finally:
            client.close()