from bs4 import BeautifulSoup
import requests
import re
import time
from typing import Optional
from src.logger import FastLogger
from cmd_.load_config import load_config
from internal.models.cophieu68_model.extract_models import StockBasicInfo, CompleteStockData
from internal.models.cophieu68_model.extract_models import *
from internal.dags.cophieu68_dag.extract.base_extract import Cophieu68BeautifulSoupCrawler

class extract_cophieu68(Cophieu68BeautifulSoupCrawler):
    def __init__(self):
        super().__init__()
        # Load configuration for URLs and other settings
    def crawl_financial_report(self, symbol: str, report_type: str) -> Optional[List[StockFinancialReport]]:
        """
        Hàm generic crawl báo cáo tài chính.
        report_type: 'income' | 'balance' | 'cashflow'
        Trả về list StockFinancialReport (mỗi bảng là 1 DataFrame wrap lại).
        """
        url = f"{self.urls['financial']}{symbol.upper()}&type={report_type}"
        try:
            self.logger.info(f"Fetching financial report: {report_type} for {symbol.upper()}")
            tables = pd.read_html(url, flavor="lxml")
            
            reports = []
            for i, df in enumerate(tables):
                reports.append(
                    StockFinancialReport(
                        symbol=symbol.upper(),
                        report_type=report_type,
                        table_index=i,
                        data=df
                    )
                )
            return reports
        except Exception as e:
            self.logger.error(f"Error fetching {report_type} report for {symbol.upper()}: {e}")
            return None

    def crawl_income_statement(self, symbol: str) -> Optional[List[IncomeStatementReport]]:
        """Crawl báo cáo kết quả kinh doanh"""
        return self.crawl_financial_report(symbol, "income")

    def crawl_balance_sheet(self, symbol: str) -> Optional[List[BalanceSheetReport]]:
        """Crawl bảng cân đối kế toán"""
        return self.crawl_financial_report(symbol, "balance")

    def crawl_cashflow_statement(self, symbol: str) -> Optional[List[CashflowStatementReport]]:
        """Crawl báo cáo lưu chuyển tiền tệ"""
        return self.crawl_financial_report(symbol, "cashflow")

    def crawl_financial_ratios(self, symbol: str, soup: BeautifulSoup = None) -> Optional[StockFinancialRatios]:
        if not soup:
            url = f"{self.urls['summary']}{symbol.upper()}"
            soup = self.get_soup(url)

        if not soup:
            return None

        try:
            self.logger.info(f"Extracting financial ratios for {symbol.upper()}")
            ratios = StockFinancialRatios(symbol=symbol.upper())
            flex_rows = soup.select(".flex_row")

            for row in flex_rows:
                label_div = row.select_one(".flex_detail")
                value_div = row.select_one(".flex_detail.bold")

                if not label_div or not value_div:
                    continue

                labels = [div.get_text(strip=True).lower() for div in label_div.find_all('div')]
                values = [div.get_text(strip=True) for div in value_div.find_all('div')]

                for i, label in enumerate(labels):
                    if i >= len(values):
                        continue
                    value = values[i]

                    for pattern, field_name in FINANCIAL_MAPPING.items():
                        if re.search(pattern, label, re.IGNORECASE):
                            # Special handling cho ROA/ROE (format có thể có "#")
                            if field_name == "roa" and "#" in value:
                                setattr(ratios, field_name, value.split("#")[0].strip())
                            elif field_name == "roe" and "#" in value:
                                setattr(ratios, field_name, value.split("#")[-1].strip())
                            else:
                                setattr(ratios, field_name, value)
                            break

            return ratios

        except Exception as e:
            self.logger.error(f"Error extracting financial ratios for {symbol}: {e}")
            return None
    
    def crawl_power_ratings(self, symbol: str, soup: BeautifulSoup = None) -> Optional[StockPowerRatings]:
        """Crawl sức mạnh các chỉ số"""
        if not soup:
            url = f"{self.urls['summary']}{symbol.upper()}"
            soup = self.get_soup(url)
        
        if not soup:
            return None
        
        try:
            self.logger.info(f"Extracting power ratings for {symbol.upper()}")
            
            power_ratings = StockPowerRatings(symbol=symbol.upper())
            
            # Tìm section có icon bolt (fa-bolt)
            flex_rows = soup.select(".flex_row")
            
            for row in flex_rows:
                if "fa-bolt" in str(row) or "fa-solid fa-bolt" in str(row):
                    try:
                        # Extract percentages từ text
                        row_text = row.get_text()
                        percentages = re.findall(r'(\d+)%', row_text)
                        
                        if len(percentages) >= 5:
                            power_ratings.eps_power = f"{percentages[0]}%"
                            power_ratings.roe_power = f"{percentages[1]}%"
                            power_ratings.pb_power = f"{percentages[3]}%"
                            power_ratings.price_growth_power = f"{percentages[4]}%"
                        
                        # Đặc biệt cho đầu tư hiệu quả (rating sao)
                        star_elements = row.select(".fa-star")
                        if star_elements:
                            # Đếm số sao có màu xanh
                            filled_stars = len([star for star in star_elements 
                                             if "color: #006600" in star.get('style', '')])
                            power_ratings.investment_efficiency = f"{filled_stars}/5 stars"
                        elif len(percentages) >= 3:
                            power_ratings.investment_efficiency = f"{percentages[2]}%"
                        
                        break
                        
                    except Exception:
                        continue
            
            return power_ratings
            
        except Exception as e:
            self.logger.error(f" Error extracting power ratings for {symbol}: {e}")
            return None
    
    def crawl_trading_data(self, symbol: str, soup: BeautifulSoup = None) -> Optional[TradingData]:
        """Crawl dữ liệu giao dịch"""

        if not soup:
            url = f"{self.urls['summary']}{symbol.upper()}"
            soup = self.get_soup(url)
        if not soup:
            return None

        try:
            self.logger.info(f"Extracting trading data for {symbol.upper()}")
            trading_data = TradingData(symbol=symbol.upper())

            # Tìm bảng giao dịch theo keyword
            tables = soup.find_all('table')
            trading_table = next(
                (t for t in tables if all(kw in t.get_text() for kw in cfg["table_identifiers"])),
                None
            )

            if trading_table:
                rows = trading_table.find_all('tr')[1: CRAWL_TRADING_DATA_CONFIG["max_rows"]+1]

                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 4:
                        buy_price, buy_volume, sell_price, sell_volume = [c.get_text(strip=True) for c in cells[:4]]

                        if buy_price and buy_volume:
                            trading_data.buy_orders.append({"price": buy_price, "volume": buy_volume})
                        if sell_price and sell_volume:
                            trading_data.sell_orders.append({"price": sell_price, "volume": sell_volume})

                # Nước ngoài
                fb = soup.select_one(CRAWL_TRADING_DATA_CONFIG["foreign_buy_selector"])
                fs = soup.select_one(CRAWL_TRADING_DATA_CONFIG["foreign_sell_selector"])
                if fb: trading_data.foreign_buy = fb.get_text(strip=True)
                if fs: trading_data.foreign_sell = fs.get_text(strip=True)

            return trading_data

        except Exception as e:
            self.logger.error(f"Error extracting trading data for {symbol}: {e}")
            return None

    
    
    def crawl_business_plan(self, symbol: str, soup: BeautifulSoup = None) -> List[BusinessPlan]:
        """Crawl kế hoạch kinh doanh"""

        if not soup:
            url = f"{self.urls['summary']}{symbol.upper()}"
            soup = self.get_soup(url)
        if not soup:
            return []

        try:
            self.logger.info(f"Extracting business plan for {symbol.upper()}")
            plans = []

            business_plan_div = soup.find('div', {'id': CRAWL_BUSINESS_PLAN_CONFIG["container_id"]})
            if not business_plan_div:
                return []

            table = business_plan_div.find('table')
            if not table:
                return []

            rows = table.find_all('tr')[1:]
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= CRAWL_BUSINESS_PLAN_CONFIG["min_columns"]:
                    year, revenue_plan, revenue_achievement, profit_plan, profit_achievement = \
                        [c.get_text(strip=True) for c in cells[:5]]

                    plans.append(BusinessPlan(
                        symbol=symbol.upper(),
                        year=year,
                        revenue_plan=revenue_plan,
                        revenue_achievement=revenue_achievement,
                        profit_plan=profit_plan,
                        profit_achievement=profit_achievement
                    ))
            return plans

        except Exception as e:
            self.logger.error(f"Error extracting business plan for {symbol}: {e}")
            return []

    
    def crawl_industry_info(self, symbol: str, soup: BeautifulSoup = None) -> Optional[IndustryInfo]:
        """Crawl thông tin ngành"""
        
        if not soup:
            url = f"{self.urls['summary']}{symbol.upper()}"
            soup = self.get_soup(url)
        if not soup:
            return None

        try:
            self.logger.info(f"Extracting industry info for {symbol.upper()}")
            industry_info = IndustryInfo(symbol=symbol.upper())

            h2_elements = soup.find_all('h2')
            for h2 in h2_elements:
                if CRAWL_INDUSTRY_INFO_CONFIG["header_text"] in h2.get_text():
                    table = h2.find_next_sibling().find('table')
                    if table:
                        cell = table.find('td')
                        if cell:
                            lines = cell.get_text(strip=True).split('\n')
                            if len(lines) >= 1:
                                industry_info.market_name = lines[0].strip()
                            if len(lines) >= 2:
                                industry_name = lines[1].strip()
                                if CRAWL_INDUSTRY_INFO_CONFIG["strip_parentheses"] and '(' in industry_name:
                                    industry_name = industry_name.split('(')[0].strip()
                                industry_info.industry_name = industry_name
                    break

            return industry_info

        except Exception as e:
            self.logger.error(f"Error extracting industry info for {symbol}: {e}")
            return None

    
    def crawl_company_profile(self, symbol: str) -> Optional[CompanyProfile]:
        """Crawl thông tin chi tiết công ty từ trang profile"""

        url = f"{self.urls['profile']}{symbol.upper()}"
        soup = self.get_soup(url)
        if not soup:
            return None

        try:
            self.logger.info(f"Extracting company profile for {symbol.upper()}")
            profile = CompanyProfile(symbol=symbol.upper())

            field_map = CRAWL_COMPANY_PROFILE_CONFIG["field_map"]

            tables = soup.find_all('table')
            for table in tables:
                for row in table.find_all('tr'):
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        label = cells[0].get_text(strip=True).lower()
                        value = cells[1].get_text(strip=True)
                        
                        # match key in field_map
                        for key, attr in field_map.items():
                            if key in label:
                                setattr(profile, attr, value)
                                break

            return profile

        except Exception as e:
            self.logger.error(f"Error extracting company profile for {symbol}: {e}")
            return None


    
    def crawl_complete_stock_data(self, symbol: str) -> CompleteStockData:
        """Crawl tất cả dữ liệu của một cổ phiếu"""

        self.logger.info(f"🎯 Starting complete crawl for {symbol.upper()}")
        complete_data = CompleteStockData()

        # summary soup (dùng lại cho submodules)
        summary_url = f"{self.urls['summary']}{symbol.upper()}"
        soup = self.get_soup(summary_url)

        if soup:
            for method_name in CRAWL_COMPLETE_STOCK_CONFIG["summary_submodules"]:
                method = getattr(self, method_name, None)
                if method:
                    try:
                        setattr(complete_data, method_name.replace("crawl_", ""), method(symbol, soup))
                    except Exception as e:
                        self.logger.error(f"Error in {method_name} for {symbol}: {e}")

        # crawl profile riêng
        profile_method = getattr(self, CRAWL_COMPLETE_STOCK_CONFIG["profile_module"], None)
        if profile_method:
            try:
                complete_data.company_profile = profile_method(symbol)
            except Exception as e:
                self.logger.error(f"Error in company profile for {symbol}: {e}")

        self.logger.info(f"Completed crawl for {symbol.upper()}")
        return complete_data

    
    def crawl_multiple_stocks(self, symbols: List[str], max_workers: int = None) -> Dict[str, CompleteStockData]:
        """Crawl nhiều cổ phiếu song song"""
    
        from concurrent import futures
        import concurrent.futures
        if max_workers is None:
            max_workers = CRAWL_MULTIPLE_STOCKS_CONFIG["max_workers_default"]

        self.logger.info(f"Starting batch crawl for {len(symbols)} symbols with {max_workers} workers")
        results = {}

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_symbol = {
                executor.submit(self.crawl_complete_stock_data, symbol): symbol 
                for symbol in symbols
            }

            for future in concurrent.futures.as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    data = future.result()
                    results[symbol.upper()] = data
                    self.logger.info(f"Completed {symbol.upper()}")
                except Exception as e:
                    self.logger.error(f"Error crawling {symbol}: {e}")
                    results[symbol.upper()] = CompleteStockData()

        self.logger.info(f"🎉 Batch crawl completed: {len(results)} symbols processed")
        return results

        
    def crawl_market_list(self, market_type: str = "all") -> List[str]:
        """Crawl danh sách mã cổ phiếu từ thị trường"""
       

        url = f"{self.urls['market_data']}?market={market_type}"
        soup = self.get_soup(url)
        if not soup:
            return []

        try:
            self.logger.info(f"Extracting stock list from market: {market_type}")
            symbols = []

            # Lấy link theo pattern từ config
            links = soup.find_all('a', href=re.compile(CRAWL_MARKET_LIST_CONFIG["link_pattern"]))
            for link in links:
                href = link.get('href', '')
                match = re.search(CRAWL_MARKET_LIST_CONFIG["symbol_regex"], href)
                if match:
                    symbol = match.group(1).upper()
                    if symbol not in symbols:
                        symbols.append(symbol)

            self.logger.info(f"Found {len(symbols)} symbols in {market_type} market")
            return symbols

        except Exception as e:
            self.logger.error(f"Error extracting market list: {e}")
            return []

    
    def crawl_industry_list(self) -> List[Dict[str, str]]:
        """Crawl danh sách ngành"""
        
        from urllib.parse import urljoin

        url = self.urls['categories']
        soup = self.get_soup(url)
        if not soup:
            return []

        try:
            self.logger.info("🏭 Extracting industry list")
            industries = []

            # Lấy link theo pattern từ config
            links = soup.find_all('a', href=re.compile(CRAWL_INDUSTRY_LIST_CONFIG["link_pattern"]))

            for link in links[:CRAWL_INDUSTRY_LIST_CONFIG["limit"]]:
                try:
                    industry_name = link.get_text(strip=True)
                    industry_url = link.get('href', '')

                    if industry_name and industry_url:
                        if not industry_url.startswith('http'):
                            industry_url = urljoin(self.urls['base_url'], industry_url)

                        industries.append({
                            "name": industry_name,
                            "url": industry_url
                        })
                except Exception:
                    continue

            self.logger.info(f"Found {len(industries)} industries")
            return industries

        except Exception as e:
            self.logger.error(f"Error extracting industry list: {e}")
            return []

    
    # def save_results(self, data: Dict[str, CompleteStockData], output_dir: str = "results"):
    #     """Lưu kết quả ra files"""
    #     import os
        
    #     os.makedirs(output_dir, exist_ok=True)
        
    #     # Lưu từng loại dữ liệu riêng
    #     basic_info_list = []
    #     financial_ratios_list = []
    #     balance_sheet_list = []
    #     power_ratings_list = []
    #     trading_data_list = []
    #     financial_statements_list = []
    #     business_plans_list = []
    #     industry_info_list = []
    #     company_profiles_list = []
        
    #     for symbol, stock_data in data.items():
    #         if stock_data.basic_info:
    #             basic_info_list.append(asdict(stock_data.basic_info))
            
    #         if stock_data.financial_ratios:
    #             financial_ratios_list.append(asdict(stock_data.financial_ratios))
            
    #         if stock_data.balance_sheet:
    #             balance_sheet_list.append(asdict(stock_data.balance_sheet))
            
    #         if stock_data.power_ratings:
    #             power_ratings_list.append(asdict(stock_data.power_ratings))
            
    #         if stock_data.trading_data:
    #             trading_data_list.append(asdict(stock_data.trading_data))
            
    #         if stock_data.financial_statements:
    #             for stmt in stock_data.financial_statements:
    #                 financial_statements_list.append(asdict(stmt))
            
    #         if stock_data.business_plans:
    #             for plan in stock_data.business_plans:
    #                 business_plans_list.append(asdict(plan))
            
    #         if stock_data.industry_info:
    #             industry_info_list.append(asdict(stock_data.industry_info))
            
    #         if stock_data.company_profile:
    #             company_profiles_list.append(asdict(stock_data.company_profile))
        
    #     # Lưu ra CSV files
    #     datasets = {
    #         "basic_info": basic_info_list,
    #         "financial_ratios": financial_ratios_list,
    #         "balance_sheet": balance_sheet_list,
    #         "power_ratings": power_ratings_list,
    #         "trading_data": trading_data_list,
    #         "financial_statements": financial_statements_list,
    #         "business_plans": business_plans_list,
    #         "industry_info": industry_info_list,
    #         "company_profiles": company_profiles_list
    #     }
        
    #     for name, dataset in datasets.items():
    #         if dataset:
    #             df = pd.DataFrame(dataset)
    #             csv_path = os.path.join(output_dir, f"{name}.csv")
    #             df.to_csv(csv_path, index=False, encoding='utf-8')
    #             logger.info(f"💾 Saved {len(dataset)} records to {csv_path}")
        
    #     # Lưu raw data dạng JSON
    #     json_data = {}
    #     for symbol, stock_data in data.items():
    #         json_data[symbol] = {
    #             "basic_info": asdict(stock_data.basic_info) if stock_data.basic_info else None,
    #             "financial_ratios": asdict(stock_data.financial_ratios) if stock_data.financial_ratios else None,
    #             "balance_sheet": asdict(stock_data.balance_sheet) if stock_data.balance_sheet else None,
    #             "power_ratings": asdict(stock_data.power_ratings) if stock_data.power_ratings else None,
    #             "trading_data": asdict(stock_data.trading_data) if stock_data.trading_data else None,
    #             "financial_statements": [asdict(stmt) for stmt in stock_data.financial_statements],
    #             "business_plans": [asdict(plan) for plan in stock_data.business_plans],
    #             "industry_info": asdict(stock_data.industry_info) if stock_data.industry_info else None,
    #             "company_profile": asdict(stock_data.company_profile) if stock_data.company_profile else None
    #         }
        
    #     json_path = os.path.join(output_dir, "complete_data.json")
    #     with open(json_path, 'w', encoding='utf-8') as f:
    #         json.dump(json_data, f, ensure_ascii=False, indent=2)
        
    #     logger.info(f"💾 Saved complete data to {json_path}")