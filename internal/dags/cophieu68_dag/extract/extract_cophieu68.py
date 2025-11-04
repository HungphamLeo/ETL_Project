from bs4 import BeautifulSoup
import re
import time
from typing import Optional, List
import pandas as pd
import json
from dataclasses import asdict
from internal.models.cophieu68_model.extract_models import StockBasicInfo, CompleteStockData
from internal.models.cophieu68_model.extract_models import *
from internal.dags.cophieu68_dag.extract.base_extract import Cophieu68BeautifulSoupCrawler

class extract_cophieu68(Cophieu68BeautifulSoupCrawler):
    def __init__(self, pipeline_config=None, pipeline_logger=None):
        if pipeline_config is not None or pipeline_logger is not None:
            super().__init__(pipeline_config, pipeline_logger)
        else:
            super().__init__()
        self.endpoint = self.crawler_cfg["endpoints"]
        
    def crawl_financial_report(self, symbol: str, report_type: str) -> Optional[List[StockFinancialReport]]:
        """
        Hàm generic crawl báo cáo tài chính.
        report_type: 'income' | 'balance' | 'cashflow'
        Trả về list StockFinancialReport (mỗi bảng là 1 DataFrame wrap lại).
        """

        url = f"{self.urls}{self.endpoint['summary_financial']}".format(symbol=symbol.upper())
        try:
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
        try:
            reports = self.crawl_financial_report(symbol, "income")
            if reports:
                return [IncomeStatementReport(**report.__dict__) for report in reports]
        except Exception as e:
            self.logger.error(f"Error processing income statement for {symbol}: {e}")
            return None
        

    def crawl_balance_sheet(self, symbol: str) -> Optional[List[BalanceSheetReport]]:
        """Crawl bảng cân đối kế toán"""
        try:
            reports = self.crawl_financial_report(symbol, "balance")
            if reports:
                return [BalanceSheetReport(**report.__dict__) for report in reports]
        except Exception as e:
            self.logger.error(f"Error processing balance sheet for {symbol}: {e}")
            return None
        

    def crawl_cashflow_statement(self, symbol: str) -> Optional[List[CashflowStatementReport]]:
        """Crawl báo cáo lưu chuyển tiền tệ"""
        try:
            reports = self.crawl_financial_report(symbol, "cashflow")
            if reports:
                return [CashflowStatementReport(**report.__dict__) for report in reports]
        except Exception as e:
            self.logger.error(f"Error processing cashflow statement for {symbol}: {e}")
            return None


    def crawl_industry_info(self, type_info: str) -> Optional[pd.DataFrame]:
        """Crawl bảng thông tin ngành (Giá TB, Giá sổ sách, EPS, PE, ROA, ROE)"""
        if type_info not in INDUSTRIAL_INFO_TYPE:
            self.logger.error(f"Invalid industry type: {type_info}")
            return None

        url = None
        if type_info == "summary_info":
            url = f"{self.urls}{self.endpoint['stock_category'][0]}"
        else:
            sub = INDUSTRIAL_INFO_TYPE[type_info]
            url = f"{self.urls}{self.endpoint['stock_category'][1]}?sub={sub}"
        self.logger.info(f"Crawling industry info for type {type_info} from {url}")
        soup = self.get_soup(url)
        if not soup:
            return None

        try:
            table = soup.select_one("table.table_content")
            if not table:
                raise ValueError("Không tìm thấy bảng dữ liệu ngành trong HTML.")

            rows = []
            sub = INDUSTRIAL_INFO_TYPE[type_info]

            for tr in table.select("tr.border_bottom"):
                tds = tr.find_all("td")
                if not tds:
                    continue

                a_tag = tds[0].select_one("a[href]")
                if not a_tag:
                    continue

                code_tag = a_tag.select_one("div:nth-of-type(1)")
                name_tag = a_tag.select_one("div:nth-of-type(2)")
                if not (code_tag and name_tag):
                    continue

                industry_code = code_tag.get_text(strip=True)
                industry_name = name_tag.get_text(strip=True)
                industry_url = a_tag["href"]

                def get_text_safe(idx):
                    return tds[idx].get_text(strip=True) if len(tds) > idx else None

                # match-case: loại bảng
                match sub:
                    case 0:
                        row = IndustrySummaryInfo(
                            industry_code=industry_code,
                            industry_name=industry_name,
                            industry_url=industry_url,
                            index=get_text_safe(1),
                            change=get_text_safe(2),
                            liquidity=get_text_safe(3),
                            capital=get_text_safe(4),
                        )
                    case 1:
                        row = IndustryFinancialInfo(
                            industry_code=industry_code,
                            industry_name=industry_name,
                            industry_url=industry_url,
                            avg_price=get_text_safe(1),
                            book_value=get_text_safe(2),
                            eps=get_text_safe(3),
                            pe=get_text_safe(4),
                            roa=get_text_safe(5),
                            roe=get_text_safe(6),
                        )
                    case 2:
                        row = IndustryCapitalInfo(
                            industry_code=industry_code,
                            industry_name=industry_name,
                            industry_url=industry_url,
                            total_asset=get_text_safe(1),
                            total_equity=get_text_safe(2),
                            total_liabilities=get_text_safe(3),
                            percentage_debt_on_equity=get_text_safe(4),
                            percentage_equity_on_assets=get_text_safe(5),
                            revenue=get_text_safe(6),
                            profit_before_tax=get_text_safe(7),
                        )
                rows.append(row.__dict__)

            df = pd.DataFrame(rows)
            return df

        except Exception as e:
            self.logger.error(f"Error extracting industry info for type {type_info}: {e}")
            return None


    def crawl_financial_ratios(self, symbol: str, soup: BeautifulSoup = None) -> Optional[StockFinancialRatios]:
            """Crawl bảng tóm tắt các chỉ tiêu tài chính trên trang chi tiết cổ phiếu"""
            

            if not soup:
                url = f"{self.urls}{self.endpoint['summary_financial']}".format(symbol=symbol.upper())
                soup = self.get_soup(url)
            if not soup:
                return None

            try:
                table = soup.select_one("#financial_brief")
                if not table:
                    raise ValueError("Không tìm thấy bảng tài chính (#financial_brief).")

                header_cells = table.select("tr.tr_header td")[1:]
                periods = [h.get_text(strip=True) for h in header_cells]

                rows = []
                for tr in table.select("tr"):
                    tds = tr.find_all("td")
                    if len(tds) <= 1:
                        continue

                    label = tds[0].get_text(strip=True)
                    values = [td.get_text(strip=True) for td in tds[1:]]
                    field = None

                    for pattern, mapped in FINANCIAL_MAPPING.items():
                        if re.search(pattern, label, re.IGNORECASE):
                            field = mapped
                            break

                    row = {"metric": label, "field": field or "unknown"}
                    for i, p in enumerate(periods):
                        row[p] = values[i] if i < len(values) else None
                    rows.append(row)

                df = pd.DataFrame(rows)
                ratios = StockFinancialRatios(symbol=symbol.upper())
                ratios.data = df  # Nếu muốn lưu DataFrame trực tiếp
                return ratios

            except Exception as e:
                self.logger.error(f"Error extracting financial ratios for {symbol}: {e}")
                return None

        
    def crawl_trading_data(self, symbol: str, page: Optional[int] = None) -> Optional[str]:
        """Crawl dữ liệu lịch sử giao dịch cổ phiếu và trả về JSON"""
        all_rows = []
        page_num = 1 if page is None else page

        while True:
            try:
                url = f"{self.urls}{self.endpoint['trading_data']}".format(page=page_num, symbol=symbol.upper())
                soup = self.get_soup(url)
                if soup is None:
                    self.logger.warning(f"Không lấy được dữ liệu trang {page_num} cho {symbol}")
                    break

                table = soup.find("table", {"id": "history"})
                if not table:
                    self.logger.warning(f"Không tìm thấy bảng lịch sử giao dịch trên trang {page_num}")
                    break

                rows = []
                for tr in table.find_all("tr")[1:]:
                    tds = [td.get_text(strip=True).replace(",", "").replace("\xa0", "") for td in tr.find_all("td")]
                    if len(tds) == 9:
                        rows.append(tds)

                if not rows:
                    self.logger.info(f"Hết dữ liệu ở trang {page_num}")
                    break

                all_rows.extend(rows)
                self.logger.info(f"Đã crawl {len(rows)} dòng dữ liệu từ trang {page_num}")

                if page is not None:
                    break

                page_num += 1
                time.sleep(self.delay)

            except Exception as e:
                self.logger.error(f"Lỗi khi crawl dữ liệu {symbol} trang {page_num}: {e}")
                break

        if not all_rows:
            return None

        records = []
        for r in all_rows:
            try:
                record = TradingRecord(
                    date=r[0],
                    close_price=float(r[1]),
                    volume=int(float(r[2])),
                    open_price=float(r[3]),
                    high_price=float(r[4]),
                    low_price=float(r[5]),
                    foreign_buy=int(float(r[6])),
                    foreign_sell=int(float(r[7])),
                    foreign_value=float(r[8])
                )
                records.append(record)
            except Exception as e:
                self.logger.warning(f"Lỗi parse dòng dữ liệu {r}: {e}")

        return json.dumps({
            "symbol": symbol.upper(),
            "records": [asdict(r) for r in records]
        }, ensure_ascii=False, indent=2)

    
    def crawl_company_profile(self, symbol: str) -> Optional[CompanyProfile]:
        """Crawl thông tin chi tiết công ty từ trang profile"""

        url = f"{self.urls}{self.endpoint['company_profile']}".format(symbol=symbol.upper())
        soup = self.get_soup(url)
        if not soup:
            return None

        try:
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

        
    def crawl_market_list(self, market_type: str) -> List[str]:
        """
        Crawl danh sách mã cổ phiếu từ thị trường
        
        :param market_type: Loại thị trường (VN_INDEX, HOSE, HNX, UPCOM)
        :return: Danh sách mã cổ phiếu
        """
        if market_type not in CRAWL_MARKET_LIST_CONFIG:
            self.logger.error(f"Invalid market type: {market_type}")
            return []
        url = f"{self.urls}{self.endpoint['market_list']}?id=^{market_type}"
        print(f"URL: {url}")
        soup = self.get_soup(url)
        if not soup:
            return []

        try:
            symbols = []
            # Find all table rows with class "stock_online"
            for tr in soup.find_all("tr", class_="stock_online"):
                # Get the stock code from the "data-id" attribute
                code = tr.get("data-id")
                if code:
                    symbols.append(code.upper())
            return symbols

        except Exception as e:
            self.logger.error(f"Error extracting market list: {e}")
            return []



    
   