import requests
import time
import re
from typing import Optional, List, Union
from bs4 import BeautifulSoup
from internal.models.cophieu68_model.extract_models import StockBasicInfo, PriceInfo
from src.logger import FastLogger
from cmd_.load_config import load_config


class Cophieu68BeautifulSoupCrawler:
    def __init__(self, pipeline_config, pipeline_logger):
        config = pipeline_config
        self.crawler_cfg = config["sources"]["cophieu68"]
        self.urls = self.crawler_cfg["base_url"]
        self.delay = config["http"].get("delay_seconds", 1.0)
        self.timeout =  config["http"].get("timeout_seconds", 30)
        self.session = requests.Session()
        self.session.headers.update(config["http"].get("headers", {}))
        self.logger = pipeline_logger


    def get_soup(self, url: str, retries: int = 3) -> Optional[BeautifulSoup]:
        for attempt in range(retries):
            try:
                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()
                response.encoding = "utf-8"
                soup = BeautifulSoup(response.text, "html.parser")
                time.sleep(self.delay)
                return soup
            except Exception as e:
                self.logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                continue
        self.logger.error(f"Failed to fetch {url} after {retries} attempts")
        return None

    def safe_extract_text(self, soup: BeautifulSoup, 
                                selector: str, 
                                multiple: bool = False) -> Union[str, List[str]]:
        """
        An toàn trích xuất text từ selector

        :param soup: Điểm khởi đầu để tìm kiếm
        :param selector: Chọn lọc để tìm kiếm
        :param multiple: Nếu True, trả về List[str], ngược lại trả về str
        :return: Text được trích xuất nếu thành công, ngược lại trả về rỗng
        """
        try:
            if multiple:
                return [el.get_text(strip=True) for el in soup.select(selector)]
            element = soup.select_one(selector)
            return element.get_text(strip=True) if element else ""
        except Exception:
            return [] if multiple else ""

    def extract_number(self, text: str) -> str:
        if not text:
            return ""
        return re.sub(r"[^\d.,\-]", "", text)

    def crawl_basic_info(self, symbol: str, price_info:PriceInfo) -> Optional[StockBasicInfo]:
        url = f"{self.urls['summary']}{symbol.upper()}"
        soup = self.get_soup(url)
        if not soup:
            return None

        try:
            h1_element = soup.find("h1")
            company_name = ""
            if h1_element:
                company_name = h1_element.get_text(strip=True)
                if "(" in company_name:
                    company_name = company_name.split("(")[0].strip()

            current_price = self.safe_extract_text(soup, price_info.current_price)
            price_change = self.safe_extract_text(soup, price_info.price_change) 
            percent_change = self.safe_extract_text(soup, price_info.percent_change)
            volume = self.safe_extract_text(soup, price_info.volume) 
            highest = self.safe_extract_text(soup, price_info.high_price)
            lowest = self.safe_extract_text(soup, price_info.low_price)

            reference_price = ""
            open_price = ""
            flex_detail_divs = soup.select(".flex_detail")
            if len(flex_detail_divs) >= 2:
                value_div = flex_detail_divs[1]
                value_elements = value_div.find_all("div")
                if len(value_elements) >= 2:
                    reference_price = value_elements[0].get_text(strip=True)
                    open_price = value_elements[1].get_text(strip=True)

            return StockBasicInfo(
                symbol=symbol.upper(),
                company_name=company_name,
                current_price=current_price,
                price_change=price_change,
                percent_change=percent_change,
                reference_price=reference_price,
                open_price=open_price,
                high_price=highest,
                low_price=lowest,
                volume=volume,
                timestamp=str(int(time.time()))
            )
        except Exception as e:
            self.logger.error(f"Error extracting basic info for {symbol}: {e}")
            return None
