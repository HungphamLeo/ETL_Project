import requests
import time
import re
from typing import Optional, List, Union
from bs4 import BeautifulSoup
from internal.models.cophieu68_model.extract_models import StockBasicInfo
from src.logger import FastLogger
from cmd_.load_config import load_config


class Cophieu68BeautifulSoupCrawler:
    def __init__(self):
        config = load_config()
        crawler_cfg = config["crawler"]
        self.urls = crawler_cfg["sources_crawl"]["name"]["urls"] if crawler_cfg["sources_crawl"]["name"] == "cophieu68" else crawler_cfg["sources_crawl"]["urls"]
        self.delay = crawler_cfg.get("delay", 1.0)
        self.timeout = crawler_cfg.get("timeout", 30)
        self.session = requests.Session()
        self.session.headers.update(crawler_cfg.get("headers", {}))
        self.logger = FastLogger(config).get_logger()
        self.logger.info("Cophieu68 BeautifulSoup Crawler initialized")

    def get_soup(self, url: str, retries: int = 3) -> Optional[BeautifulSoup]:
        for attempt in range(retries):
            try:
                self.logger.info(f"Fetching: {url} (attempt {attempt + 1})")
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

    def safe_extract_text(self, soup: BeautifulSoup, selector: str, multiple: bool = False) -> Union[str, List[str]]:
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

    def crawl_basic_info(self, symbol: str) -> Optional[StockBasicInfo]:
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

            current_price = self.safe_extract_text(soup, "#stockname_close")
            price_change = self.safe_extract_text(soup, "#stockname_price_change")
            percent_change = self.safe_extract_text(soup, "#stockname_percent_change")
            volume = self.safe_extract_text(soup, "#stockname_volume")
            highest = self.safe_extract_text(soup, "#stockname_price_highest")
            lowest = self.safe_extract_text(soup, "#stockname_price_lowest")

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
