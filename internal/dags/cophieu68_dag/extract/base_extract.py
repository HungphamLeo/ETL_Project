import requests
import time
import re
from typing import Optional, List, Union
from bs4 import BeautifulSoup
from internal.models.cophieu68_model.extract_models import StockBasicInfo



class Cophieu68BeautifulSoupCrawler:
    def __init__(self, pipeline_config, pipeline_logger):
        config = pipeline_config
        self.crawler_cfg = config["sources"]["cophieu68"]
        self.urls = self.crawler_cfg["base_url"]
        self.delay = config["http"].get("delay_seconds", 0.2)
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
                else:
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

