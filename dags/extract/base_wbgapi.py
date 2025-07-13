import wbgapi as wb
from config.load_config import load_config
from src.logger import setup_logging
from dags.extract.wbapi_extract import (
    wbapi_series, wbapi_economy, wbapi_topic,
    wbapi_time, wbapi_source, wbapi_region,
    wbapi_income, wbapi_lending
)

class wbapi_main:
    def __init__(self):
        self.series = wbapi_series()
        self.economy = wbapi_economy()
        self.topic = wbapi_topic()
        self.time = wbapi_time()
        self.source = wbapi_source()
        self.region = wbapi_region()
        self.income = wbapi_income()
        self.lending = wbapi_lending()
        self.logger = setup_logging(load_config(), 'wbgapi_main_topic')
    


