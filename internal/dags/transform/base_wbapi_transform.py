import wbgapi as wb
from internal.config.load_config import load_config
from src.logger import FastLogger
from internal.dags.transform.transform_wbapi import TransformEconomy


class wbapi_transform:
    def __init__(self):
        self.economy = TransformEconomy()
        