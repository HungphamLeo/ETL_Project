import wbgapi as wb
from config.load_config import load_config
from src.logger import FastLogger
from dags.transform.transform_wbapi import TransformEconomy


class wbapi_transform:
    def __init__(self):
        self.economy = TransformEconomy()
        