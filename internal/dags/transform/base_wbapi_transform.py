import wbgapi as wb
from internal.config.load_config import load_config
from src.logger import FastLogger
from internal.dags.transform.transform_wbapi import (TransformEconomy, TransformTopic, 
                                                    TransformSeries, TransformTime, 
                                                    TransformSource, TransformRegion, 
                                                    TransformIncome, TransformLending)


class wbapi_transform:
    def __init__(self):
        self.economy = TransformEconomy()
        self.topic = TransformTopic()
        self.series = TransformSeries()
        self.time = TransformTime()
        self.source = TransformSource()
        self.region = TransformRegion() 
        self.income = TransformIncome()
        self.lending = TransformLending()
        
        
        