import pandas as pd
from dataclasses import dataclass, asdict
from typing import Optional


@dataclass
class SeriesFetchOutput:
    ID: str
    Topic_Name: str

@dataclass
class SeriesMetadataOutput:
    IndicatorName: str
    Longdefinition: str
    Topic: str
    Aggregationmethod: str
    Periodicity: str
    Unitofmeasure: str
    Source: str
    License_Type: str
    License_URL: str
    Developmentrelevance: str
    Generalcomments: str
    Limitationsandexceptions: str
    Statisticalconceptandmethodology: str