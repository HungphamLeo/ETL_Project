import pandas as pd
from dataclasses import dataclass, asdict
from typing import Optional


@dataclass
class SeriesFetchOutput:
    ID: str
    Topic_Name: str

@dataclass
class SeriesMetadataOutput:
    Aggregationmethod: Optional[str]
    Dataset: Optional[str]
    Developmentrelevance: Optional[str]
    Generalcomments: Optional[str]
    IndicatorName: Optional[str]
    License_Type: Optional[str]
    License_URL: Optional[str]
    Limitationsandexceptions: Optional[str]
    Longdefinition: Optional[str]
    Periodicity: Optional[str]
    Source: Optional[str]
    Statisticalconceptandmethodology: Optional[str]
    Topic: Optional[str]
    Unitofmeasure: Optional[str]

@dataclass
class SeriesInfoOutput:
    ID: str
    Value: str

@dataclass
class SeriesDataOutput:
    ID: str
    Value: str