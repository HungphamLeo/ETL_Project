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
    LongDefinition: str
    Topic: str
    AggregationMethod: str
    Periodicity: str
    UnitOfMeasure: str
    Source: str
    License_Type: str
    License_URL: str
    DevelopmentRelevance: str
    GeneralComments: str
    LimitationsAndExceptions: str
    StatisticalConceptAndMethodology: str