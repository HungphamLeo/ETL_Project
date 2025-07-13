
import os
import sys
from dataclasses import fields, asdict
import pandas as pd

def dataframe_rename_by_dataclass(df: pd.DataFrame, output_cls) -> pd.DataFrame:
    df = df.copy()
    field_names = [f.name for f in fields(output_cls)]
    
    if len(df.columns) != len(field_names):
        raise ValueError("Column count mismatch between DataFrame and dataclass")
    
    df.columns = field_names
    return df

def parse_metadata_to_df(meta_obj, output_cls) -> pd.DataFrame:
    # Convert the metadata attributes into a dict
    data = output_cls(
        IndicatorName = meta_obj['IndicatorName'],
        LongDefinition = meta_obj['Longdefinition'],
        Topic = meta_obj['Topic'],
        AggregationMethod = meta_obj['Aggregationmethod'],
        Periodicity = meta_obj['Periodicity'],
        UnitOfMeasure = meta_obj['Unitofmeasure'],
        Source = meta_obj['Source'],
        License_Type = meta_obj['License_Type'],
        License_URL = meta_obj['License_URL'],
        DevelopmentRelevance = meta_obj['Developmentrelevance'],
        GeneralComments = meta_obj['Generalcomments'],
        LimitationsAndExceptions = meta_obj['Limitationsandexceptions'],
        StatisticalConceptAndMethodology = meta_obj['Statisticalconceptandmethodology'],
    )
    return pd.DataFrame([asdict(data)])