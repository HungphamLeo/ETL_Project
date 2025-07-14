
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