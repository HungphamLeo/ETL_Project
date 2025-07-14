
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

def auto_parse_metadata_to_df(meta_obj, cls):
    meta_dict = {}
    meta_data = meta_obj.__dict__  # <-- dùng __dict__ để lấy tất cả thuộc tính

    for f in fields(cls):
        meta_dict[f.name] = meta_data.get(f.name, None)

    return pd.DataFrame([meta_dict])