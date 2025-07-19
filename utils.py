
import os
import sys
from dataclasses import fields, asdict
import pandas as pd
from pathlib import Path

def find_project_root(marker="pyproject.toml", fallback_name="ETL_Project"):
    path = Path(__file__ if '__file__' in globals() else Path().resolve())
    for parent in path.parents:
        if (parent / marker).exists() or parent.name == fallback_name:
            return parent
    return Path().resolve()

project_root = find_project_root()
sys.path.insert(0, str(project_root))

def dataframe_rename_by_dataclass(df: pd.DataFrame, output_cls) -> pd.DataFrame:
    df = df.copy()
    field_names = [f.name for f in fields(output_cls)]
    
    if len(df.columns) != len(field_names):
        raise ValueError("Column count mismatch between DataFrame and dataclass")
    
    df.columns = field_names
    return df

