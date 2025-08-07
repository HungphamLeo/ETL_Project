
import os
import sys
from dataclasses import fields
import pandas as pd
from pathlib import Path
import time
import threading
import pandas as pd
import threading


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



# Snowflake generator 64-bit

class SnowflakeGenerator:
    def __init__(self, machine_id: int = 1, character_specific: str = None):
        self.epoch = 1577836800000  # Jan 1, 2020
        self.machine_id = machine_id & 0x3FF
        self.sequence = 0
        self.last_timestamp = -1
        self.lock = threading.Lock()
        self.character_specific = character_specific  # e.g. "user", "topic"

    def _timestamp(self):
        return int(time.time() * 1000)

    def get_id(self):
        with self.lock:
            now = self._timestamp()
            if now == self.last_timestamp:
                self.sequence = (self.sequence + 1) & 0xFFF
                if self.sequence == 0:
                    while self._timestamp() <= self.last_timestamp:
                        time.sleep(0.001)
                    now = self._timestamp()
            else:
                self.sequence = 0
            self.last_timestamp = now
            snowflake_id = ((now - self.epoch) << 22) | (self.machine_id << 12) | self.sequence
            if self.character_specific:
                return f"{self.character_specific}_{snowflake_id}"
            else:
                return str(snowflake_id)

class TableCreator(SnowflakeGenerator):
    def __init__(self, machine_id: int = 1, character_specific: str = None):
        super().__init__(machine_id, character_specific)

    def generate_create_table_sql(self, table_name, rules_dict, extra_types=None):
        """
        Sinh câu lệnh CREATE TABLE từ dict rule, có thêm cột id làm PRIMARY KEY.
        """
        extra_types = extra_types or {}
        columns = ["`id` VARCHAR(32) PRIMARY KEY"]
        for col, rule in rules_dict.items():
            if col == "drop_columns":
                continue
            col_type = extra_types.get(col, "VARCHAR(255)")
            columns.append(f"`{col}` {col_type}")
        columns_sql = ",\n  ".join(columns)
        return f"CREATE TABLE IF NOT EXISTS `{table_name}` (\n  {columns_sql}\n);"

    def add_id_column(self, df):
        """
        Thêm cột id vào DataFrame với giá trị sinh từ get_id().
        """
        df = df.copy()
        df.insert(0, "id", [self.get_id() for _ in range(len(df))])
        return df


