
from dataclasses import fields
from pathlib import Path
from typing import Type
import pandas as pd
from typing import Dict, Any, Optional
import time
import threading

def find_project_root(marker: str = "pyproject.toml", fallback_name: str = "ETL_Project") -> Path:
    """
    Tìm root của project dựa trên file marker hoặc tên folder fallback.
    """
    path = Path(__file__ if '__file__' in globals() else Path().resolve())
    for parent in path.parents:
        if (parent / marker).exists() or parent.name == fallback_name:
            return parent
    return Path().resolve()


def dataframe_rename_by_dataclass(df: pd.DataFrame, output_cls: Type) -> pd.DataFrame:
    """
    Đổi tên cột DataFrame theo dataclass field.
    """
    df = df.copy()
    field_names = [f.name for f in fields(output_cls)]

    if len(df.columns) != len(field_names):
        raise ValueError("Column count mismatch between DataFrame and dataclass")

    df.columns = field_names
    return df

# ==============================
# Snowflake ID Generator
# ==============================

class SnowflakeGenerator:
    """
    Generator ID theo Snowflake algorithm (64-bit).
    """

    def __init__(self, machine_id: int = 1, character_specific: Optional[str] = None):
        self.epoch = 1577836800000  # Epoch: Jan 1, 2020
        self.machine_id = machine_id & 0x3FF
        self.sequence = 0
        self.last_timestamp = -1
        self.lock = threading.Lock()
        self.character_specific = character_specific

    def _timestamp(self) -> int:
        return int(time.time() * 1000)

    def get_id(self) -> str:
        with self.lock:
            now = self._timestamp()
            if now == self.last_timestamp:
                self.sequence = (self.sequence + 1) & 0xFFF
                if self.sequence == 0:
                    # chờ sang ms tiếp theo
                    while self._timestamp() <= self.last_timestamp:
                        time.sleep(0.001)
                    now = self._timestamp()
            else:
                self.sequence = 0

            self.last_timestamp = now
            snowflake_id = ((now - self.epoch) << 22) | (self.machine_id << 12) | self.sequence

            return f"{self.character_specific}_{snowflake_id}" if self.character_specific else str(snowflake_id)


class TableCreator(SnowflakeGenerator):
    """
    Convert DW schema → CREATE TABLE SQL (type + constraints)
    Sinh surrogate key dạng Snowflake.
    """

    def generate_create_table_sql(self, table_name: str, rules_dict: Dict[str, Any]) -> str:
        columns = []

        for col, meta in rules_dict.items():
            col_type = meta.get("type")
            constraints = meta.get("constraints", "")
            columns.append(f'"{col}" {col_type} {constraints}'.strip())

        return (
            f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n  ' +
            ",\n  ".join(columns) +
            "\n);"
        )

    def add_surrogate_key(self, df: pd.DataFrame, key_name: str) -> pd.DataFrame:
        df[key_name] = [self.get_id() for _ in range(len(df))]
        return df
