import os
import logging
from dataclasses import dataclass, field
from typing import Dict, Any, Optional
import duckdb
import polars as pl

@dataclass
class DuckDBConfig:
    """Dataclass chứa cấu hình cho DuckDB Engine."""
    database_path: str = ":memory:"
    read_only: bool = False
    storage_options: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DuckDBConfig":
        return cls(
            database_path=data.get("database_path", ":memory:"),
            read_only=data.get("read_only", False),
            storage_options=data.get("storage_options", {})
        )

class DuckDBEngine:
    """
    Lớp quản lý engine xử lý dữ liệu bằng DuckDB.
    Cung cấp kết nối và các hàm tiện ích để truy vấn dữ liệu trên Data Lake (MinIO/S3).
    """
    def __init__(self, config: DuckDBConfig, logger: Optional[logging.Logger] = None):
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self.connection = self._connect()

    def _connect(self) -> duckdb.DuckDBPyConnection:
        """Khởi tạo kết nối DuckDB và cấu hình S3 access."""
        self.logger.info(f"🦆 Khởi tạo kết nối DuckDB tới: '{self.config.database_path}'")
        
        con = duckdb.connect(
            database=self.config.database_path,
            read_only=self.config.read_only
        )
        
        if self.config.storage_options:
            self.logger.info("🔧 Cấu hình S3/MinIO cho DuckDB...")
            s3_endpoint = self.config.storage_options.get("endpoint_url", "").split("://")[-1]
            s3_access_key = self.config.storage_options.get("aws_access_key_id")
            s3_secret_key = self.config.storage_options.get("aws_secret_access_key")

            con.execute("INSTALL httpfs; LOAD httpfs;")
            con.execute(f"SET s3_endpoint='{s3_endpoint}';")
            con.execute(f"SET s3_access_key_id='{s3_access_key}';")
            con.execute(f"SET s3_secret_access_key='{s3_secret_key}';")
            con.execute("SET s3_use_ssl=false;")
            con.execute("SET s3_url_style='path';")
            con.execute("SET s3_region='us-east-1';")
            self.logger.info("✅ Cấu hình S3/MinIO hoàn tất.")

        return con

    def query_to_polars(self, sql_query: str) -> pl.LazyFrame:
        """Thực thi một câu lệnh SQL và trả về kết quả dưới dạng Polars LazyFrame."""
        self.logger.debug(f"Executing query: {sql_query}")
        return self.connection.sql(sql_query).pl()

    def query_to_df(self, sql_query: str) -> pl.DataFrame:
        """Thực thi một câu lệnh SQL và trả về kết quả dưới dạng Polars DataFrame."""
        self.logger.debug(f"Executing query: {sql_query}")
        return self.connection.sql(sql_query).pl().collect()

    def close(self):
        """Đóng kết nối DuckDB."""
        if self.connection:
            self.logger.info("🔌 Đóng kết nối DuckDB.")
            self.connection.close()
            self.connection = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()