import os
import logging
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List, Union
import polars as pl

@dataclass
class PolarsConfig:
    """Dataclass chứa cấu hình cho Polars Engine"""
    thread_pool_size: Optional[int] = None
    enable_streaming: bool = True
    storage_options: Dict[str, Any] = field(default_factory=dict)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PolarsConfig":
        return cls(
            thread_pool_size=data.get("thread_pool_size"),
            enable_streaming=data.get("enable_streaming", True),
            storage_options=data.get("storage_options", {})
        )

class PolarsEngine:
    """
    Lớp quản lý engine xử lý dữ liệu bằng Polars (Tương tự SparkSession).
    Cung cấp các hàm I/O chuẩn với cấu hình tập trung.
    """
    def __init__(self, config: PolarsConfig, logger: Optional[logging.Logger] = None):
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self._setup_environment()

    def _setup_environment(self):
        """Cấu hình môi trường (VD: giới hạn số lượng thread)"""
        if self.config.thread_pool_size:
            os.environ["POLARS_MAX_THREADS"] = str(self.config.thread_pool_size)
            self.logger.info(f"🚀 Set POLARS_MAX_THREADS = {self.config.thread_pool_size}")

    def read_parquet(self, source_path: str) -> pl.LazyFrame:
        """Đọc dữ liệu Parquet dưới dạng LazyFrame (tối ưu hóa bộ nhớ)"""
        self.logger.info(f"📥 Đọc dữ liệu từ: {source_path}")
        return pl.scan_parquet(
            source_path, 
            storage_options=self.config.storage_options
        )

    def write_parquet(
        self, 
        df: Union[pl.DataFrame, pl.LazyFrame], 
        target_path: str, 
        partition_by: Optional[List[str]] = None
    ) -> str:
        """Ghi dữ liệu Data/Lazy Frame ra S3/MinIO dạng Parquet (hỗ trợ phân vùng)"""
        self.logger.info(f"💾 Ghi dữ liệu tới: {target_path} (Partition: {partition_by})")
        
        # Nếu là LazyFrame, thực thi (collect) theo streaming mode
        if isinstance(df, pl.LazyFrame):
            df = df.collect(streaming=self.config.enable_streaming)
            
        df.write_parquet(
            target_path,
            partition_by=partition_by
        )
        return target_path
