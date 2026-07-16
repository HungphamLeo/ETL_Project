import polars as pl
import os
from typing import List, Dict

# from platforms.processing.polars.polars_engine import PolarsConfig, PolarsEngine
# from platforms.processing.polars.base_polars_processor import BasePolarsProcessor

class Cophieu68PolarsIngester(BasePolarsProcessor):
    
    def process(self, raw_data: List[Dict], batch_id: str) -> str:
        """
        1. Chuyển đổi dữ liệu thô list[dict] sang DataFrame
        2. Thêm Audit fields
        3. Ghi vào Data Lake (MinIO)
        """
        self.logger.info(f"Bắt đầu Ingest {len(raw_data)} records...")
        
        # Tạo Polars DataFrame
        df = pl.DataFrame(raw_data)
        
        # Thêm metadata
        df = self.add_audit_metadata(df, batch_id=batch_id, source_system="cophieu68")
        
        # Xác định đường dẫn ghi file
        table_name = "stock_prices"
        s3_path = f"s3://lakehouse/bronze/{table_name}/"
        
        # Ghi Parquet chia partition theo ngày ingest
        saved_path = self.engine.write_parquet(
            df=df,
            target_path=s3_path,
            partition_by=["ingest_date"]
        )
        return saved_path

# ================================
# KHỞI TẠO TRONG PREFECT FLOW/APP
# ================================
if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    
    # 1. Đọc Config (Bình thường sẽ lấy từ ConfigLoader của bạn)
    raw_config = {
        "thread_pool_size": 4,
        "storage_options": {
            "endpoint_url": os.getenv("S3_ENDPOINT", "http://localhost:9000"),
            "aws_access_key_id": os.getenv("MINIO_ROOT_USER", "minioadmin"),
            "aws_secret_access_key": os.getenv("MINIO_ROOT_PASSWORD", "minioadmin_secure_123@#")
        }
    }
    
    # 2. Khởi tạo Engine
    config = PolarsConfig.from_dict(raw_config)
    engine = PolarsEngine(config)
    
    # 3. Khởi chạy Processor
    ingester = Cophieu68PolarsIngester(engine)
    
    mock_data = [
        {"symbol": "FPT", "close_price": 115.5, "volume": 2500000},
        {"symbol": "VNM", "close_price": 68.2, "volume": 3100000},
    ]
    
    result_path = ingester.process(raw_data=mock_data, batch_id="batch_prefect_001")
    print(f"Hoàn tất Ingestion tại: {result_path}")
