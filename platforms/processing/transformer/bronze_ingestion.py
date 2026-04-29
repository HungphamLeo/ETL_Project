import polars as pl
from prefect import task, flow
from datetime import date, datetime
import os
from dotenv import load_dotenv

# Tải thông tin MinIO từ file .env (đã tạo ở Phase 1)
load_dotenv()

@task(name="Extract Mock API Data", retries=2, retry_delay_seconds=5)
def extract_stock_data():
    """
    Subsystem 3: Extract System. 
    Mô phỏng việc gọi API hoặc cào dữ liệu trả về các DTOs (Danh sách Dictionary).
    """
    print("📥 Đang kéo dữ liệu từ Nguồn...")
    # Giả lập dữ liệu giá cổ phiếu cuối ngày
    mock_dto_data = [
        {"symbol": "FPT", "close_price": 115.5, "volume": 2500000, "foreign_buy": 150000},
        {"symbol": "VNM", "close_price": 68.2,  "volume": 3100000, "foreign_buy": 50000},
        {"symbol": "HPG", "close_price": 29.8,  "volume": 15000000, "foreign_buy": 2000000}
    ]
    return mock_dto_data

@task(name="Transform and Load to Bronze with Polars")
def load_to_bronze(data_dtos):
    """
    Sử dụng Polars để chuyển đổi danh sách DTO thành DataFrame, 
    gán Metadata và đẩy lên MinIO chuẩn Parquet.
    """
    print("⚡ Đang xử lý dữ liệu bằng Polars...")
    df = pl.DataFrame(data_dtos)
    
    # Subsystem 4: Archive & Lineage (Thêm các cột Audit Metadata)
    df = df.with_columns([
        pl.lit(date.today()).alias("ingest_date"),          # Phục vụ Partitioning
        pl.lit(datetime.now()).alias("ingest_timestamp"),   # Dấu thời gian chính xác
        pl.lit("api_batch_001").alias("batch_id")           # Truy xuất nguồn gốc
    ])
    
    # Cấu hình kết nối MinIO thông qua s3fs
    storage_options = {
        "key": os.getenv("MINIO_ROOT_USER"),
        "secret": os.getenv("MINIO_ROOT_PASSWORD"),
        "client_kwargs": {"endpoint_url": "http://localhost:9000"}
    }
    
    # Định nghĩa đường dẫn lưu trữ, phân mảnh (partition) theo ngày
    current_date = date.today().strftime("%Y-%m-%d")
    s3_path = f"s3://lakehouse/bronze/stock_prices/date={current_date}/raw_prices.parquet"
    
    print(f"💾 Đang ghi Parquet vào: {s3_path}")
    
    # Ghi file Parquet trực tiếp lên S3 (MinIO)
    df.write_parquet(
        s3_path, 
        use_pyarrow=True, 
        storage_options=storage_options
    )
    
    return s3_path

@flow(name="Daily Bronze Ingestion Flow", log_prints=True)
def run_bronze_ingestion():
    """Luồng chính (Main Flow) được Prefect điều phối"""
    print("🚀 BẮT ĐẦU LUỒNG INGESTION TẦNG BRONZE")
    
    # Bước 1: Lấy dữ liệu
    raw_data = extract_stock_data()
    
    # Bước 2: Xử lý và ghi xuống MinIO
    saved_path = load_to_bronze(raw_data)
    
    print(f"🎉 Hoàn thành! Dữ liệu đã an toàn tại: {saved_path}")

if __name__ == "__main__":
    # Khởi chạy flow cục bộ để test
    run_bronze_ingestion()