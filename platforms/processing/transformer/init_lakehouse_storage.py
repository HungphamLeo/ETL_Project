# init_lakehouse_storage.py
import os
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv

# Tải credentials từ file .env
load_dotenv()

def initialize_lakehouse():
    print("🚀 Bắt đầu khởi tạo cấu trúc Stock Lakehouse...")
    
    # Kết nối tới MinIO local
    client = Minio(
        "localhost:9000",
        access_key=os.getenv("MINIO_ROOT_USER"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
        secure=False # Dùng HTTP cho local dev
    )

    # Bucket duy nhất cho toàn bộ Data Lake
    bucket_name = "lakehouse"

    # Tạo Bucket nếu chưa tồn tại
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"✅ Đã tạo S3 Bucket: '{bucket_name}'")
        else:
            print(f"ℹ️ Bucket '{bucket_name}' đã tồn tại.")

        # Tạo cấu trúc thư mục (Medallion Architecture)
        folders = [
            "bronze/stock_prices/",
            "bronze/financial_reports/",
            "bronze/company_profiles/",
            "silver/dim_stock/",
            "silver/dim_date/",
            "silver/fact_stock_price/",
            "gold/mart_kpi_daily/"
        ]

        # Trong S3, "thư mục" là các object kết thúc bằng '/'
        for folder in folders:
            # Tạo một file rỗng để giữ cấu trúc thư mục
            client.put_object(
                bucket_name,
                folder,
                data=b"",
                length=0,
            )
            print(f"   📂 Đã tạo phân vùng: {folder}")
            
        print("🎉 Khởi tạo Lakehouse thành công!")

    except S3Error as exc:
        print("❌ Lỗi kết nối MinIO:", exc)

if __name__ == "__main__":
    initialize_lakehouse()