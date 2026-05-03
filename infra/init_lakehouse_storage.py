import os
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv

load_dotenv()

def initialize_lakehouse():
    client = Minio(
        "localhost:9000",
        access_key=os.getenv("MINIO_ROOT_USER"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
        secure=False
    )
    bucket_name = "lakehouse"
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
        folders = [
            "bronze/stock_prices/",
            "bronze/financial_reports/",
            "silver/dim_stock/",
            "silver/dim_date/",
            "silver/fact_stock_price/",
            "gold/mart_kpi_daily/"
        ]
        for folder in folders:
            client.put_object(bucket_name, folder, data=b"", length=0)
        print("Lakehouse initialized")
    except S3Error as exc:
        print("MinIO error:", exc)

if __name__ == "__main__":
    initialize_lakehouse()
