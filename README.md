# ETL_Project

ETL_Project là nền tảng Data Platform hiện đại, được xây dựng trên các kiến trúc tiên tiến nhằm phục vụ nhu cầu xử lý dữ liệu tài chính, kinh tế vĩ mô và blockchain. Dự án áp dụng:

- **Microservices Architecture**
- **Domain-Driven Design (DDD)**
- **Clean Architecture**
- **Data Pipeline Architecture** (Ingestion → Lake → DWH → Mart)
- **Lakehouse (DeltaLake)**
- **Event-driven** (Kafka / HDFS publisher)
- **ML/Trading Engine**
- **Blockchain Indexing & Smart Contract Integration**

## 🎯 Mục tiêu
- Thu thập dữ liệu thị trường chứng khoán Việt Nam (Cophieu68, DNSE, SSI...)
- Thu thập dữ liệu vĩ mô từ World Bank, FED
- Xây dựng Data Lake, Data Warehouse, Data Mart
- Triển khai streaming ingestion sử dụng Kafka
- Orchestration pipelines với Airflow/Prefect
- Phát triển ML Trading System
- Tích hợp dữ liệu blockchain (on-chain/off-chain)

## 🏗️ Kiến trúc tổng thể
```
+-------------------------------------------+
|            Data Sources                   |
| API / Web / Blockchain                    |
+-------------------+-----------------------+
                    |
                    v
  +------ Platforms: Ingestion ------+
  | Extractors / Parsers / Modules   |
  +------------+---------------------+
                | Kafka / HDFS / Load
                v
+--- Platforms: Storage (Lake/DWH) ---+
| Datalake: MongoDB/HDFS/DeltaLake    |
| Warehouse: PostgreSQL/Kimball Model |
+-------+-----------------------------+
        |           |
        |           v
        |   +------ Processing ------+
        |   | Airflow/Prefect/Spark |
        |   +-------+---------------+
        |           |
        |           v
        |   +------ Analytics/ML ---+
        |   | Trading/Forecasting   |
        |   +----------------------+
        v
+----- Streaming Platform ----------+
| Kafka Producers/Consumers         |
+-----------------------------------+

+---------------------------------------+
| Blockchain Integration                |
| (Smart Contract, Chaincode, Oracle)   |
+---------------------------------------+
```

## 📦 Cấu trúc thư mục

```
ETL_Project/
├── infra/           # Docker, Kubernetes, Terraform
├── platforms/       # Ingestion, Storage, Streaming, Processing
│   ├── ingestion/
│   ├── processing/
│   ├── streaming/
│   ├── storage/
│   ├── quality/
│   └── blockchain/
├── services/        # Domain Microservices: marketdata, macrodata, analytics, reporting, user
├── shared/          # Thư viện dùng chung
├── scripts/         # CLI scripts & notebooks
└── tests/           # Unit, Integration, E2E tests
```

Ưu điểm:
- Phân tách nhiệm vụ rõ ràng (Separation of concerns)
- Triển khai độc lập từng module (microservice)
- Dễ mở rộng, bảo trì, scale hệ thống
- Hỗ trợ đầy đủ ETL, Streaming, ML, Blockchain

## ⚙️ Công nghệ sử dụng

**Data Engineering:**
- Python 3.11+
- Airflow / Prefect
- Apache Spark
- DBT (Data Build Tool)
- Kafka
- HDFS / MinIO

**Storage:**
- MongoDB (Data Lake)
- PostgreSQL (Data Warehouse)
- DeltaLake / Parquet

**ML & Trading:**
- PyTorch
- Scikit-learn
- XGBoost / CatBoost
- Backtesting Engine

**Blockchain:**
- Solidity (Smart Contract)
- Go (Chaincode)
- Oracle & Indexer

**DevOps:**
- Docker / Docker Compose
- Kubernetes (K8s)
- Terraform
- Helm

## 🔥 Các pipeline chính

**Pipeline 1 – Stock Market (Cophieu68):**
- Crawl web, extract dữ liệu OHLCV, lưu vào MongoDB Data Lake
- Transform, lưu sang PostgreSQL DWH (Fact/Dim)
- Publish event qua Kafka, hỗ trợ streaming và ML prediction

**Pipeline 2 – FED Macroeconomic Data:**
- Fetch API theo lịch
- Load vào DWH dạng Kimball (dim_macro, fact_macro)

**Pipeline 3 – Blockchain:**
- Index event từ smart contract
- Lưu dữ liệu vào Lakehouse
- Oracle push dữ liệu off-chain lên smart contract

**Pipeline 4 – ML Trading:**
- Feature Store
- Train/Predict/Backtest/Deploy mô hình

## 🧪 Chiến lược kiểm thử

```
tests/
├── unit/         # Test từng module nhỏ
├── integration/  # Test các pipeline liên hệ qua lại
└── e2e/          # Kiểm thử toàn bộ ETL, từ raw data đến DWH
```

## 🚀 Hướng dẫn chạy dự án

1. Clone repository:
    ```sh
    git clone https://github.com/HungphamLeo/ETL_Project.git
    cd ETL_Project
    ```

2. Tạo môi trường Python:
    ```sh
    pip install -r requirements_common.txt
    ```

3. Chạy Docker stack (Airflow, Kafka, MongoDB, Postgres...):
    ```sh
    docker compose -f infra/docker-compose.yml up -d
    ```

4. Khởi động Airflow UI tại [`http://localhost:8080`](http://localhost:8080)

5. Khởi động Prefect:
    ```sh
    prefect server start
    ```

## 📊 Data Warehouse (Kimball Model)
- dim_company, dim_calendar, dim_exchange
- fact_stock_price, fact_macro_indicator

## 🔐 Bảo mật
- Data encryption key lưu trong `shared/security/`
- WAF + API rate limit cho mảng ingestion
- JWT cho Layer services

## 🗺️ Roadmap
- Thêm ingestion cho SSI, VNDirect API
- Thêm compaction jobs DeltaLake
- Tích hợp lựa chọn Snowflake/BigQuery
- Xây dựng auto-refresh Superset/Power BI
- On-chain ↔ Off-chain reconciliation engine

## 👨‍💻 Tác giả

**Hùng Phạm – Data Engineer & Analytics**  
Fintech | Blockchain | Machine Learning

---

**Liên hệ**: hungphamtudo@gmail.com
