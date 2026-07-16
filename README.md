# Vietnam Stock Market — Data Lakehouse ETL Pipeline

Pipeline ETL tự động thu thập, xử lý và lưu trữ dữ liệu chứng khoán Việt Nam
theo kiến trúc **Medallion (Bronze → Silver → Gold → Serving)** trên nền tảng
Data Lakehouse hiện đại.

> 📖 **Xem tài liệu kiến trúc chi tiết:** [`ARCHITECTURE.md`](ARCHITECTURE.md)

---

## Tổng quan kiến trúc

```
cophieu68.vn (HTML)
      │  BeautifulSoup scraper
      ▼
  [BRONZE]  Parquet  →  MinIO  s3a://lakehouse/bronze/
      │  DuckDB query + Polars dedup + Surrogate key
      ▼
  [SILVER]  Parquet  →  MinIO  s3a://lakehouse/silver/
      │  SQLMesh incremental models
      ▼
  [GOLD]    Parquet  →  MinIO  s3a://lakehouse/gold/
      │  DuckDB ATTACH postgres → INSERT
      ▼
  [SERVING] PostgreSQL  →  BI / Analytics
```

| Tầng | Nội dung | Công cụ |
|------|----------|---------|
| **Bronze** | Raw data + metadata tracking | Polars · MinIO · Great Expectations |
| **Silver** | Deduplicated + typed + surrogate key | DuckDB · Polars · Pandas |
| **Gold** | KPI aggregation, mart tables | SQLMesh · DuckDB |
| **Serving** | OLAP-ready, phục vụ query | PostgreSQL · dbt |

---

## Tech Stack

| Layer | Công nghệ | Phiên bản |
|-------|-----------|-----------|
| Ngôn ngữ | Python | 3.11 / 3.13 |
| DataFrame engine | Polars | 1.5.0 |
| SQL engine nhúng | DuckDB | 1.0.0 |
| Data modeling | SQLMesh | 0.234.1 |
| Transformation | dbt-postgres | 1.7.15 |
| Data Quality | Great Expectations | 1.19.0 |
| Object Storage | MinIO (S3-compatible) | latest |
| Serving DB | PostgreSQL | 15 |
| Orchestration | Prefect | 2.14.0 |
| Streaming (future) | Apache Kafka · Flink | 3.8 · 1.20 |
| Distributed (future) | Apache Spark | 3.5.0 |
| Query engine (future) | Trino | 458 |
| Web crawling | BeautifulSoup4 | 4.14.2 |
| Containerization | Docker / Docker Compose | — |

---

## Nguồn dữ liệu

**cophieu68.vn** — website chứng khoán Việt Nam, thu thập qua HTTP scraping:

- Lịch sử giao dịch (giá, khối lượng, foreign buy/sell)
- Hồ sơ công ty niêm yết
- Báo cáo tài chính (quý / năm)
- Phân ngành — sector / industry

Symbols mặc định: `FPT · VNM · HPG · MBB · SSI`

---

## Cấu trúc thư mục

```
ETL_Project/
├── scripts/
│   └── deploy_full_pipeline.py      ← Entry point CLI chính
├── platforms/
│   ├── ingestion/cophieu68/         ← Scraper + DTO models
│   ├── processing/
│   │   ├── base_processing_subsystem/  ← Core subsystems (DQ, Dedup, SCD, SK…)
│   │   ├── polars/                  ← Polars engine wrapper
│   │   ├── duckdb/                  ← DuckDB engine wrapper
│   │   ├── sqlmesh/                 ← SQLMesh engine wrapper
│   │   └── dbt/models/vietnam_stocks/  ← dbt staging + marts
│   ├── orchestration/prefect/       ← Flows + config YAML
│   └── storage/                     ← MinIO + PostgreSQL connectors
├── shared/logger/                   ← Structured JSON logger
├── tests/
│   ├── unit/                        ← 6 subsystem unit test suites
│   └── intergration/                ← Integration tests
├── infra/
│   ├── docker_compose/docker-compose.dev.yml
│   └── init_lakehouse_storage.py    ← MinIO bucket setup
├── ARCHITECTURE.md                  ← Tài liệu kiến trúc đầy đủ
└── Makefile                         ← Shortcut commands
```

---

## Bắt đầu nhanh

### 1. Yêu cầu

- Docker & Docker Compose
- Python 3.11+
- `make` (tùy chọn, dùng shortcut)

### 2. Khởi động hạ tầng

```bash
# Khởi động toàn bộ services (MinIO, PostgreSQL, Prefect, Kafka, Spark...)
make infra-up

# Khởi tạo bucket lakehouse trên MinIO
make infra-init
```

Hoặc thủ công:

```bash
docker-compose -f infra/docker_compose/docker-compose.dev.yml up -d
python infra/init_lakehouse_storage.py
```

### 3. Cài đặt dependencies

```bash
pip install -r requirements_common.txt
pip install -r requirements_prefect.txt
pip install -r requirements_dbt_postgre.txt
```

### 4. Cấu hình môi trường

Tạo file `.env` ở root:

```env
LAKEHOUSE_BASE_PATH=s3a://lakehouse
S3_ENDPOINT=http://localhost:9000
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin_secure_123@#

POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=etl_project
POSTGRES_USER=postgres
POSTGRES_PASSWORD=password
```

### 5. Chạy pipeline

```bash
# Chạy toàn bộ pipeline (Bronze → Silver → Gold → Serving)
python scripts/deploy_full_pipeline.py full --symbols FPT VNM HPG --date 2026-07-16

# Chạy từng phase độc lập
python scripts/deploy_full_pipeline.py bronze  --symbols FPT VNM
python scripts/deploy_full_pipeline.py silver  --date 2026-07-16
python scripts/deploy_full_pipeline.py gold    --date 2026-07-16
python scripts/deploy_full_pipeline.py serving --date 2026-07-16

# Kiểm tra config trước khi chạy
python scripts/deploy_full_pipeline.py validate
```

---

## Tests

```bash
# Chạy toàn bộ unit tests
pytest tests/unit/ -v

# Hoặc dùng Make
make test
```

Unit test coverage hiện tại:

| File test | Subsystem được test |
|-----------|---------------------|
| `test_subsystem1_data_profiling.py` | Data Profiling + GE integration |
| `test_subsystem4_data_cleansing.py` | DQ Rules + Non-blocking cleansing |
| `test_subsystem5_error_event.py` | Error Event schema |
| `test_subsystem7_deduplication.py` | Dedup strategies |
| `test_subsystem9_scd_manager.py` | SCD Type 1 & 2 |
| `test_subsystem10_surrogate_key.py` | Hash-based surrogate keys |

---

## Services & Ports

| Service | URL / Port | Ghi chú |
|---------|------------|---------|
| MinIO Console | http://localhost:9001 | Object storage UI |
| MinIO API | http://localhost:9000 | S3-compatible endpoint |
| PostgreSQL | localhost:5432 | Serving database |
| Prefect UI | http://localhost:4200 | Workflow orchestration |
| Spark Master UI | http://localhost:8080 | Distributed compute |
| Flink JobManager | http://localhost:8081 | Stream processing |
| Trino | http://localhost:8082 | Federated query |
| Kafka | localhost:9092 | Message broker |
| MongoDB | localhost:27017 | (legacy / datalake alt) |

---

## Makefile shortcuts

```bash
make infra-up        # Khởi động toàn bộ Docker services
make infra-down      # Dừng services
make infra-init      # Khởi tạo MinIO buckets
make infra-status    # Xem trạng thái containers
make sqlmesh-plan    # Xem SQLMesh execution plan
make sqlmesh-run     # Chạy SQLMesh transforms
make test            # Chạy pytest
make rebuild         # Clean + rebuild toàn bộ containers
```

---

## Chiến lược Data Quality

DQ trong dự án này là **quan sát, không phải chốt chặn**:

- Mọi records đều đi qua pipeline dù có vi phạm DQ rule
- Violations được log thành `ErrorEvent` (WARNING level) và `dq_observations`
  (Great Expectations) để phân tích sau
- Kết quả GE lưu trong `DataProfile.ge_results` — chuẩn bị cho Grafana reporting

```
Record vi phạm rule
       │
       ▼ log WARNING
  [DQ_OBS] stored        ← Great Expectations result
       │
       ▼ pipeline tiếp tục
  Record vẫn được lưu
```

---

## Tài liệu liên quan

- [`ARCHITECTURE.md`](ARCHITECTURE.md) — Kiến trúc chi tiết, lý thuyết từng tech stack,
  minh họa code, design patterns và roadmap học tập cho fresher
