# ETL Project – MongoDB → Delta Lake Migration TODO

## Progress Tracker

### Nhóm 1 – Delta Lake Storage Backend
- [ ] `platforms/storage/deltalake/delta_schema_registry.py` – Bronze/Silver/Gold schema definitions
- [ ] `platforms/storage/deltalake/delta_lake_storage.py` – StorageBackend implementation (PySpark + Delta)

### Nhóm 2 – Delta Lake Loader (cophieu68)
- [ ] `platforms/ingestion/cophieu68/load/load_datalake_delta_cophieu68.py` – DeltaLoader replacing MongoLoader

### Nhóm 3 – Config Update
- [ ] `platforms/processing/prefect/config/cophieu68_config.yaml` – add `storage.deltalake` section
- [ ] `requirements_common.txt` – add delta-spark, pyspark

### Nhóm 4 – Subsystem Implementations
- [ ] `platforms/processing/transformer/data_cleansing.py` – Subsystem 1, 4, 5 (Profiling + Cleansing + Error Event)
- [ ] `platforms/processing/transformer/deduplication.py` – Subsystem 7
- [ ] `platforms/processing/transformer/scd_manager.py` – Subsystem 9 (SCD Type 1 & 2)
- [ ] `platforms/processing/transformer/surrogate_key_generator.py` – Subsystem 10
- [ ] `platforms/processing/transformer/metadata_repository.py` – Subsystem 34 (Lineage + Metadata)

### Nhóm 5 – Migration & Documentation
- [ ] `docs/mongodb_to_deltalake_migration.md` – Architecture Decision Record + Migration Phases
- [ ] `platforms/ingestion/cophieu68/load/migrate_mongo_to_delta.py` – Backfill script

---

## Status Legend
- [ ] Pending
- [x] Done
- [~] In Progress

Dưới đây là cách hiểu và triển khai **Delta Architecture (Lakehouse)** theo hướng hiện đại, phù hợp với định hướng bạn đang xây dựng data platform (Kafka + Go + Spark + ML + BI).

---

# 1. Bản chất Delta / Lakehouse Architecture

Các khái niệm cốt lõi:

* Delta Lake
* Apache Iceberg
* Apache Hudi

👉 Đây **không phải là engine**, mà là **table format + transaction layer** trên Data Lake.

### Khác biệt chính so với Lambda

| Lambda                   | Delta / Lakehouse          |
| ------------------------ | -------------------------- |
| Batch + Speed tách riêng | Unified processing         |
| Data lake immutable      | Data lake có ACID + update |
| Duplicate logic          | Single pipeline            |
| Serving layer riêng      | Query trực tiếp lake       |

👉 Tư tưởng chính:

> “Data Lake trở thành Data Warehouse”

---

# 2. Kiến trúc tổng thể (modern Delta Architecture)

```text
                ┌──────────────┐
                │ Data Sources │
                └──────┬───────┘
                       ↓
               (Kafka / CDC / API)
                       ↓
                ┌──────────────┐
                │   Ingestion  │  ← Golang
                └──────┬───────┘
                       ↓
                ┌──────────────┐
                │   Streaming  │ ← Flink / Spark
                └──────┬───────┘
                       ↓
        ┌─────────────────────────────┐
        │   Data Lake (Lakehouse)     │
        │  Delta / Iceberg / Hudi     │
        └─────────────────────────────┘
                       ↓
         ┌────────────┴────────────┐
         ↓                         ↓
   Batch Processing         Real-time Query
     (Spark)                (Trino / Druid)
         ↓                         ↓
                ┌──────────────┐
                │ BI / ML / API│
                └──────────────┘
```

---

# 3. Pipeline chi tiết (chuẩn production)

## (1) Ingestion Layer

### Tech stack

* Apache Kafka
* Debezium
* Golang:

  * kafka producer/consumer
  * API crawler

### Output

* Raw event → Kafka topic

---

## (2) Streaming Processing (core của Delta)

### Tech stack

* Apache Flink (real-time chuẩn nhất)
* hoặc:

  * Apache Spark (structured streaming)

### Xử lý

* Clean
* Deduplicate
* Enrichment
* Join stream

### Output

* Ghi trực tiếp vào:

  * Delta / Iceberg table

👉 Đây là điểm khác biệt lớn:

> Stream ghi thẳng vào data lake (không cần speed layer riêng)

---

## (3) Storage Layer (Lakehouse)

### Tech stack

* Storage:

  * S3 / MinIO / HDFS
* Table format:

  * Delta Lake (Databricks ecosystem)
  * Iceberg (neutral, đang phổ biến nhất)

### Tính năng chính

* ACID transaction
* Time travel
* Schema evolution
* Upsert / Merge

---

## (4) Batch Processing (optional nhưng vẫn cần)

### Tech stack

* Spark (ETL lớn, ML feature engineering)

### Vai trò

* Backfill
* Recompute
* Training dataset

---

## (5) Serving / Query Layer

### Tech stack

* Trino
* Presto
* Apache Druid
* ClickHouse

👉 Query trực tiếp Lakehouse (không cần DW riêng)

---

## (6) BI / ML / API

* BI: Power BI / Superset
* ML:

  * PyTorch / Sklearn
* API:

  * Golang (serve data + model)

---

# 4. So sánh nhanh: nên chọn gì?

| Thành phần         | Khuyến nghị        |
| ------------------ | ------------------ |
| Streaming          | Flink              |
| Batch              | Spark              |
| Storage format     | Iceberg (neutral)  |
| Query              | Trino              |
| Realtime analytics | ClickHouse / Druid |
| Orchestration      | Airflow            |
| Ingestion          | Kafka + Go         |

---

# 5. Stack tối ưu cho bạn (rất thực tế)

Dựa trên định hướng bạn đang build full system:

### Core

* Kafka
* Golang
* Flink
* Spark

### Lakehouse

* Iceberg + MinIO

### Query

* Trino

### Realtime

* ClickHouse

### ML

* PyTorch / Sklearn

### Orchestration

* Airflow

### Monitoring

* Prometheus + Grafana

---

# 6. Roadmap học (quan trọng nhất)

## Giai đoạn 1 – Foundation

* Kafka (must)
* SQL nâng cao
* Data modeling (Kimball)

---

## Giai đoạn 2 – Streaming

* Flink:

  * window
  * state
  * checkpoint
* Kafka Streams (optional)

---

## Giai đoạn 3 – Lakehouse

* Iceberg / Delta Lake:

  * partitioning
  * compaction
  * merge/upsert

---

## Giai đoạn 4 – Batch + ML

* Spark:

  * DataFrame API
  * optimization (shuffle, partition)

---

## Giai đoạn 5 – Serving

* Trino:

  * query federation
* ClickHouse:

  * OLAP optimization

---

## Giai đoạn 6 – Production

* Docker + Kubernetes
* CI/CD
* Data quality (Great Expectations)

---

# 7. Nhận định thực tế

1. Delta/Lakehouse là xu hướng chính hiện tại
   → thay thế Lambda

2. Iceberg đang phổ biến hơn Delta (open ecosystem)

3. Flink + Iceberg + Kafka = stack mạnh nhất hiện nay

4. Golang không phải core processing engine
   → chỉ dùng:

   * ingestion
   * service/API

---

# 8. Kết luận

Một kiến trúc Delta hiện đại sẽ:

* **Không tách batch và stream**
* **Stream ghi trực tiếp vào data lake**
* **Query trực tiếp lake bằng SQL engine**
* **Lake = Warehouse**

---

Nếu cần, có thể đi sâu hơn:

* Thiết kế **schema Kafka + partition strategy**
* Thiết kế **table Iceberg cho banking (transaction, CIF, CASA)**
* Hoặc build **end-to-end project giống production**
