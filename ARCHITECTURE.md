# ETL Project — Architecture Guide

> **Mục tiêu tài liệu này:** Giúp một Data Engineer fresher có thể hiểu toàn bộ
> kiến trúc, đọc hiểu từng công nghệ từ lý thuyết đến code thực tế trong dự án,
> và nắm được luồng ETL end-to-end như một senior.

---

## Mục lục

1. [Tổng quan kiến trúc — Medallion Architecture](#1-tổng-quan-kiến-trúc--medallion-architecture)
2. [Luồng ETL end-to-end](#2-luồng-etl-end-to-end)
3. [Stack công nghệ](#3-stack-công-nghệ)
4. [Extract — Thu thập dữ liệu](#4-extract--thu-thập-dữ-liệu)
5. [Bronze Layer — Ingest thô vào Data Lake](#5-bronze-layer--ingest-thô-vào-data-lake)
6. [Subsystem xử lý cốt lõi](#6-subsystem-xử-lý-cốt-lõi)
7. [Silver Layer — Làm sạch và chuẩn hóa](#7-silver-layer--làm-sạch-và-chuẩn-hóa)
8. [Gold Layer — Modeling với SQLMesh](#8-gold-layer--modeling-với-sqlmesh)
9. [Serving Layer — Đưa dữ liệu ra PostgreSQL](#9-serving-layer--đưa-dữ-liệu-ra-postgresql)
10. [Orchestration — Prefect & Master Orchestrator](#10-orchestration--prefect--master-orchestrator)
11. [Observability — DQ, Lineage, Error Events, Metadata](#11-observability--dq-lineage-error-events-metadata)
12. [Cấu trúc thư mục dự án](#12-cấu-trúc-thư-mục-dự-án)
13. [Thiết kế pattern quan trọng](#13-thiết-kế-pattern-quan-trọng)
14. [Roadmap kỹ năng cho fresher](#14-roadmap-kỹ-năng-cho-fresher)

---

## 1. Tổng quan kiến trúc — Medallion Architecture

### Lý thuyết

**Medallion Architecture** (hay còn gọi là Multi-hop Architecture) là pattern
lưu trữ dữ liệu được phổ biến bởi Databricks. Dữ liệu được tổ chức theo 3 tầng
chất lượng tăng dần, mỗi tầng là một "huy chương" về độ sạch và giá trị:

```
Raw Source ──► BRONZE ──► SILVER ──► GOLD ──► Serving (PostgreSQL / BI)
               (thô)     (sạch)    (nghiệp vụ)
```

| Tầng | Vai trò | Đặc điểm | Format trong dự án |
|------|---------|-----------|-------------------|
| **Bronze** | Ingest trực tiếp từ source | Giữ nguyên raw, thêm metadata | Parquet trên MinIO/S3 |
| **Silver** | Cleanse + Dedup + type casting | Deduplicated, có surrogate key | Parquet trên MinIO/S3 |
| **Gold** | Aggregation / Business logic | KPI, mart tables | SQLMesh models → Parquet |
| **Serving** | Phục vụ truy vấn | OLAP-ready | PostgreSQL tables |

### Trong dự án này

```
cophieu68.vn (HTML/JSON)
       │
       ▼  BeautifulSoup scraper
  [Extract Layer]
       │
       ▼  Polars → write_parquet → MinIO
  [BRONZE]  s3a://lakehouse/bronze/stock_prices/ingest_date=YYYY-MM-DD/
       │
       ▼  DuckDB query + Polars dedup + SurrogateKey
  [SILVER]  s3a://lakehouse/silver/fact_stock_price/
       │
       ▼  SQLMesh models (DuckDB backend)
  [GOLD]    s3a://lakehouse/gold/mart_kpi_daily/
       │
       ▼  DuckDB ATTACH postgres → INSERT
  [SERVING] PostgreSQL: gold.mart_kpi_daily
```

---

## 2. Luồng ETL end-to-end

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     MasterPipelineOrchestrator                          │
│  scripts/deploy_full_pipeline.py                                        │
│                                                                         │
│  run_id = make_run_id()  # "run_20260716_143022_a3b4c5"                 │
│                                                                         │
│  ┌──────────┐   ┌──────────┐   ┌─────────┐   ┌──────────┐             │
│  │ Bronze   │──►│ Silver   │──►│  Gold   │──►│ Serving  │             │
│  │ Executor │   │ Executor │   │Executor │   │ Executor │             │
│  └──────────┘   └──────────┘   └─────────┘   └──────────┘             │
└─────────────────────────────────────────────────────────────────────────┘
```

### Phase Bronze (per symbol: FPT, VNM, HPG…)

```python
# scripts/deploy_full_pipeline.py — BronzeExecutor.execute()

trading_data = extractor.crawl_trading_data(symbol=symbol, page=1)
# → HTTP GET cophieu68.vn/quote/history.php?id=FPT
# → BeautifulSoup parse HTML → List[Dict]

cleansing = _build_cleansing_rules(symbol)   # CleansingRuleSet
ingestion_result = bronze_ingester.process(  # BronzePolarsIngester
    raw_records=raw_records,
    batch_id=batch_id,
    run_id=context.run_id,
    symbol=symbol,
    cleansing=cleansing,
)
# → DQ observations logged (non-blocking)
# → df.write_parquet → s3a://lakehouse/bronze/stock_prices/
```

### Phase Silver (per date)

```python
# SilverProcessor.transform_stock_prices()

df = duck.query_to_polars("SELECT * FROM read_parquet('s3a://...bronze...')")
# DuckDB đọc parquet từ MinIO

dedup_engine = DeduplicationEngine(keys=["symbol", "date"], strategy=KEEP_LAST)
deduped = dedup_engine.deduplicate(df_pd)

df = df.with_columns(
    pl.col("symbol").map_elements(lambda s: sk_gen.hash_key(s)).alias("stock_sk")
)
# → s3a://lakehouse/silver/fact_stock_price/
```

### Phase Gold

```python
# GoldProcessor.run_gold_models()
sqlmesh_engine.plan(environment="prod")
sqlmesh_engine.run(start=target_date, end=target_date)
sqlmesh_engine.audit()
# → SQLMesh chạy SQL models, xuất gold/mart_kpi_daily/
```

### Phase Serving

```python
# ServingSyncProcessor.sync_gold_to_postgres()
con.execute("INSTALL postgres; LOAD postgres;")
con.execute("ATTACH 'postgresql://...' AS pg_serving (TYPE POSTGRES)")
con.execute("""
    INSERT INTO pg_serving.gold.mart_kpi_daily
        SELECT * FROM read_parquet('s3a://...gold...', hive_partitioning=true)
    ON CONFLICT DO NOTHING
""")
```

---

## 3. Stack công nghệ

### 3.1 Python — Ngôn ngữ chính

Python được dùng xuyên suốt từ scraping, xử lý, orchestration đến testing.

**Tại sao Python?** Ecosystem data engineering phong phú nhất (pandas, polars,
duckdb, prefect, great_expectations đều có Python SDK).

### 3.2 Polars — DataFrame engine tốc độ cao

**Lý thuyết:**  
Polars là DataFrame library được viết bằng Rust, sử dụng Apache Arrow columnar
memory format. Khác với pandas (row-based), Polars xử lý theo cột — phù hợp
với analytics workload.

Hai chế độ thực thi:
- **Eager** (`pl.DataFrame`): tính ngay, giống pandas
- **Lazy** (`pl.LazyFrame`): xây dựng execution plan trước, tối ưu hóa rồi mới
  thực thi — tiết kiệm memory với dataset lớn

```python
# platforms/processing/polars/polars_engine.py

class PolarsEngine:
    def read_parquet(self, source_path: str) -> pl.LazyFrame:
        # scan_parquet = LAZY — chưa đọc dữ liệu vào memory
        return pl.scan_parquet(source_path, storage_options=self.config.storage_options)

    def write_parquet(self, df, target_path, partition_by=None):
        if isinstance(df, pl.LazyFrame):
            df = df.collect(streaming=self.config.enable_streaming)
            # streaming=True: xử lý từng batch, không load toàn bộ vào RAM
        df.write_parquet(target_path, ...)
```

**Trong dự án:** Polars là engine chính cho Bronze ingest và Silver
transformation, kết hợp với DuckDB để query từ S3/MinIO.

### 3.3 DuckDB — SQL query engine nhúng trong process

**Lý thuyết:**  
DuckDB là analytical SQL database chạy **trong-process** (embedded), không cần
server. Hỗ trợ đọc trực tiếp Parquet từ S3/MinIO qua extension `httpfs`.

Đây là "Swiss Army Knife" của data engineer hiện đại:
- Query Parquet files như table SQL bình thường
- `ATTACH` database ngoài (PostgreSQL, SQLite) để COPY data
- Dùng làm backend cho SQLMesh

```python
# platforms/processing/duckdb/duckdb_engine.py

def _connect(self) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(database=":memory:")  # in-memory, không cần file

    # Cấu hình để đọc file từ MinIO (tương thích S3 API)
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{s3_endpoint}';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")

def query_to_polars(self, sql: str) -> pl.LazyFrame:
    # Kết hợp DuckDB (SQL) + Polars (DataFrame) — best of both worlds
    return self.connection.execute(sql).pl()
```

**Pattern hay dùng trong dự án:**

```python
# Silver Processor đọc Bronze data qua DuckDB
df = duck.query_to_polars(
    f"SELECT * FROM read_parquet('{bronze_glob}')"
).collect()

# Serving: ATTACH PostgreSQL rồi INSERT trực tiếp từ Parquet
con.execute("ATTACH 'postgresql://...' AS pg_serving (TYPE POSTGRES, READ_WRITE)")
con.execute("""
    INSERT INTO pg_serving.gold.mart_kpi_daily
        SELECT * FROM read_parquet('s3a://lakehouse/gold/...', hive_partitioning=true)
    ON CONFLICT DO NOTHING
""")
```

### 3.4 SQLMesh — Data transformation với versioning

**Lý thuyết:**  
SQLMesh là thế hệ tiếp theo của dbt, tập trung vào:
- **Semantic understanding**: SQLMesh hiểu SQL, không chỉ template text
- **Virtual environments**: cách ly môi trường dev/prod mà không cần duplicate data
- **Incremental by default**: tự động xác định data cần recompute
- **Audits tích hợp**: viết test SQL ngay trong model file

SQLMesh dùng DuckDB làm local backend → developer có thể chạy toàn bộ pipeline
offline, không cần cloud.

```python
# platforms/processing/sqlmesh/sqlmesh_engine.py

class SqlMeshEngine:
    def _init_context(self) -> Context:
        return Context(paths=abs_path, gateway=self.config.gateway)
        # gateway="local_duckdb" → dùng DuckDB local khi develop

    def plan(self, environment="prod"):
        # SQLMesh diff: tìm thay đổi so với version trước → chỉ recompute cần thiết
        return self.context.plan(environment=environment)

    def run(self, environment, start, end):
        self.context.run(environment=environment, start=start, end=end)
        # Chạy incremental: chỉ process data trong khoảng [start, end]

    def audit(self):
        self.context.audit()
        # Chạy tất cả audit assertions trong model files
```

**Khi nào dùng SQLMesh vs dbt?**  
Dự án có cả 2. dbt được dùng cho `vietnam_stocks` staging/marts với PostgreSQL.
SQLMesh được dùng cho Gold layer vì cần incremental processing mạnh hơn và
môi trường cách ly tốt hơn.

### 3.5 Great Expectations — Data Quality Framework

**Lý thuyết:**  
Great Expectations (GE) là framework kiểm tra chất lượng dữ liệu dựa trên
"expectation" — một assertion về dữ liệu ở dạng có thể tái sử dụng và document.

**Triết lý trong dự án: DQ là quan sát, không phải chốt chặn.**

```
Trước đây (blocking):           Hiện tại (non-blocking):
  record fails rule               record fails rule
        │                               │
        ▼                               ▼
   REJECT record               LOG as WARNING
        │                       (GE observation)
        ▼                               │
   pipeline stalls             pipeline CONTINUES
                                        │
                                        ▼
                              DataProfile.ge_results
                              (→ Grafana báo cáo sau)
```

```python
# platforms/processing/base_processing_subsystem/subsystem1_data_profiling.py

def _run_ge_profiling(df, table_name, run_id, profile_config, logger):
    context = gx.get_context(mode="ephemeral")  # không cần config file
    ds = context.data_sources.add_pandas(name=f"ds_{table_name}_{run_id}")
    batch = ds.add_dataframe_asset("asset") \
              .add_batch_definition_whole_dataframe("batch") \
              .get_batch(batch_parameters={"dataframe": df})

    # Run expectation — KHÔNG raise exception khi fail
    vr = batch.validate(
        gx.expectations.ExpectColumnProportionOfNonNullValuesToBeBetween(
            column=col, min_value=0.5, max_value=1.0
        )
    )
    # Kết quả: {"success": False, "unexpected_count": 120, "element_count": 500}
    # → log WARNING, lưu vào DataProfile.ge_results
```

### 3.6 MinIO — Object Storage tương thích S3

**Lý thuyết:**  
MinIO là self-hosted object storage tương thích hoàn toàn với Amazon S3 API.
Trong data lakehouse, object storage thay thế HDFS — rẻ hơn, dễ scale hơn.

```
Path convention trong dự án:
s3a://lakehouse/
  bronze/
    stock_prices/
      ingest_date=2026-07-16/   ← Hive partitioning
        part-0001.parquet
  silver/
    fact_stock_price/
      ingest_date=2026-07-16/
        ...
  gold/
    mart_kpi_daily/
      date=2026-07-16/
        ...
```

**Hive partitioning** là kỹ thuật tổ chức file theo thư mục có key=value, giúp
DuckDB/Polars chỉ scan partition cần thiết thay vì toàn bộ table.

### 3.7 PostgreSQL — Serving Database

**Lý thuyết:**  
PostgreSQL là RDBMS dùng cho Serving Layer — nơi BI tools (Grafana, Metabase)
và application API kết nối vào.

Trong dự án, PostgreSQL **không** tham gia vào quá trình transformation (Bronze
→ Gold), chỉ nhận data cuối cùng từ Gold layer.

### 3.8 Prefect — Workflow Orchestration

**Lý thuyết:**  
Prefect là modern workflow orchestrator. Mỗi ETL pipeline được định nghĩa là
một `Flow`, bao gồm các `Task`. Prefect quản lý:
- Scheduling (chạy theo lịch)
- Retry logic (tự động thử lại khi lỗi)
- Logging & monitoring (UI dashboard)
- State management (RUNNING/SUCCESS/FAILED)

```python
# platforms/orchestration/prefect/flows/prefect_orchestra_etl.py

class PrefectETLPipelineConfig:
    """
    Centralized configuration manager cho ETL pipeline.
    Inject vào các flow qua Dependency Injection pattern.
    """
    def __init__(self, config_path, config_loader, logger_factory):
        self._loader = config_loader or FileConfigLoader()
        self._logger_factory = logger_factory or DefaultLoggerFactory()
        self._load_config()

    @property
    def cophieu68_extract_logger(self) -> logging.Logger:
        return self._get_logger("logger.ingestion_log.cophieu68.extract")
```

---

## 4. Extract — Thu thập dữ liệu

### Nguồn dữ liệu

Dự án thu thập dữ liệu từ **cophieu68.vn** — website chứng khoán Việt Nam.
Dữ liệu dạng HTML, được parse bằng BeautifulSoup.

Các loại dữ liệu:
- **Trading data**: lịch sử giá, khối lượng giao dịch
- **Company profile**: thông tin công ty niêm yết
- **Financial reports**: báo cáo tài chính theo quý/năm
- **Industry/sector**: phân loại ngành nghề

```python
# platforms/ingestion/cophieu68/extract/extract_cophieu68.py

class Cophieu68BeautifulSoupCrawler:
    def get_soup(self, url: str, retries: int = 3) -> Optional[BeautifulSoup]:
        for attempt in range(retries):
            try:
                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()
                response.encoding = "utf-8"
                soup = BeautifulSoup(response.text, "html.parser")
                time.sleep(self.delay)          # rate limiting — tránh bị block
                return soup
            except Exception as e:
                time.sleep(2 ** attempt)        # exponential backoff
                continue

class ExtractCophieu68(Cophieu68BeautifulSoupCrawler):
    def crawl_trading_data(self, symbol: str) -> Optional[Dict]:
        url = f"{self.urls}{self.endpoint['trading_data']}".format(symbol=symbol)
        # → "https://cophieu68.vn/quote/history.php?cP=1&id=FPT"
        soup = self.get_soup(url)
        # parse HTML table → List[Dict[str, Any]]
```

**DTO (Data Transfer Objects)** — dự án dùng dataclass để đảm bảo type safety
từ lúc extract:

```
platforms/ingestion/cophieu68/dto/
  extract_models.py    ← raw scraped data shapes
  transform_models.py  ← transformed data shapes
  load_models.py       ← database-ready shapes
```

---

## 5. Bronze Layer — Ingest thô vào Data Lake

**Nguyên tắc Bronze:** lưu raw as-is, thêm metadata tracking, không biến đổi
business logic.

```python
# scripts/deploy_full_pipeline.py — BronzePolarsIngester.process()

def process(self, raw_records, batch_id, run_id, symbol, cleansing):
    # 1. DQ observation (non-blocking)
    clean_records, reject_records = self._pre_evaluate(raw_records, cleansing, run_id)
    # → với non-blocking DQ: clean_records = tất cả records

    # 2. Convert sang Polars DataFrame
    df = pl.DataFrame(clean_records)

    # 3. Thêm metadata columns
    df = df.with_columns([
        pl.lit(batch_id).alias("_batch_id"),        # traceability
        pl.lit(run_id).alias("_run_id"),             # lineage
        pl.lit(datetime.now().isoformat()).alias("_ingest_timestamp"),
    ])

    # 4. Write to Bronze path trên MinIO (Hive partitioned)
    bronze_path = f"{self.base_path}/bronze/{self.table_name}/"
    saved_path = self.engine.write_parquet(
        df=df,
        target_path=bronze_path,
        partition_by=["ingest_date"]  # → /ingest_date=2026-07-16/
    )
```

**Batch ID vs Run ID:**
- `batch_id`: unique per (symbol, run) — tracing 1 lần crawl 1 symbol
- `run_id`: unique per pipeline execution — tracking toàn bộ pipeline

---

## 6. Subsystem xử lý cốt lõi

Dự án tổ chức logic xử lý theo **numbered subsystems**, mỗi subsystem là một
module độc lập với responsibility rõ ràng.

### Subsystem 1 — Data Profiling

Phân tích thống kê trên incoming data trước khi xử lý.

```python
# platforms/processing/base_processing_subsystem/subsystem1_data_profiling.py

@dataclass
class ColumnProfile:
    column_name:  str
    total_count:  int
    null_count:   int
    null_pct:     float   # tỷ lệ null
    unique_count: int
    min_value:    Any
    max_value:    Any
    sample_values: List[Any]

profile = DataProfiler.profile(
    profile_config,
    records,        # List[Dict]
    table_name="stock_prices",
    run_id="run_abc123",
)
# profile.columns      → List[ColumnProfile]
# profile.issues       → ["HIGH NULL RATE: close_price = 65%"]
# profile.ge_results   → [{"column":"close_price", "status":"FAIL", ...}]
```

### Subsystem 4 — Data Quality Pre-Evaluation

Định nghĩa và áp dụng business rules trên data.

```python
# platforms/processing/base_processing_subsystem/subsystem4_data_quality_pre_evaluate.py

# Định nghĩa rules bằng factory methods
ruleset = CleansingRuleSet(table_name="stock_prices")
ruleset.add_rule(CleansingRuleSet.rule_not_null("symbol"))
ruleset.add_rule(CleansingRuleSet.rule_numeric_range("close_price", min_val=0.0))
ruleset.add_rule(CleansingRuleSet.rule_regex("symbol", r"^[A-Z0-9]{2,10}$"))

# Apply rules — logs violations, KHÔNG reject
engine = DataCleansingEngine(rules=ruleset.rules)
result = engine.cleanse(records, source="cophieu68.trading")

# result.cleaned         → TẤT CẢ records (không reject)
# result.rejected        → [] (luôn empty)
# result.error_events    → [ErrorEvent(..., "[DQ_OBS] close_price <= 0")]
# result.dq_observations → [{"expectation":"numeric_range","passed":False,...}]

summary = result.summary()
# {"total_input":100, "total_cleaned":100, "rejection_rate":0,
#  "dq_failures":3, "dq_observations":5}
```

### Subsystem 7 — Deduplication

Xử lý duplicate records trong Silver layer.

```python
# platforms/processing/base_processing_subsystem/subsystem7_deduplication.py

class DeduplicationStrategy(str, Enum):
    KEEP_FIRST   = "KEEP_FIRST"    # giữ record đầu tiên
    KEEP_LAST    = "KEEP_LAST"     # giữ record cuối (latest wins)
    KEEP_MAX_COL = "KEEP_MAX_COL"  # giữ record có giá trị max của tiebreaker_col
    KEEP_MIN_COL = "KEEP_MIN_COL"

# Silver processor dùng KEEP_LAST với tiebreaker là ingest_timestamp
dedup_engine = DeduplicationEngine(
    keys=["symbol", "date"],                    # composite natural key
    strategy=DeduplicationStrategy.KEEP_LAST,
    tiebreaker_col="ingest_timestamp",          # nếu 2 records cùng key, giữ cái mới nhất
)
deduped_df = dedup_engine.deduplicate(df_pd)
stats = dedup_engine.last_stats
# stats.duplicates_removed = 15
# stats.duplicate_rate     = 0.03
```

**Cross-batch dedup** với `SurrogateKeyDeduplicator`:

```python
# Khi cùng 1 record xuất hiện ở nhiều batch khác nhau
deduper = SurrogateKeyDeduplicator()
new_records = deduper.filter_new(
    records,
    key_fields=["symbol", "date"],
    inject_key_as="natural_key_hash"  # thêm hash vào record
)
# deduper._seen = set() — track seen keys in-memory
```

### Subsystem 9 — SCD Manager (Slowly Changing Dimensions)

Quản lý lịch sử thay đổi của Dimension tables.

**Lý thuyết SCD:**

```
SCD Type 1 — Overwrite (không giữ lịch sử):
  Khi company thay đổi tên → ghi đè bản cũ

SCD Type 2 — Add row (giữ toàn bộ lịch sử):
  Khi company thay đổi sector:
    OLD: {symbol="FPT", sector="IT", is_current=True, end_date=NULL}
    → CLOSE: {is_current=False, end_date="2026-07-16"}
    → NEW:   {symbol="FPT", sector="Technology", is_current=True, end_date=NULL}
```

```python
# platforms/processing/base_processing_subsystem/subsystem9_and_25_scd_manage_and_version.py

@dataclass
class SCD2Result:
    to_close:   pd.DataFrame  # records cũ cần đóng (is_current=False)
    to_insert:  pd.DataFrame  # records mới cần thêm vào
    unchanged:  pd.DataFrame  # records không thay đổi

    def has_changes(self) -> bool:
        return len(self.to_close) > 0 or len(self.to_insert) > 0
```

### Subsystem 10 — Surrogate Key Generator

Tạo surrogate key cho dimension tables (thay thế natural key).

```python
# platforms/processing/base_processing_subsystem/subsystem10_surrogate_key_generator.py

gen = SurrogateKeyGenerator(prefix="STK_", key_length=32)

# Hash-based (deterministic): cùng input → cùng key
key = gen.hash_key("FPT")
# → "STK_a3f9d2c1..." (SHA-256 của "FPT", lấy 32 chars đầu)

key = gen.hash_key("FPT", "2026-07-16")
# → "STK_b7e3a1f2..." (SHA-256 của "FPT|2026-07-16")

# Batch: thêm cột surrogate key vào DataFrame
df = gen.add_hash_key_column(df, key_fields=["symbol"], output_col="company_key")
```

**Tại sao dùng surrogate key?**
- Natural key (`symbol="FPT"`) có thể thay đổi nghiệp vụ
- Surrogate key ổn định, không phụ thuộc business logic
- Tăng tốc JOIN trong data warehouse (int/hash vs varchar)

---

## 7. Silver Layer — Làm sạch và chuẩn hóa

Silver là trái tim của transformation pipeline. Tại đây:
1. Đọc Bronze data qua DuckDB (SQL trên Parquet)
2. Dedup với `DeduplicationEngine`
3. Gán surrogate key với `SurrogateKeyGenerator`
4. Thêm metadata columns `_silver_run_id`, `_silver_processed_at`
5. Write back xuống MinIO dưới dạng Parquet

```python
# scripts/deploy_full_pipeline.py — SilverProcessor.transform_stock_prices()

# Bước 1: Đọc Bronze qua DuckDB
bronze_glob = f"{self.base}/bronze/stock_prices/ingest_date={target_date}/*.parquet"
df = self.duck.query_to_polars(
    f"SELECT * FROM read_parquet('{bronze_glob}')"
).collect()

# Bước 2: Dedup
dedup_engine = DeduplicationEngine(
    keys=["symbol", "date"],
    strategy=DeduplicationStrategy.KEEP_LAST,
    tiebreaker_col="ingest_timestamp",
)
deduped_pd = dedup_engine.deduplicate(df.to_pandas())

# Bước 3: Surrogate key
df = pl.from_pandas(deduped_pd)
df = df.with_columns(
    pl.col("symbol")
      .map_elements(lambda sym: self.sk_gen.hash_key(sym), return_dtype=pl.Utf8)
      .alias("stock_sk")
)

# Bước 4: Metadata + Write
df = df.with_columns([
    pl.lit(run_id).alias("_silver_run_id"),
    pl.lit(datetime.now().isoformat()).alias("_silver_processed_at"),
])
self.polars.write_parquet(df, f"{self.base}/silver/fact_stock_price/", partition_by=["ingest_date"])
```

---

## 8. Gold Layer — Modeling với SQLMesh

Gold layer chứa aggregation và business metrics, được build bằng SQL models.

**dbt** cũng được dùng cho Vietnam Stocks domain với schema tổ chức:

```
platforms/processing/dbt/models/vietnam_stocks/
  staging/     ← views: rename, cast, light cleaning
  marts/       ← tables: final business metrics
  schema.yml   ← documentation + tests
  sources.yml  ← declare source tables
```

**dbt model materialization:**

```yaml
# dbt_project.yml
models:
  my_dbt_project:
    vietnam_stocks:
      staging:
        +materialized: view      # staging = views (tái tính mỗi lần query)
      marts:
        +materialized: table     # marts = tables (materialize một lần)
```

**SQLMesh** thực hiện:

```python
# GoldProcessor.run_gold_models()

# plan() — diff so với trạng thái trước
plan_result = self.sqlmesh.plan(environment="prod")
# SQLMesh hiểu: "model mart_kpi_daily phụ thuộc fact_stock_price,
#  fact_stock_price đã thay đổi → cần recompute mart_kpi_daily"

# run() — thực thi incremental
self.sqlmesh.run(start="2026-07-16", end="2026-07-16")
# Chỉ tính lại data của ngày 2026-07-16, không rebuild toàn bộ table

# audit() — chạy assertions
self.sqlmesh.audit()
# Ví dụ audit: "not_null(close_price)", "unique(symbol, date)"
```

---

## 9. Serving Layer — Đưa dữ liệu ra PostgreSQL

Serving layer sync Gold data vào PostgreSQL để BI tools query.

**Kỹ thuật DuckDB ATTACH:** đây là cách "zero-ETL" copy data — DuckDB đọc
Parquet từ S3 và ghi thẳng vào PostgreSQL mà không cần Python dataframe trung gian.

```python
# ServingSyncProcessor.sync_gold_to_postgres()

con = self.duck.connection
con.execute("INSTALL postgres; LOAD postgres;")

# ATTACH PostgreSQL như một remote database
pg_conn = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
con.execute(f"ATTACH '{pg_conn}' AS pg_serving (TYPE POSTGRES, READ_WRITE)")

# INSERT trực tiếp: Parquet on S3 → PostgreSQL
con.execute("""
    INSERT INTO pg_serving.gold.mart_kpi_daily
        SELECT * FROM read_parquet(
            's3a://lakehouse/gold/mart_kpi_daily/**/*.parquet',
            hive_partitioning=true
        )
    ON CONFLICT DO NOTHING
""")
```

---

## 10. Orchestration — Prefect & Master Orchestrator

### Master Pipeline Orchestrator

Đây là entry point chính của toàn bộ pipeline:

```python
# scripts/deploy_full_pipeline.py — CLI usage

python deploy_full_pipeline.py full --symbols FPT VNM HPG --date 2026-07-16
python deploy_full_pipeline.py bronze --symbols FPT VNM
python deploy_full_pipeline.py silver --date 2026-07-16
python deploy_full_pipeline.py gold   --date 2026-07-16
python deploy_full_pipeline.py validate
```

**ExecutionContext** là "state carrier" — mang toàn bộ context của 1 pipeline run:

```python
@dataclass
class ExecutionContext:
    run_id:        str              # "run_20260716_143022_a3b4c5"
    phase:         ExecutionPhase   # BRONZE | SILVER | GOLD | SERVING | FULL
    symbols:       List[str]        # ["FPT", "VNM", "HPG"]
    backend:       ProcessingBackend # POLARS | DUCKDB | SQLMESH
    target_date:   str              # "2026-07-16"
    environment:   str              # "prod" | "dev"
    metadata_repo: MetadataRepository
    error_log:     ErrorEventLog
    logger:        logging.Logger
```

### Prefect Configuration

```yaml
# platforms/orchestration/prefect/config/cophieu68_config.yaml

project_params:
  sources:
    cophieu68:
      base_url: "https://cophieu68.vn"
      endpoints:
        trading_data: "/quote/history.php?cP={page}&id={symbol}"
      rate_limit_per_minute: 60
      retry:
        max_attempts: 3
        backoff_seconds: 30

  http:
    delay_seconds: 0.3   # rate limiting để tránh ban IP
    timeout_seconds: 30
```

---

## 11. Observability — DQ, Lineage, Error Events, Metadata

### Subsystem 5 — Error Event Schema

Mọi lỗi và DQ violation đều được chuẩn hóa thành `ErrorEvent`:

```python
# subsystem5_and_30_error_event_schema_and_escalate.py

@dataclass
class ErrorEvent:
    err_id:        str           # SHA-256 hash(run_id|record_id|message)[:16]
    run_id:        str
    job_name:      str           # "cophieu68.trading_data"
    error_level:   ErrorLevel    # WARNING | ERROR | FATAL
    error_message: str
    record_id:     Optional[str] # natural key của record bị lỗi
    raw_json:      Optional[str] # JSON dump của raw record

class ErrorLevel(str, Enum):
    WARNING = "WARNING"   # DQ observations
    ERROR   = "ERROR"     # recoverable errors
    FATAL   = "FATAL"     # unrecoverable, pipeline stops
```

### Subsystem 29 — Data Lineage

Track nguồn gốc data: data này từ đâu, qua những bước nào.

```python
# subsystem29_data_lineage.py

@dataclass
class LineageRecord:
    lineage_id:    str
    run_id:        str
    source_layer:  str   # "external" | "bronze" | "silver"
    source_table:  str   # "cophieu68_FPT"
    target_layer:  str   # "bronze" | "silver" | "gold"
    target_table:  str   # "bronze_stock_prices"
    operation:     str   # "APPEND" | "MERGE" | "SCD2" | "OVERWRITE"
    rows_affected: int
    recorded_at:   datetime

# Được gọi sau mỗi phase:
context.metadata_repo.log_lineage(
    run_id=context.run_id,
    source_layer="bronze", source_table="bronze_stock_prices",
    target_layer="silver", target_table="silver_fact_stock_price",
    operation="MERGE",
    rows_affected=silver_result["rows_out"],
)
```

### Subsystem 34 — Metadata Repository

Central hub cho tất cả metadata của pipeline:

```python
# subsystem34_metadata_repo.py

class MetadataRepository:
    """
    Tổng hợp subsystem 22 (Job Scheduler), 27 (Workflow Monitor),
    29 (Lineage), 5 (Error Events) vào 1 interface duy nhất.
    """

    # Tracking ETL run lifecycle
    def start_run(self, job_name, layer, table_name, run_id) -> str: ...
    def end_run(self, run_id, status, rows_written) -> None: ...

    # Logging
    def log_error(self, run_id, job, level, message, record) -> None: ...
    def log_quality_check(self, run_id, table, check_name, status, ...) -> None: ...
    def log_lineage(self, run_id, source_layer, source_table, ...) -> None: ...
```

**Pattern context manager:**

```python
with repo.run_context(job_name="extract_FPT", layer="bronze") as run_id:
    records = extractor.crawl(symbol="FPT")
    # nếu exception → repo tự động end_run(status="FAILED")
    # nếu thành công → repo end_run(status="SUCCESS")
```

### Logging System

Dự án dùng structured JSON logging:

```json
{
  "time": "2026-07-16T20:38:21",
  "level": "WARNING",
  "message": "[DQ_OBS] Field 'close_price' = -5.0 below min 0.0 | record_id=FPT|2026-07-16",
  "caller": "subsystem5_and_30_error_event_schema_and_escalate.py:114"
}
```

Log files được phân tầng theo source và level:

```
logger_storage/
  ingestion/cophieu68/
    extract_info.log
    extract_warning.log
    extract_error.log
  storage/
    mongodb_info.log
    postgresql_error.log
```

---

## 12. Cấu trúc thư mục dự án

```
ETL_Project/
├── scripts/
│   └── deploy_full_pipeline.py       ← Entry point CLI chính
│
├── platforms/
│   ├── ingestion/
│   │   └── cophieu68/
│   │       ├── extract/
│   │       │   └── extract_cophieu68.py   ← BeautifulSoup scraper
│   │       └── dto/                       ← Data Transfer Objects
│   │
│   ├── processing/
│   │   ├── base_processing_subsystem/     ← Core subsystems (1,4,7,9,10,22,27,29,34)
│   │   │   ├── subsystem1_data_profiling.py
│   │   │   ├── subsystem4_data_quality_pre_evaluate.py
│   │   │   ├── subsystem7_deduplication.py
│   │   │   ├── subsystem9_and_25_scd_manage_and_version.py
│   │   │   ├── subsystem10_surrogate_key_generator.py
│   │   │   ├── subsystem5_and_30_error_event_schema_and_escalate.py
│   │   │   ├── subsystem29_data_lineage.py
│   │   │   ├── subsystem34_metadata_repo.py
│   │   │   └── config/                    ← YAML configs cho từng subsystem
│   │   │
│   │   ├── polars/
│   │   │   └── polars_engine.py           ← Polars read/write wrapper
│   │   ├── duckdb/
│   │   │   └── duckdb_engine.py           ← DuckDB query + S3 wrapper
│   │   ├── sqlmesh/
│   │   │   └── sqlmesh_engine.py          ← SQLMesh plan/run/audit wrapper
│   │   ├── dbt/
│   │   │   ├── dbt_project.yml
│   │   │   └── models/
│   │   │       └── vietnam_stocks/
│   │   │           ├── staging/           ← views
│   │   │           └── marts/             ← tables
│   │   └── spark/                         ← (future: large-scale processing)
│   │
│   ├── orchestration/
│   │   └── prefect/
│   │       ├── config/
│   │       │   └── cophieu68_config.yaml  ← pipeline configuration
│   │       └── flows/
│   │           └── prefect_orchestra_etl.py
│   │
│   └── storage/
│       ├── lake_storage/                  ← MinIO/Delta Lake connectors
│       └── datawarehouse/                 ← PostgreSQL connectors
│
├── shared/
│   ├── logger/
│   │   └── python_main_logger.py         ← Centralized JSON logger
│   ├── events/                           ← Shared event schemas
│   └── utils/                            ← Common utilities
│
├── tests/
│   ├── unit/                             ← Unit tests mỗi subsystem
│   └── intergration/                     ← Integration tests
│
├── docs/                                 ← Additional documentation
├── infra/                                ← Infrastructure configs
├── ARCHITECTURE.md                       ← (file này)
├── pyproject.toml
├── requirements_*.txt
└── Makefile
```

---

## 13. Thiết kế pattern quan trọng

### Pattern 1: Dependency Injection

Thay vì hardcode logger hay config loader, inject qua constructor:

```python
class DataCleansingEngine:
    def __init__(
        self,
        rules=None,
        rejection_level=ErrorLevel.WARNING,
        run_profiling=True,
        logger=None,               # ← inject, không hardcode
    ):
        self.logger = logger or logging.getLogger(__name__)
```

**Lợi ích:** Dễ test (inject mock logger), dễ swap implementation.

### Pattern 2: Config-driven behavior

Hành vi của subsystem được điều khiển hoàn toàn bởi YAML, không hardcode trong code:

```yaml
# config/data_profiling_config.yaml
data_profiling:
  null_pct_fail_threshold: 0.5      # ← thay đổi ngưỡng không cần sửa code
  great_expectations:
    mostly_not_null: 0.5
    numeric_ranges:
      close_price: { min: 0, max: 1000000 }
```

```python
fail_threshold = profile_config.get("null_pct_fail_threshold", 0.5)  # với default
```

### Pattern 3: Dataclass as schema

Dùng `@dataclass` thay vì dict để có type safety và documentation tự động:

```python
@dataclass
class DeduplicationStats:
    source:             str
    run_id:             str
    total_input:        int
    total_output:       int
    duplicates_removed: int
    strategy:           str
    keys:               List[str]
    deduped_at:         datetime

    @property
    def duplicate_rate(self) -> float:
        return round(self.duplicates_removed / self.total_input, 4) if self.total_input else 0.0
```

### Pattern 4: Non-blocking DQ (Observation over Rejection)

DQ failures không dừng pipeline, chỉ log để phân tích:

```python
# DataCleansingEngine.cleanse() — KHÔNG reject record

for record in records:
    for rule in self.rules:
        is_valid, message = rule(record)
        if not is_valid:
            error_log.add(ErrorLevel.WARNING, f"[DQ_OBS] {message}")
            # ← log WARNING, KHÔNG break, KHÔNG reject

# ALL records pass through
return CleansingResult(
    cleaned        = list(records),    # ← tất cả
    rejected       = [],               # ← luôn rỗng
    total_rejected = 0,
    dq_observations = ge_results,      # ← để report sau
)
```

### Pattern 5: Template Method với YAML schema

Serialize object theo schema định nghĩa trong YAML:

```python
def to_dict(self) -> Dict[str, Any]:
    context = asdict(self)  # flat dict của dataclass
    schema = DEDUP_CONFIG.get("stats_schema")
    record = {}
    for field_name, template in schema.items():
        if "{" in template:
            record[field_name] = template.format(**context)  # f-string style
        elif template in context:
            record[field_name] = context[template]           # key lookup
    return record
```

---

## 14. Roadmap kỹ năng cho fresher

Dưới đây là thứ tự học tốt nhất để tiếp cận dự án này:

### Giai đoạn 1: Foundation (Tuần 1–2)

1. **Python dataclasses & type hints** — đọc `subsystem7_deduplication.py`
2. **pandas basics** — hiểu DataFrame operations trong các subsystems
3. **YAML config** — đọc `cophieu68_config.yaml` và `data_profiling_config.yaml`
4. **BeautifulSoup** — đọc `extract_cophieu68.py`

**Bài tập:** Viết một script crawl 1 URL HTML bằng BeautifulSoup, parse table,
lưu thành `List[Dict]`.

### Giai đoạn 2: Processing (Tuần 3–4)

1. **Polars** — chạy `polars_engine.py`, thử LazyFrame vs DataFrame
2. **DuckDB** — mở Python REPL, `import duckdb`, query một file CSV như SQL
3. **Parquet format** — hiểu tại sao Parquet tốt hơn CSV cho analytics
4. **Hive partitioning** — tạo folder structure `date=2026-07-16/`, query bằng DuckDB

**Bài tập:** Đọc Bronze parquet từ disk bằng DuckDB SQL, group by symbol, write
kết quả ra Silver parquet.

### Giai đoạn 3: Data Quality (Tuần 5)

1. **Great Expectations** — chạy `_run_ge_profiling()` trong subsystem 1
2. **CleansingRuleSet** — viết thêm rule mới (ví dụ: `rule_date_not_future`)
3. **pytest** — chạy `tests/unit/test_subsystem4_data_cleansing.py`

**Bài tập:** Thêm rule `rule_date_not_future("date")` vào `CleansingRuleSet`,
viết unit test.

### Giai đoạn 4: Warehouse Patterns (Tuần 6–7)

1. **Surrogate Keys** — hiểu tại sao SHA-256 hash, đọc `subsystem10_surrogate_key_generator.py`
2. **SCD Type 1 & 2** — đọc `subsystem9_and_25_scd_manage_and_version.py`
3. **Deduplication strategies** — hiểu KEEP_FIRST vs KEEP_LAST vs KEEP_MAX_COL
4. **dbt models** — đọc `vietnam_stocks/` models, hiểu staging vs marts

**Bài tập:** Implement SCD1 merge bằng pandas: given current_df và incoming_df,
return (to_update, to_insert, unchanged).

### Giai đoạn 5: Orchestration (Tuần 8)

1. **SQLMesh** — chạy `sqlmesh_engine.py`, hiểu plan() vs run()
2. **ExecutionContext pattern** — đọc `deploy_full_pipeline.py`
3. **End-to-end run** — chạy `python scripts/deploy_full_pipeline.py validate`
4. **Prefect flow** — đọc `prefect_orchestra_etl.py`

### Giai đoạn 6: Senior mindset

1. **Lineage tracking** — tại sao cần `LineageRecord`?
2. **Metadata repository** — `subsystem34_metadata_repo.py` kết nối tất cả
3. **Non-blocking DQ** — tại sao không reject records? Trade-off là gì?
4. **Medallion Architecture** — vẽ lại luồng từ source → serving cho 1 symbol

---

## Quick Reference

### Chạy pipeline

```bash
# Full pipeline
python scripts/deploy_full_pipeline.py full --symbols FPT VNM --date 2026-07-16

# Từng phase
python scripts/deploy_full_pipeline.py bronze --symbols FPT
python scripts/deploy_full_pipeline.py silver --date 2026-07-16
python scripts/deploy_full_pipeline.py gold   --date 2026-07-16
python scripts/deploy_full_pipeline.py serving --date 2026-07-16

# Validate config
python scripts/deploy_full_pipeline.py validate
```

### Chạy tests

```bash
pytest tests/unit/ -v                          # Tất cả unit tests
pytest tests/unit/test_subsystem1_data_profiling.py -v
pytest tests/unit/test_subsystem4_data_cleansing.py -v
pytest tests/ -v --tb=short                   # Tất cả tests, short traceback
```

### Environment variables

```bash
LAKEHOUSE_BASE_PATH=s3a://lakehouse
S3_ENDPOINT=http://localhost:9000
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin_secure_123
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=etl_project
POSTGRES_USER=...
POSTGRES_PASSWORD=...
```

---

*Tài liệu này được tạo từ source code thực tế của dự án tại commit hiện tại.
Cập nhật khi có thay đổi kiến trúc lớn.*
