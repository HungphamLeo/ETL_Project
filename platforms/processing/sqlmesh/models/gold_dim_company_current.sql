-- Gold: dim_company_current
-- Current-only snapshot of silver/dim_company (is_current = true).
-- Optimized for Power BI slicers and dimension lookups — no SCD2 history needed.
-- Registry: GOLD_DIM_COMPANY_CURRENT
--
-- NOTE: Bronze flatten dict → silver có thể chỉ có full_name (không có company_name riêng).
-- Dùng TRY_CAST + COALESCE để tránh lỗi khi cột không tồn tại trong Parquet schema.

MODEL (
  name gold.dim_company_current,
  kind FULL,
  cron '@daily',
  grain (company_key),
  description 'Current company snapshot. Optimized for Power BI slicers.'
);

SELECT
  company_key,
  symbol,
  -- full_name là tên đầy đủ từ Bronze; alias sang company_name cho BI layer
  full_name                            AS company_name,
  full_name,
  english_name,
  short_name,
  address,
  website,
  email_address,
  established_date,
  listed_date,
  listed_volume,
  market_capitalization,
  effective_date,
  CURRENT_TIMESTAMP                    AS _updated_at

FROM read_parquet(
  's3://lakehouse/silver/dim_company/**/*.parquet',
  hive_partitioning = true
)
WHERE COALESCE(TRY_CAST(is_current AS BOOLEAN), true) = true
