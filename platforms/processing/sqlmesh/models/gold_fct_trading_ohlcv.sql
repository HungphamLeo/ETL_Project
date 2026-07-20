-- Gold: fact_trading_ohlcv
-- Denormalized daily OHLCV enriched with company_name, industry_name, market_type.
-- Reads from: silver/fact_stock_price (partitioned by year/month)
-- Target:     gold/fct_trading_ohlcv  (partitioned by year/month)
-- Registry:   GOLD_FCT_TRADING_OHLCV
--
-- Schema thực tế của silver dims:
--   dim_industry   (từ CompanyBelongToIndustrySector):
--       symbol, industry_code, industry_name, company_name, close_price, ...
--       → KHÔNG có market_type_code
--   dim_market_type (từ CompanyBelongToMarketType):
--       symbol, market_type_code, market_type_name, company_name, ...
--       → CÓ symbol → JOIN trực tiếp với fact_stock_price

MODEL (
  name gold.fct_trading_ohlcv,
  kind FULL,
  cron '@daily',
  grain (trade_key),
  partitioned_by (year, month),
  description 'Optimized daily OHLCV for Power BI. Includes denormalized company/industry/market dims.'
);

SELECT
  t.trade_key,
  t.symbol,

  -- Denormalize company_name từ dim_company (is_current snapshot)
  c.full_name                          AS company_name,

  -- Denormalize industry_name từ dim_industry (per-symbol)
  i.industry_name,

  -- Denormalize market_type từ dim_market_type (per-symbol, có cột market_type_code)
  m.market_type_code                   AS market_type,

  -- Trading columns — all arrive as Utf8 from Bronze/Silver, cast on the way out
  TRY_CAST(t.date AS DATE)             AS trade_date,
  TRY_CAST(t.close_price AS DOUBLE)    AS close_price,
  TRY_CAST(t.open_price  AS DOUBLE)    AS open_price,
  TRY_CAST(t.high_price  AS DOUBLE)    AS high_price,
  TRY_CAST(t.low_price   AS DOUBLE)    AS low_price,
  TRY_CAST(t.volume      AS BIGINT)    AS volume,
  TRY_CAST(t.foreign_buy AS BIGINT)    AS foreign_buy,
  TRY_CAST(t.foreign_sell AS BIGINT)   AS foreign_sell,
  TRY_CAST(t.foreign_value AS DOUBLE)  AS foreign_net_value,

  -- Partition columns (được thêm bởi SilverProcessor)
  t.year,
  t.month,

  CURRENT_TIMESTAMP                    AS _updated_at

FROM read_parquet(
  's3://lakehouse/silver/fact_stock_price/**/*.parquet',
  hive_partitioning = true
) AS t

-- dim_company: lấy full_name theo symbol (is_current snapshot)
LEFT JOIN (
  SELECT symbol, full_name
  FROM read_parquet(
    's3://lakehouse/silver/dim_company/**/*.parquet',
    hive_partitioning = true
  )
  WHERE COALESCE(TRY_CAST(is_current AS BOOLEAN), true) = true
) AS c ON t.symbol = c.symbol

-- dim_industry: lấy industry_name theo symbol
-- Schema: symbol, industry_code, industry_name (KHÔNG có market_type_code)
LEFT JOIN (
  SELECT DISTINCT symbol, industry_name
  FROM read_parquet(
    's3://lakehouse/silver/dim_industry/**/*.parquet',
    hive_partitioning = true
  )
  WHERE COALESCE(TRY_CAST(is_current AS BOOLEAN), true) = true
) AS i ON t.symbol = i.symbol

-- dim_market_type: lấy market_type_code theo symbol
-- Schema: symbol, market_type_code, market_type_name (CÓ symbol — crawled per-company)
LEFT JOIN (
  SELECT DISTINCT symbol, market_type_code
  FROM read_parquet(
    's3://lakehouse/silver/dim_market_type/**/*.parquet',
    hive_partitioning = true
  )
) AS m ON t.symbol = m.symbol
