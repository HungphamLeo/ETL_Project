-- Gold: fact_trading_ohlcv
-- Denormalized daily OHLCV enriched with company_name, industry_name, market_type.
-- Reads from: silver/fact_stock_price (partitioned by year/month)
-- Target:     gold/fct_trading_ohlcv  (partitioned by year/month)
-- Registry:   GOLD_FCT_TRADING_OHLCV
--
-- NOTE về dim_market_type:
--   silver/dim_market_type không có cột symbol — đây là bảng dim toàn thị trường.
--   JOIN thông qua silver/dim_industry (company có cột industry_code → industry có market_type_code).
--   Đơn giản hoá: lấy market_type trực tiếp từ dim_industry nếu có, hoặc để NULL.

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

  -- Denormalize from dim_company (latest snapshot, is_current có thể là boolean hoặc string)
  c.full_name                                        AS company_name,

  -- Denormalize from dim_industry (latest snapshot by symbol)
  i.industry_name,

  -- market_type lấy từ dim_industry qua JOIN symbol (dim_market_type không có symbol column)
  -- Nếu dim_industry không có market_type, giá trị sẽ là NULL (safe với LEFT JOIN)
  i.market_type_code                                 AS market_type,

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

  -- Partition columns (phải tồn tại trong silver — được thêm bởi SilverProcessor)
  t.year,
  t.month,

  CURRENT_TIMESTAMP                    AS _updated_at

FROM read_parquet(
  's3://lakehouse/silver/fact_stock_price/**/*.parquet',
  hive_partitioning = true
) AS t

-- LEFT JOIN dim_company để lấy company_name
LEFT JOIN (
  SELECT symbol, full_name
  FROM read_parquet(
    's3://lakehouse/silver/dim_company/**/*.parquet',
    hive_partitioning = true
  )
  WHERE COALESCE(TRY_CAST(is_current AS BOOLEAN), true) = true
) AS c ON t.symbol = c.symbol

-- LEFT JOIN dim_industry để lấy industry_name và market_type_code
-- dim_industry có cột symbol (mỗi company thuộc một ngành)
LEFT JOIN (
  SELECT DISTINCT symbol, industry_name, market_type_code
  FROM read_parquet(
    's3://lakehouse/silver/dim_industry/**/*.parquet',
    hive_partitioning = true
  )
  WHERE COALESCE(TRY_CAST(is_current AS BOOLEAN), true) = true
) AS i ON t.symbol = i.symbol
