-- Gold: fact_trading_ohlcv
-- Denormalized daily OHLCV enriched with company_name, industry_name, market_type.
-- Reads from: silver/fact_stock_price (partitioned by year/month)
-- Target:     gold/fct_trading_ohlcv  (partitioned by year/month)
-- Registry:   GOLD_FCT_TRADING_OHLCV

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

  -- Denormalize from dim_company (latest snapshot)
  c.full_name                          AS company_name,

  -- Denormalize from dim_industry (latest snapshot by symbol)
  i.industry_name,

  -- Denormalize from dim_market_type (latest snapshot by symbol)
  m.market_type,

  -- Trading columns — keep Utf8 and cast numerics
  TRY_CAST(t.date AS DATE)             AS trade_date,
  TRY_CAST(t.close_price AS DOUBLE)    AS close_price,
  TRY_CAST(t.open_price  AS DOUBLE)    AS open_price,
  TRY_CAST(t.high_price  AS DOUBLE)    AS high_price,
  TRY_CAST(t.low_price   AS DOUBLE)    AS low_price,
  TRY_CAST(t.volume      AS BIGINT)    AS volume,
  TRY_CAST(t.foreign_buy AS BIGINT)    AS foreign_buy,
  TRY_CAST(t.foreign_sell AS BIGINT)   AS foreign_sell,
  TRY_CAST(t.foreign_value AS DOUBLE)  AS foreign_net_value,

  -- Partition columns
  t.year,
  t.month,

  CURRENT_TIMESTAMP                    AS _updated_at

FROM read_parquet(
  's3://lakehouse/silver/fact_stock_price/**/*.parquet',
  hive_partitioning = true
) AS t

-- LEFT JOIN dim_company for company_name
LEFT JOIN (
  SELECT symbol, full_name
  FROM read_parquet(
    's3://lakehouse/silver/dim_company/**/*.parquet',
    hive_partitioning = true
  )
  WHERE is_current = true
) AS c ON t.symbol = c.symbol

-- LEFT JOIN dim_industry for industry_name (distinct industry per symbol)
LEFT JOIN (
  SELECT DISTINCT symbol, industry_name
  FROM read_parquet(
    's3://lakehouse/silver/dim_industry/**/*.parquet',
    hive_partitioning = true
  )
  WHERE is_current = true
) AS i ON t.symbol = i.symbol

-- LEFT JOIN dim_market_type for market_type (distinct per symbol)
LEFT JOIN (
  SELECT DISTINCT symbol, market_name AS market_type
  FROM read_parquet(
    's3://lakehouse/silver/dim_market_type/**/*.parquet',
    hive_partitioning = true
  )
) AS m ON t.symbol = m.symbol
