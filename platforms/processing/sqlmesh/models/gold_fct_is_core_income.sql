-- Gold: fct_is_core_income
-- Income statement core income/expense per symbol/period.
-- Silver stores raw JSON blobs from cophieu68 HTML tables.
-- Registry: GOLD_FCT_IS_CORE_INCOME

MODEL (
  name gold.fct_is_core_income,
  kind FULL,
  cron '@daily',
  grain (symbol, time_report_type, year),
  partitioned_by (time_report_type),
  description 'Core income statement per symbol/period. For Power BI P&L analysis.'
);

SELECT
  symbol,
  time_report_type,
  report_type                                    AS financial_report_type,
  COALESCE(
    TRY_CAST(year AS VARCHAR),
    LEFT(ingest_date, 4)
  )                                              AS year,
  NULL                                           AS period,

  -- Income columns — placeholders until full metric normalization is done in Gold
  TRY_CAST(NULL AS DECIMAL(30,4))                AS net_interest_income,
  TRY_CAST(NULL AS DECIMAL(30,4))                AS profit_before_tax,
  TRY_CAST(NULL AS DECIMAL(30,4))                AS profit_after_tax,

  CURRENT_TIMESTAMP                              AS update_time,
  CURRENT_TIMESTAMP                              AS _updated_at

FROM read_parquet(
  's3://lakehouse/silver/fact_income_statement/**/*.parquet',
  hive_partitioning = true
)
