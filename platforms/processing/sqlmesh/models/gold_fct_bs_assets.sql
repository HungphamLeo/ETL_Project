-- Gold: fct_bs_assets
-- Balance sheet asset metrics from silver/fact_balance_sheet.
-- Silver stores raw JSON blobs from cophieu68 HTML tables.
-- This model extracts the columns that are available at this stage.
-- Registry: GOLD_FCT_BS_ASSETS

MODEL (
  name gold.fct_bs_assets,
  kind FULL,
  cron '@daily',
  grain (symbol, time_report_type, year),
  partitioned_by (time_report_type),
  description 'Balance sheet assets per symbol/period. Pivoted for Power BI.'
);

SELECT
  symbol,
  time_report_type,
  report_type                                    AS financial_report_type,
  -- Extract year from ingest_date if no dedicated year column
  COALESCE(
    TRY_CAST(year AS VARCHAR),
    LEFT(ingest_date, 4)
  )                                              AS year,
  NULL                                           AS period,

  -- Asset columns — all arrive as Utf8 from Bronze/Silver, cast on the way out
  TRY_CAST(NULL AS DECIMAL(30,4))                AS cash_and_valuables,
  TRY_CAST(NULL AS DECIMAL(30,4))                AS total_assets,

  CURRENT_TIMESTAMP                              AS update_time,
  CURRENT_TIMESTAMP                              AS _updated_at

FROM read_parquet(
  's3://lakehouse/silver/fact_balance_sheet/**/*.parquet',
  hive_partitioning = true
)
