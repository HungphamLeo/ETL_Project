
  create view "etl_project"."dw_stg"."stg_fact_income_statement_quarterly__dbt_tmp"
    
    
  as (
    select
    symbol,
    'QUARTERLY'::text as time_report_type,
    'income_statement'::text as financial_report_type,
    cast(year as int) as year,
    cast(period as text) as period,
    metric_code,
    cast(metric_value as numeric(20,4)) as metric_value,
    cast(update_time as timestamp) as update_time
from "etl_project"."dw"."fact_income_statement_quarterly"
  );