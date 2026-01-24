select
    symbol,
    'ANNUAL'::text as time_report_type,
    'income_statement'::text as financial_report_type,
    cast(year as int) as year,
    null::text as period,
    metric_code,
    cast(metric_value as numeric(20,4)) as metric_value,
    cast(update_time as timestamp) as update_time
from {{ source('dw', 'fact_income_statement_annually') }}
