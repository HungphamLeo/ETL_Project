select
    symbol,
    time_report_type,
    financial_report_type,
    cast(year as int) as year,
    -- period có thể là Quarter_1/Quarter_2 hoặc Q1/Q2 hoặc 1..4 => normalize về text luôn cho an toàn
    cast(period as text) as period,

    metric_code,
    cast(metric_value as numeric(20,4)) as metric_value,
    cast(update_time as timestamp) as update_time
from {{ ref('fact_income_statement') }}
where financial_report_type = 'income_statement'
