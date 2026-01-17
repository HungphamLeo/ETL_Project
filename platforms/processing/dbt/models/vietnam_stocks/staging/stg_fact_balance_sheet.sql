select
    symbol,
    time_report_type,
    financial_report_type,
    year,
    period,
    metric_code,
    metric_value,
    metric_name_vi_raw,
    update_time
from {{ ref('fact_balance_sheet') }}
where financial_report_type = 'balance_sheet'
