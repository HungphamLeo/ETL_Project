select
    symbol,
    'QUARTERLY'::text as time_report_type,
    'balance_sheet'::text as financial_report_type,
    cast(year as int) as year,
    cast(period as text) as period,

    metric_code,
    cast(metric_value as numeric(20,4)) as metric_value,

    {{ col_or_null('dw', 'fact_balance_sheet_quarterly', 'metric_name_vi_raw', 'text') }} as metric_name_vi_raw,
    {{ col_or_null('dw', 'fact_balance_sheet_quarterly', 'metric_name_en', 'text') }}     as metric_name_en,
    {{ col_or_null('dw', 'fact_balance_sheet_quarterly', 'metric_group', 'text') }}       as metric_group,

    cast(update_time as timestamp) as update_time
from {{ source('dw', 'fact_balance_sheet_quarterly') }}
