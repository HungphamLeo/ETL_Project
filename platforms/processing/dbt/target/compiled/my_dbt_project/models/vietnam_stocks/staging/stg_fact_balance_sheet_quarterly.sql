select
    symbol,
    'QUARTERLY'::text as time_report_type,
    'balance_sheet'::text as financial_report_type,
    cast(year as int) as year,
    cast(period as text) as period,

    metric_code,
    cast(metric_value as numeric(20,4)) as metric_value,

    null::text as metric_name_vi_raw,
    "metric_name_en"     as metric_name_en,
    "metric_group"       as metric_group,

    cast(update_time as timestamp) as update_time
from "etl_project"."dw"."fact_balance_sheet_quarterly"