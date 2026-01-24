with fact as (
    select *
    from "etl_project"."dw_stg"."stg_fact_balance_sheet"
    where metric_code in (
        'TRADING_SECURITIES_GROSS',
        'ALLOWANCE_TRADING_SECURITIES',
        'TRADING_SECURITIES_NET'
    )
)

select
    symbol,
    time_report_type,
    'balance_sheet' as financial_report_type,
    year,
    period,

    max(case when metric_code = 'TRADING_SECURITIES_GROSS' then metric_value end) as trading_securities_gross,
    max(case when metric_code = 'ALLOWANCE_TRADING_SECURITIES' then metric_value end) as allowance_trading_securities,
    max(case when metric_code = 'TRADING_SECURITIES_NET' then metric_value end) as trading_securities_net,

    max(update_time) as update_time
from fact
group by symbol, time_report_type, year, period