
  
    

  create  table "etl_project"."dw_dw"."fct_is_financial_trading__dbt_tmp"
  
  
    as
  
  (
    with src as (
    select *
    from "etl_project"."dw_stg"."stg_fact_income_statement"
    where metric_code in (
        'NET_FX_TRADING_INCOME',
        'NET_TRADING_SECURITIES_INCOME',
        'NET_INVESTMENT_SECURITIES_INCOME'
    )
)

select
    symbol,
    time_report_type,
    'income_statement' as financial_report_type,
    year,
    period,

    max(case when metric_code = 'NET_FX_TRADING_INCOME' then metric_value end) as net_fx_trading_income,
    max(case when metric_code = 'NET_TRADING_SECURITIES_INCOME' then metric_value end) as net_trading_securities_income,
    max(case when metric_code = 'NET_INVESTMENT_SECURITIES_INCOME' then metric_value end) as net_investment_securities_income,

    max(update_time) as update_time
from src
group by symbol, time_report_type, year, period
  );
  