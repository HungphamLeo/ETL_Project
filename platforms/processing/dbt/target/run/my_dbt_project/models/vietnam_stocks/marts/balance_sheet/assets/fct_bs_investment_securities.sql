
  
    

  create  table "etl_project"."dw_dw"."fct_bs_investment_securities__dbt_tmp"
  
  
    as
  
  (
    with fact as (
    select *
    from "etl_project"."dw_stg"."stg_fact_balance_sheet"
    where metric_code in (
        'AFS_SECURITIES',
        'HTM_SECURITIES',
        'ALLOWANCE_INVESTMENT_SECURITIES',
        'INVESTMENT_SECURITIES_NET'
    )
)

select
    symbol,
    time_report_type,
    'balance_sheet' as financial_report_type,
    year,
    period,

    max(case when metric_code = 'AFS_SECURITIES' then metric_value end) as afs_securities,
    max(case when metric_code = 'HTM_SECURITIES' then metric_value end) as htm_securities,
    max(case when metric_code = 'ALLOWANCE_INVESTMENT_SECURITIES' then metric_value end) as allowance_investment_securities,
    max(case when metric_code = 'INVESTMENT_SECURITIES_NET' then metric_value end) as investment_securities_net,

    max(update_time) as update_time
from fact
group by symbol, time_report_type, year, period
  );
  