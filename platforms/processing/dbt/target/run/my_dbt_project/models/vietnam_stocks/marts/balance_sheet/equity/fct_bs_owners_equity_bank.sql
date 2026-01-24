
  
    

  create  table "etl_project"."dw_dw"."fct_bs_owners_equity_bank__dbt_tmp"
  
  
    as
  
  (
    with fact as (
    select *
    from "etl_project"."dw_stg"."stg_fact_balance_sheet"
    where metric_code in (
        'PAID_IN_CAPITAL',
        'CAPITAL_FOR_CONSTRUCTION',
        'SHARE_PREMIUM',
        'TREASURY_SHARES',
        'PREFERRED_SHARES',
        'OTHER_CAPITAL'
    )
)

select
    symbol,
    time_report_type,
    'balance_sheet' as financial_report_type,
    year,
    period,

    max(case when metric_code = 'PAID_IN_CAPITAL' then metric_value end) as paid_in_capital,
    max(case when metric_code = 'CAPITAL_FOR_CONSTRUCTION' then metric_value end) as capital_for_construction,
    max(case when metric_code = 'SHARE_PREMIUM' then metric_value end) as share_premium,
    max(case when metric_code = 'TREASURY_SHARES' then metric_value end) as treasury_shares,
    max(case when metric_code = 'PREFERRED_SHARES' then metric_value end) as preferred_shares,
    max(case when metric_code = 'OTHER_CAPITAL' then metric_value end) as other_capital,

    max(update_time) as update_time
from fact
group by symbol, time_report_type, year, period
  );
  