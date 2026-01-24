
  
    

  create  table "etl_project"."dw_dw"."fct_is_other_operating__dbt_tmp"
  
  
    as
  
  (
    with src as (
    select *
    from "etl_project"."dw_stg"."stg_fact_income_statement"
    where metric_code in (
        'NET_OTHER_OPERATIONAL_INCOME',
        'OTHER_OPERATING_INCOME',
        'OTHER_OPERATING_EXPENSES',
        'INCOME_FROM_EQUITY_INVESTMENTS'
    )
)

select
    symbol,
    time_report_type,
    'income_statement' as financial_report_type,
    year,
    period,

    max(case when metric_code = 'NET_OTHER_OPERATIONAL_INCOME' then metric_value end) as net_other_operational_income,
    max(case when metric_code = 'OTHER_OPERATING_INCOME' then metric_value end) as other_operating_income,
    max(case when metric_code = 'OTHER_OPERATING_EXPENSES' then metric_value end) as other_operating_expenses,
    max(case when metric_code = 'INCOME_FROM_EQUITY_INVESTMENTS' then metric_value end) as income_from_equity_investments,

    max(update_time) as update_time
from src
group by symbol, time_report_type, year, period
  );
  