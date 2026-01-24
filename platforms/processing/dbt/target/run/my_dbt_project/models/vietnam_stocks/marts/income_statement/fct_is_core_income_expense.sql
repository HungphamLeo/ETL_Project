
  create view "etl_project"."dw"."fct_is_core_income_expense__dbt_tmp"
    
    
  as (
    with src as (
    select *
    from "etl_project"."dw"."stg_fact_income_statement"
    where metric_code in (
        'NET_INTEREST_INCOME',
        'INTEREST_AND_SIMILAR_INCOME',
        'INTEREST_AND_SIMILAR_EXPENSES',
        'NET_FEE_COMMISSION_INCOME',
        'FEE_COMMISSION_INCOME',
        'NET_FEE_AND_COMMISSION_INCOME',
        'FEE_COMMISSION_EXPENSES'
    )
)

select
    symbol,
    time_report_type,
    'income_statement' as financial_report_type,
    year,
    period,

    max(case when metric_code = 'NET_INTEREST_INCOME' then metric_value end) as net_interest_income,
    max(case when metric_code = 'INTEREST_AND_SIMILAR_INCOME' then metric_value end) as interest_and_similar_income,
    max(case when metric_code = 'INTEREST_AND_SIMILAR_EXPENSES' then metric_value end) as interest_and_similar_expenses,
    max(case when metric_code = 'NET_FEE_COMMISSION_INCOME' then metric_value end) as net_fee_commission_income,
    max(case when metric_code = 'FEE_COMMISSION_INCOME' then metric_value end) as fee_commission_income,
    max(case when metric_code = 'NET_FEE_AND_COMMISSION_INCOME' then metric_value end) as net_fee_and_commission_income,
    max(case when metric_code = 'FEE_COMMISSION_EXPENSES' then metric_value end) as fee_commission_expenses,

    max(update_time) as update_time
from src
group by symbol, time_report_type, year, period
  );