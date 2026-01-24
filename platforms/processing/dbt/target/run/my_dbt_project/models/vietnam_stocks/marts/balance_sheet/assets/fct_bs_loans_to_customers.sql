
  
    

  create  table "etl_project"."dw_dw"."fct_bs_loans_to_customers__dbt_tmp"
  
  
    as
  
  (
    with fact as (
    select *
    from "etl_project"."dw_stg"."stg_fact_balance_sheet"
    where metric_code in (
        'LOANS_TO_CUSTOMERS_GROSS',
        'ECL_ALLOWANCE_LOANS_TO_CUSTOMERS',
        'LOANS_TO_CUSTOMERS_NET'
    )
)

select
    symbol,
    time_report_type,
    'balance_sheet' as financial_report_type,
    year,
    period,

    max(case when metric_code = 'LOANS_TO_CUSTOMERS_GROSS' then metric_value end) as loans_to_customers_gross,
    max(case when metric_code = 'ECL_ALLOWANCE_LOANS_TO_CUSTOMERS' then metric_value end) as ecl_allowance_loans_to_customers,
    max(case when metric_code = 'LOANS_TO_CUSTOMERS_NET' then metric_value end) as loans_to_customers_net,

    max(update_time) as update_time
from fact
group by symbol, time_report_type, year, period
  );
  