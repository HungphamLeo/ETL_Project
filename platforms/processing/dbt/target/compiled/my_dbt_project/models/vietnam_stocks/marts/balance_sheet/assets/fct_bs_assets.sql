with src as (
    select *
    from "etl_project"."dw_stg"."stg_fact_balance_sheet"
    where metric_code in (
        'CASH_AND_VALUABLES',
        'DEPOSITS_WITH_CENTRAL_BANK',
        'T_BILLS_AND_ELIGIBLE_SHORT_TERM_SECURITIES',
        'INTERBANK_PLACEMENTS_AND_LOANS',
        'TRADING_SECURITIES_NET',
        'DERIVATIVES_AND_OTHER_FINANCIAL_ASSETS',
        'LOANS_TO_CUSTOMERS_NET',
        'INVESTMENT_SECURITIES_NET',
        'LONG_TERM_INVESTMENTS_NET',
        'FIXED_ASSETS_NET',
        'INVESTMENT_PROPERTIES_NET',
        'OTHER_ASSETS_NET',
        'TOTAL_ASSETS'
    )
)

select
    symbol,
    time_report_type,
    'balance_sheet' as financial_report_type,
    year,
    period,

    max(case when metric_code = 'CASH_AND_VALUABLES' then metric_value end) as cash_and_valuables,
    max(case when metric_code = 'DEPOSITS_WITH_CENTRAL_BANK' then metric_value end) as deposits_with_central_bank,
    max(case when metric_code = 'T_BILLS_AND_ELIGIBLE_SHORT_TERM_SECURITIES' then metric_value end) as t_bills_and_eligible_short_term_securities,
    max(case when metric_code = 'INTERBANK_PLACEMENTS_AND_LOANS' then metric_value end) as interbank_placements_and_loans,
    max(case when metric_code = 'TRADING_SECURITIES_NET' then metric_value end) as trading_securities_net,
    max(case when metric_code = 'DERIVATIVES_AND_OTHER_FINANCIAL_ASSETS' then metric_value end) as derivatives_and_other_financial_assets,
    max(case when metric_code = 'LOANS_TO_CUSTOMERS_NET' then metric_value end) as loans_to_customers_net,
    max(case when metric_code = 'INVESTMENT_SECURITIES_NET' then metric_value end) as investment_securities_net,
    max(case when metric_code = 'LONG_TERM_INVESTMENTS_NET' then metric_value end) as long_term_investments_net,
    max(case when metric_code = 'FIXED_ASSETS_NET' then metric_value end) as fixed_assets_net,
    max(case when metric_code = 'INVESTMENT_PROPERTIES_NET' then metric_value end) as investment_properties_net,
    max(case when metric_code = 'OTHER_ASSETS_NET' then metric_value end) as other_assets_net,
    max(case when metric_code = 'TOTAL_ASSETS' then metric_value end) as total_assets,

    max(update_time) as update_time
from src
group by symbol, time_report_type, year, period