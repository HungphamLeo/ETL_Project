with fact as (
    select *
    from {{ ref('stg_fact_balance_sheet') }}
    where metric_code in (
        'PLACEMENTS_WITH_CREDIT_INSTITUTIONS',
        'LOANS_TO_CREDIT_INSTITUTIONS',
        'ECL_ALLOWANCE_LOANS_TO_CREDIT_INSTITUTIONS',
        'INTERBANK_PLACEMENTS_AND_LOANS'
    )
)

select
    symbol,
    time_report_type,
    'balance_sheet' as financial_report_type,
    year,
    period,

    max(case when metric_code = 'PLACEMENTS_WITH_CREDIT_INSTITUTIONS' then metric_value end) as placements_with_credit_institutions,
    max(case when metric_code = 'LOANS_TO_CREDIT_INSTITUTIONS' then metric_value end) as loans_to_credit_institutions,
    max(case when metric_code = 'ECL_ALLOWANCE_LOANS_TO_CREDIT_INSTITUTIONS' then metric_value end) as ecl_allowance_loans_to_credit_institutions,
    max(case when metric_code = 'INTERBANK_PLACEMENTS_AND_LOANS' then metric_value end) as interbank_placements_and_loans,

    max(update_time) as update_time
from fact
group by symbol, time_report_type, year, period
