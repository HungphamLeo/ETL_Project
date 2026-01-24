with fact as (
    select *
    from {{ ref('stg_fact_balance_sheet') }}
    where metric_code in (
        'OWNERS_EQUITY_BANK',
        'BANK_FUNDS',
        'FX_TRANSLATION_DIFFERENCES',
        'ASSET_REVALUATION_DIFFERENCES',
        'RETAINED_EARNINGS_ACCUMULATED_LOSSES',
        'OTHER_FUNDS',
        'NON_CONTROLLING_INTERESTS',
        'TOTAL_EQUITY'
    )
)

select
    symbol,
    time_report_type,
    'balance_sheet' as financial_report_type,
    year,
    period,

    max(case when metric_code = 'OWNERS_EQUITY_BANK' then metric_value end) as owners_equity_bank,
    max(case when metric_code = 'BANK_FUNDS' then metric_value end) as bank_funds,
    max(case when metric_code = 'FX_TRANSLATION_DIFFERENCES' then metric_value end) as fx_translation_differences,
    max(case when metric_code = 'ASSET_REVALUATION_DIFFERENCES' then metric_value end) as asset_revaluation_differences,
    max(case when metric_code = 'RETAINED_EARNINGS_ACCUMULATED_LOSSES' then metric_value end) as retained_earnings_accumulated_losses,
    max(case when metric_code = 'OTHER_FUNDS' then metric_value end) as other_funds,
    max(case when metric_code = 'NON_CONTROLLING_INTERESTS' then metric_value end) as non_controlling_interests,
    max(case when metric_code = 'TOTAL_EQUITY' then metric_value end) as total_equity,

    max(update_time) as update_time
from fact
group by symbol, time_report_type, year, period
