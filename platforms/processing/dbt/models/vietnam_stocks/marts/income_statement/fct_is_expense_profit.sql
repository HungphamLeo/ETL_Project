with src as (
    select *
    from {{ ref('stg_fact_income_statement') }}
    where metric_code in (
        'OPERATING_EXPENSES',
        'OPERATING_PROFIT_BEFORE_CREDIT_PROVISION',
        'CREDIT_RISK_PROVISION_EXPENSES',
        'PROFIT_BEFORE_TAX',
        'CORPORATE_INCOME_TAX_EXPENSE',
        'CURRENT_INCOME_TAX_EXPENSE',
        'DEFERRED_INCOME_TAX_EXPENSE',
        'PROFIT_AFTER_TAX',
        'MINORITY_INTERESTS_AND_PREFERRED_DIVIDENDS',
        'NET_PROFIT_ATTRIBUTABLE_TO_PARENT'
    )
)

select
    symbol,
    time_report_type,
    'income_statement' as financial_report_type,
    year,
    period,

    max(case when metric_code = 'OPERATING_EXPENSES' then metric_value end) as operating_expenses,
    max(case when metric_code = 'OPERATING_PROFIT_BEFORE_CREDIT_PROVISION' then metric_value end) as operating_profit_before_credit_provision,
    max(case when metric_code = 'CREDIT_RISK_PROVISION_EXPENSES' then metric_value end) as credit_risk_provision_expenses,
    max(case when metric_code = 'PROFIT_BEFORE_TAX' then metric_value end) as profit_before_tax,
    max(case when metric_code = 'CORPORATE_INCOME_TAX_EXPENSE' then metric_value end) as corporate_income_tax_expense,
    max(case when metric_code = 'CURRENT_INCOME_TAX_EXPENSE' then metric_value end) as current_income_tax_expense,
    max(case when metric_code = 'DEFERRED_INCOME_TAX_EXPENSE' then metric_value end) as deferred_income_tax_expense,
    max(case when metric_code = 'PROFIT_AFTER_TAX' then metric_value end) as profit_after_tax,
    max(case when metric_code = 'MINORITY_INTERESTS_AND_PREFERRED_DIVIDENDS' then metric_value end) as minority_interests_and_preferred_dividends,
    max(case when metric_code = 'NET_PROFIT_ATTRIBUTABLE_TO_PARENT' then metric_value end) as net_profit_attributable_to_parent,

    max(update_time) as update_time
from src
group by symbol, time_report_type, year, period
