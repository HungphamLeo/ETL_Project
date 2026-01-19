with fact as (
    select *
    from {{ ref('stg_fact_balance_sheet') }}
),

dim as (
    select *
    from {{ ref('dim_balance_sheet_metric_hier') }}
),

leaf as (
    select
        f.symbol, f.time_report_type, f.year, f.period, f.metric_code, f.metric_value, f.update_time
    from fact f
    where f.metric_code in (
        'DUE_TO_GOVERNMENT_AND_CENTRAL_BANK',
        'DUE_TO_CREDIT_INSTITUTIONS',
        'DEPOSITS_FROM_CREDIT_INSTITUTIONS',
        'BORROWINGS_FROM_CREDIT_INSTITUTIONS',
        'CUSTOMER_DEPOSITS',
        'DERIVATIVES_AND_OTHER_FINANCIAL_LIABILITIES',
        'TRUST_AND_INVESTMENT_FUNDS_BANK_BEARING_RISK',
        'DEBT_SECURITIES_ISSUED',
        'OTHER_LIABILITIES',
        'ACCRUED_INTEREST_AND_FEES_PAYABLE',
        'DEFERRED_TAX_LIABILITIES',
        'PAYABLES_AND_OTHER_LIABILITIES',
        'OTHER_PROVISIONS'
    )
),

leaf_ctx as (
    select
        l.*,
        d.parent_metric_code,
        d.metric_group,
        d.display_order,
        (
          select d2.metric_code
          from dim d2
          where d2.metric_group = d.metric_group
            and d2.node_type = 'ROMAN'
            and d2.display_order <= d.display_order
          order by d2.display_order desc
          limit 1
        ) as roman_metric_code
    from leaf l
    left join dim d on d.metric_code = l.metric_code
),

enriched as (
    select
        c.*,
        fp.metric_value as parent_metric_value,
        fr.metric_value as roman_metric_value
    from leaf_ctx c
    left join fact fp
      on fp.symbol = c.symbol
     and fp.time_report_type = c.time_report_type
     and fp.year = c.year
     and ( (fp.period is null and c.period is null) or fp.period = c.period )
     and fp.metric_code = c.parent_metric_code

    left join fact fr
      on fr.symbol = c.symbol
     and fr.time_report_type = c.time_report_type
     and fr.year = c.year
     and ( (fr.period is null and c.period is null) or fr.period = c.period )
     and fr.metric_code = c.roman_metric_code
)

select
    symbol,
    time_report_type,
    'balance_sheet' as financial_report_type,
    year,
    period,

    max(case when metric_code = 'DUE_TO_GOVERNMENT_AND_CENTRAL_BANK' then metric_value end) as due_to_government_and_central_bank,
    max(case when metric_code = 'DUE_TO_CREDIT_INSTITUTIONS' then metric_value end) as due_to_credit_institutions,

    max(case when metric_code = 'DEPOSITS_FROM_CREDIT_INSTITUTIONS' then metric_value end) as deposits_from_credit_institutions,
    max(case when metric_code = 'BORROWINGS_FROM_CREDIT_INSTITUTIONS' then metric_value end) as borrowings_from_credit_institutions,

    max(case when metric_code = 'DEPOSITS_FROM_CREDIT_INSTITUTIONS' then roman_metric_value end) as hierachy_of_deposits_from_credit_institutions,
    max(case when metric_code = 'BORROWINGS_FROM_CREDIT_INSTITUTIONS' then roman_metric_value end) as hierachy_of_borrowings_from_credit_institutions,

    max(case when metric_code = 'DEPOSITS_FROM_CREDIT_INSTITUTIONS' then parent_metric_value end) as parent_of_deposits_from_credit_institutions,
    max(case when metric_code = 'BORROWINGS_FROM_CREDIT_INSTITUTIONS' then parent_metric_value end) as parent_of_borrowings_from_credit_institutions,

    max(case when metric_code = 'CUSTOMER_DEPOSITS' then metric_value end) as customer_deposits,
    max(case when metric_code = 'DERIVATIVES_AND_OTHER_FINANCIAL_LIABILITIES' then metric_value end) as derivatives_and_other_financial_liabilities,
    max(case when metric_code = 'TRUST_AND_INVESTMENT_FUNDS_BANK_BEARING_RISK' then metric_value end) as trust_and_investment_funds_bank_bearing_risk,
    max(case when metric_code = 'DEBT_SECURITIES_ISSUED' then metric_value end) as debt_securities_issued,
    max(case when metric_code = 'OTHER_LIABILITIES' then metric_value end) as other_liabilities,

    max(case when metric_code = 'ACCRUED_INTEREST_AND_FEES_PAYABLE' then metric_value end) as accrued_interest_and_fees_payable,
    max(case when metric_code = 'DEFERRED_TAX_LIABILITIES' then metric_value end) as deferred_tax_liabilities,
    max(case when metric_code = 'PAYABLES_AND_OTHER_LIABILITIES' then metric_value end) as payables_and_other_liabilities,
    max(case when metric_code = 'OTHER_PROVISIONS' then metric_value end) as other_provisions,

    max(case when metric_code = 'ACCRUED_INTEREST_AND_FEES_PAYABLE' then roman_metric_value end) as hierachy_of_accrued_interest_and_fees_payable,
    max(case when metric_code = 'DEFERRED_TAX_LIABILITIES' then roman_metric_value end) as hierachy_of_deferred_tax_liabilities,
    max(case when metric_code = 'PAYABLES_AND_OTHER_LIABILITIES' then roman_metric_value end) as hierachy_of_payables_and_other_liabilities,
    max(case when metric_code = 'OTHER_PROVISIONS' then roman_metric_value end) as hierachy_of_other_provisions,

    max(case when metric_code = 'ACCRUED_INTEREST_AND_FEES_PAYABLE' then parent_metric_value end) as parent_of_accrued_interest_and_fees_payable,
    max(case when metric_code = 'DEFERRED_TAX_LIABILITIES' then parent_metric_value end) as parent_of_deferred_tax_liabilities,
    max(case when metric_code = 'PAYABLES_AND_OTHER_LIABILITIES' then parent_metric_value end) as parent_of_payables_and_other_liabilities,
    max(case when metric_code = 'OTHER_PROVISIONS' then parent_metric_value end) as parent_of_other_provisions,

    max(update_time) as update_time
from enriched
group by symbol, time_report_type, year, period
