with fact as (
    select *
    from "etl_project"."dw_stg"."stg_fact_balance_sheet"
),

dim as (
    select *
    from "etl_project"."dw_dw"."dim_balance_sheet_metric_hier"
),

-- leaf metrics we need in this table
leaf as (
    select
        f.symbol, f.time_report_type, f.year, f.period,
        f.metric_code, f.metric_value, f.update_time
    from fact f
    where f.metric_code in (
        'RECEIVABLES',
        'ACCRUED_INTEREST_AND_FEES_RECEIVABLE',
        'DEFERRED_TAX_ASSETS',
        'OTHER_ASSETS_GROSS',
        'GOODWILL',
        'ALLOWANCE_OTHER_ON_BALANCE_ASSETS',
        'OTHER_ASSETS_NET'
    )
),

-- attach hierarchy metadata (parent/display_order/group)
leaf_with_parent as (
    select
        l.*,
        d.metric_group,
        d.display_order,
        d.parent_metric_code
    from leaf l
    left join dim d
      on d.metric_code = l.metric_code
),

-- get roman ancestor metric_code for each leaf metric_code (nearest ROMAN by display_order)
leaf_with_roman as (
    select
        x.*,
        (
          select d2.metric_code
          from dim d2
          where d2.metric_group = x.metric_group
            and d2.node_type = 'ROMAN'
            and d2.display_order <= x.display_order
          order by d2.display_order desc
          limit 1
        ) as roman_metric_code
    from leaf_with_parent x
),

-- join to fetch parent_value and roman_value from fact
enriched as (
    select
        l.symbol, l.time_report_type, l.year, l.period,
        l.metric_code, l.metric_value, l.update_time,

        fp.metric_value as parent_metric_value,
        fr.metric_value as roman_metric_value
    from leaf_with_roman l
    left join fact fp
      on fp.symbol = l.symbol
     and fp.time_report_type = l.time_report_type
     and fp.year = l.year
     and fp.period is not distinct from l.period
     and fp.metric_code = l.parent_metric_code

    left join fact fr
      on fr.symbol = l.symbol
     and fr.time_report_type = l.time_report_type
     and fr.year = l.year
     and fr.period is not distinct from l.period
     and fr.metric_code = l.roman_metric_code
)

select
    symbol,
    time_report_type,
    'balance_sheet' as financial_report_type,
    year,
    period,

    max(case when metric_code = 'RECEIVABLES' then metric_value end) as receivables_net,
    max(case when metric_code = 'ACCRUED_INTEREST_AND_FEES_RECEIVABLE' then metric_value end) as accrued_interest_and_fees_receivable,
    max(case when metric_code = 'DEFERRED_TAX_ASSETS' then metric_value end) as deferred_tax_assets,
    max(case when metric_code = 'OTHER_ASSETS_GROSS' then metric_value end) as other_assets_gross,
    max(case when metric_code = 'GOODWILL' then metric_value end) as goodwill,
    max(case when metric_code = 'ALLOWANCE_OTHER_ON_BALANCE_ASSETS' then metric_value end) as allowance_other_on_balance_assets,
    max(case when metric_code = 'OTHER_ASSETS_NET' then metric_value end) as other_assets_net,

    max(case when metric_code = 'GOODWILL' then roman_metric_value end) as hierachy_of_goodwill,
    max(case when metric_code = 'ALLOWANCE_OTHER_ON_BALANCE_ASSETS' then roman_metric_value end) as hierachy_of_allowance_other_on_balance_assets,

    max(case when metric_code = 'GOODWILL' then parent_metric_value end) as parent_of_goodwill,
    max(case when metric_code = 'ALLOWANCE_OTHER_ON_BALANCE_ASSETS' then parent_metric_value end) as parent_of_allowance_other_on_balance_assets,

    max(update_time) as update_time
from enriched
group by symbol, time_report_type, year, period