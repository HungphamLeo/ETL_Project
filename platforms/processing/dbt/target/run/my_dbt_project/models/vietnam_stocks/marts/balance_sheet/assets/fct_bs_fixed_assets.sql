
  
    

  create  table "etl_project"."dw_dw"."fct_bs_fixed_assets__dbt_tmp"
  
  
    as
  
  (
    with src as (
    select *
    from "etl_project"."dw_stg"."stg_fact_balance_sheet"
    where metric_code in (
        'TANGIBLE_FIXED_ASSETS_NET',
        'FIXED_ASSETS_COST',
        'ACCUMULATED_DEPRECIATION',
        'FINANCE_LEASE_FIXED_ASSETS_NET',
        'INTANGIBLE_ASSETS_NET',
        'CIP_CONSTRUCTION_IN_PROGRESS'
    )
),

hier as (
    select *,
        case
            when coalesce(metric_name_vi_raw,'') ~ '^[IVXLCDM]+\.' then 'ROMAN'
            when metric_name_vi_raw ~ '^\\d+\\.' then 'ARABIC'
            when metric_name_vi_raw ~ '^- ' then 'DASH'
        end as hierarchy_type
    from src
)

select
    symbol,
    time_report_type,
    year,
    period,

    max(case when metric_code = 'TANGIBLE_FIXED_ASSETS_NET' then metric_value end) as tangible_fixed_assets_net,
    max(case when metric_code = 'FIXED_ASSETS_COST' then metric_value end) as fixed_assets_cost,
    max(case when metric_code = 'ACCUMULATED_DEPRECIATION' then metric_value end) as accumulated_depreciation,

    max(case when metric_code = 'FIXED_ASSETS_COST' and hierarchy_type='ROMAN' then metric_value end)
        as hierachy_of_fixed_assets_cost,

    max(case when metric_code = 'ACCUMULATED_DEPRECIATION' and hierarchy_type='ROMAN' then metric_value end)
        as hierachy_of_accumulated_depreciation,

    max(case when metric_code = 'FINANCE_LEASE_FIXED_ASSETS_NET' then metric_value end) as finance_lease_fixed_assets_net,
    max(case when metric_code = 'INTANGIBLE_ASSETS_NET' then metric_value end) as intangible_assets_net,
    max(case when metric_code = 'CIP_CONSTRUCTION_IN_PROGRESS' then metric_value end) as cip_construction_in_progress,

    max(update_time) as update_time
from hier
group by symbol, time_report_type, year, period
  );
  