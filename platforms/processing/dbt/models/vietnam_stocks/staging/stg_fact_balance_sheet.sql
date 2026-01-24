with q as (
    select * from {{ ref('stg_fact_balance_sheet_quarterly') }}
),
a as (
    select * from {{ ref('stg_fact_balance_sheet_annually') }}
)

select * from q
union all
select * from a
