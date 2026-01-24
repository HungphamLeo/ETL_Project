with q as (
    select * from {{ ref('stg_fact_income_statement_quarterly') }}
),
a as (
    select * from {{ ref('stg_fact_income_statement_annually') }}
)

select * from q
union all
select * from a
