
  create view "etl_project"."dw_stg"."stg_fact_balance_sheet__dbt_tmp"
    
    
  as (
    with q as (
    select * from "etl_project"."dw_stg"."stg_fact_balance_sheet_quarterly"
),
a as (
    select * from "etl_project"."dw_stg"."stg_fact_balance_sheet_annually"
)

select * from q
union all
select * from a
  );