
  create view "etl_project"."dw_stg"."stg_fact_income_statement__dbt_tmp"
    
    
  as (
    with q as (
    select * from "etl_project"."dw_stg"."stg_fact_income_statement_quarterly"
),
a as (
    select * from "etl_project"."dw_stg"."stg_fact_income_statement_annually"
)

select * from q
union all
select * from a
  );