with base as (
    select distinct
        metric_group,
        metric_code,
        metric_name_vi_raw,
        metric_name_en
    from {{ ref('stg_fact_balance_sheet') }}
    where metric_code is not null
),

parsed as (
    select
        metric_group,
        metric_code,
        metric_name_vi_raw,
        metric_name_en,

        case
            when metric_name_vi_raw ~ '^[IVXLCDM]+\\.' then 'ROMAN'
            when metric_name_vi_raw ~ '^\\d+\\.' then 'ARABIC'
            when metric_name_vi_raw ~ '^\\-\\s+' then 'DASH'
            when upper(metric_name_vi_raw) ~ '^TỔNG' then 'TOTAL'
            else 'LEAF'
        end as node_type,

        case
            when metric_name_vi_raw ~ '^[IVXLCDM]+\\.' then substring(metric_name_vi_raw from '^([IVXLCDM]+)\\.')
            else null
        end as roman_no,

        case
            when metric_name_vi_raw ~ '^\\d+\\.' then cast(substring(metric_name_vi_raw from '^(\\d+)\\.') as int)
            else null
        end as arabic_no
    from base
),

ordered as (
    select
        *,
        case
            when metric_group = 'ASSETS' then 10
            when metric_group = 'LIABILITIES' then 20
            when metric_group = 'EQUITY' then 30
            else 90
        end as grp_order,

        coalesce(
            case upper(roman_no)
                when 'I' then 1 when 'II' then 2 when 'III' then 3 when 'IV' then 4 when 'V' then 5
                when 'VI' then 6 when 'VII' then 7 when 'VIII' then 8 when 'IX' then 9 when 'X' then 10
                when 'XI' then 11 when 'XII' then 12 when 'XIII' then 13 when 'XIV' then 14 when 'XV' then 15
                else null
            end, 999
        ) as roman_order,

        coalesce(arabic_no, 999) as arabic_order,

        case
            when node_type = 'DASH' then 1
            when node_type = 'LEAF' then 2
            when node_type = 'TOTAL' then 3
            else 0
        end as tail_order
    from parsed
),

ranked as (
    select
        *,
        row_number() over (
            order by grp_order, roman_order, arabic_order, tail_order, metric_name_vi_raw, metric_code
        ) as display_order
    from ordered
),

ctx as (
    select
        *,
        max(case when node_type = 'ROMAN' then metric_code end)
            over (partition by metric_group order by display_order rows between unbounded preceding and current row)
            as current_roman_metric_code,

        max(case when node_type = 'ARABIC' then metric_code end)
            over (partition by metric_group order by display_order rows between unbounded preceding and current row)
            as current_arabic_metric_code
    from ranked
)

select
    {{ dbt_utils.generate_surrogate_key(['metric_code']) }} as metric_key,
    metric_group,
    metric_code,
    metric_name_vi_raw as metric_name_vi,
    metric_name_en,
    node_type,
    display_order,
    case
        when node_type = 'ROMAN' then 1
        when node_type = 'ARABIC' then 2
        when node_type in ('DASH','LEAF') then 3
        when node_type = 'TOTAL' then 9
        else 3
    end as level,
    case
        when node_type = 'ARABIC' then current_roman_metric_code
        when node_type in ('DASH','LEAF') then coalesce(current_arabic_metric_code, current_roman_metric_code)
        else null
    end as parent_metric_code
from ctx
