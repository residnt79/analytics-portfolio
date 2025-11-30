
{{ config(
    materialized = 'table'
    )
}}

with date_spine as (
    {{
        dbt.date_spine(
            datepart = "day",
            start_date = "cast('2025-01-01' as date)",
            end_date = "cast('2030-12-31' as date)"
        )
    }}
)

SELECT to_char(date_day, 'YYYYMMDD')::integer as date_key
    , date_day:: date as date
    , date_part('year', date_day) as year
    , date_part('quarter', date_day) as quarter
    , date_part('month', date_day) as month
    , date_part('day', date_day) as day_of_month
    , date_part('dow', date_day) as day_of_week
    , date_part('doy', date_day) as day_of_year
    , date_part('week', date_day) as week_of_year
    , to_char(date_day, 'Day') as day_name
    , to_char(date_day, 'Month') as month_name
    , case when date_part('dow', date_day) in (0,6) then TRUE else FALSE end as is_weekend
from date_spine