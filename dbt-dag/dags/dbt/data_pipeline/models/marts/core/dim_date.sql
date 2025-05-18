{{ config(materialized = 'table') }}


with calendar as (
    {{ dbt_utils.date_spine(
        datepart = "day",
        start_date = "'2000-01-01'",
        end_date   = "'2030-12-31'"
    ) }}
)

select
    date_day                       as date_day,                  
    to_char(date_day, 'YYYYMMDD')  as date_day_key,
    date_trunc('month', date_day)  as month_start,
    to_char(date_day, 'YYYY-MM-01')::date as year_month_key,
    extract(year  from date_day)   as calendar_year
from calendar
