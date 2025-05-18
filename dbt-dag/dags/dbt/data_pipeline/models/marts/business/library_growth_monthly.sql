{{ config(materialized='table', transient=false, temporary=false) }}


with monthly_counts as (

    select
        d.month_start,
        count(distinct f.title_key) as titles_added       -- use the correct column name
    from {{ ref('fct_netflix_title') }}  f
    join {{ ref('dim_date') }}          d
      on d.date_day_key = f.date_day_key
    group by d.month_start
)

select
    month_start,
    titles_added,
    round(
        100.0 * (titles_added
                 - lag(titles_added) over (order by month_start))
        / nullif(lag(titles_added) over (order by month_start), 0)
    , 2)                                             as pct_change_mom
from monthly_counts
order by month_start

