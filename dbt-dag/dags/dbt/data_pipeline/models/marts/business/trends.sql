{{ config(materialized = 'table') }}

with monthly_genre as (

  select
    d.year_month_key,
    g.genre,
    count(distinct f.title_key) as titles_added    -- use the real key column
  from {{ ref('fct_netflix_title') }} f
  join {{ ref('dim_date') }}          d on d.date_day_key   = f.date_day_key
  left join {{ ref('dim_genre') }}     g on g.genre_key       = f.genre_key
  group by d.year_month_key, g.genre

)

select
  year_month_key,
  coalesce(genre, 'Unknown')  as genre,
  titles_added,
  round(
    100.0 * titles_added
    / sum(titles_added) over (partition by year_month_key)
  , 2)                         as pct_of_month
from monthly_genre
order by year_month_key, pct_of_month desc
