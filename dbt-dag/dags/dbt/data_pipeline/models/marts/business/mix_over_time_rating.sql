{{ config(materialized='table', transient=false, temporary=false) }}


with annual_ratings as (

    select
        d.calendar_year                as year,
        r.rating_group                 as rating_group,
        count(distinct f.title_key)    as titles_added    -- use the real key column
    from {{ ref('fct_netflix_title') }} f
    join {{ ref('dim_date') }}         d on d.date_day_key = f.date_day_key
    left join {{ ref('dim_rating') }}  r on r.rating_key   = f.rating_key
    group by d.calendar_year, r.rating_group

)

select
    year,
    coalesce(rating_group, 'Unknown')  as rating_group,
    titles_added,
    round(
        100.0 * titles_added
        / sum(titles_added) over (partition by year)
    , 2)                               as pct_of_year
from annual_ratings
order by year, rating_group
