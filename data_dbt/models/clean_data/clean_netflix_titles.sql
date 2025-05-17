with src as (
    select *
    FROM {{ ref('stg_netflix_titles_core') }}
)

select
    show_id,
    initcap(trim(type))         as type,      
    trim(title)                 as title,
    trim(country)                as country,
    trim(rating)                as rating,
    trim(duration)              as duration,
    cast(release_year as int)   as release_year
from src
