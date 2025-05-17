
with src as (
    select  *
    from    {{ source('DATA_SOURCE', 'STG_NETFLIX_TITLES_CORE') }}
)

select
    show_id,
    initcap(trim(type))                      as title_type,      
    trim(title)                       as title_name,
    split(trim(country), ',')         as country,
    trim(rating)                      as mpaa_rating,
    trim(duration)                    ,
    cast(release_year as int)         as release_year,
from src
