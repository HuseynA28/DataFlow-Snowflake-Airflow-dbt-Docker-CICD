 
with raw_dataset as (
    select *
    from {{ source('NETFLIX_DATA_TEST', 'NETFLIX_DATA') }}
)

select
    "show_id" as show_id,
    "listed_in" as  listed_in,
    "date_added"  as date_added,
    "description" as film_description
from raw_dataset




-- models/staging/stg_netflix_titles.sql
with src as (
    select  *
    from    {{ source('raw_netflix', 'titles') }}
)

select
    show_id ,
    initcap(trim(type))               as title_type,     -- “Movie” / “TV Show”
    trim(title)                       as title_name,
    to_date(date_added,'MMMM d, yyyy') as date_added,
    trim(listed_in)                   as raw_genres,
    split(raw_genres, ',')            as genres,         -- array for later explode
    split(trim(cast), ',')            as cast_array,
    split(trim(country), ',')         as country_array,
    trim(rating)                      as mpaa_rating,
    trim(duration)                    as run_time,
    cast(release_year as int)         as release_year,
    current_timestamp                 as _loaded_at
from src
