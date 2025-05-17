{{ config(materialized='table') }}

with genres as (
    select
        trim(genre) as genre
    from {{ ref('stg_netflix_titles_genres_and_dates') }},
         lateral flatten(input => split(listed_in, ',')) g(genre)
    group by 1
)

select
    {{ dbt_utils.generate_surrogate_key(['genre']) }} as genre_key,
    genre
from genres
