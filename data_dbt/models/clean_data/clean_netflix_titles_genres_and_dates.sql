with src as (
    select *
      from {{ ref('stg_netflix_titles_genres_and_dates') }}
)

select
    show_id,
    to_date(date_added, 'MMMM d, yyyy') as date_added,
    trim(listed_in)              as listed_in,
    film_description             as film_description
from src

