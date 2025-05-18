with src as (
  select *
    from {{ ref('stg_netflix_titles_people') }}
)

select
  show_id,
  trim(director)     as director,
  film_cast             as film_cast
from src


