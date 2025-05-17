
with raw_dataset as (
    select *
    from {{ source('NETFLIX_DATA_TEST', 'NETFLIX_DATA') }}
)

select
    "show_id" as show_id,
    "director" as director,
    "cast" as film_cast,
from raw_dataset
