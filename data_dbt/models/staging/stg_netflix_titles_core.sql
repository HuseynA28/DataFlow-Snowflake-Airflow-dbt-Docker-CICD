

with raw_dataset as (
    select *
    from {{ source('NETFLIX_DATA_TEST', 'NETFLIX_DATA') }}
)

select
    "show_id" AS show_id,
    "type" AS type,
    "title" AS title,
    "country" AS country,
    "release_year" AS release_year,
    "rating" AS rating,
    "duration" AS duration
from raw_dataset
