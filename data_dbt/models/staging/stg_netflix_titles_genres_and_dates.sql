

with raw_dataset as (
    select *
    from {{ source('NETFLIX_DATA_SOURCE', 'NETFLIX_DATA') }}
)

select
    "show_id" as show_id,
    "listed_in" as  listed_in,
    "date_added"  as date_added,
    "description" as film_description
from raw_dataset
