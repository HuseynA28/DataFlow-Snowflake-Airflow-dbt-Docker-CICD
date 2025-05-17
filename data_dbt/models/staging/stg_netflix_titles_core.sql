{{ config(materialized='table') }}      -- or 'table' if you prefer

with raw_dataset as (
    select *
    from {{ source('NETFLIX_DATA_TEST', 'NETFLIX_DATA') }}
)

select
    "show_id",
    "type",
    "title",
    "country",
    "release_year",
    "rating",
    "duration"
from raw_dataset
