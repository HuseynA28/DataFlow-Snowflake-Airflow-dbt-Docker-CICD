{{ config(materialized='table') }}      -- or 'table' if you prefer

with raw_dataset as (
    select *
    from {{ source('NETFLIX_DATA_TEST', 'NETFLIX_DATA') }}
)

select
    "show_id",
    "director",
    "cast"
from raw_dataset
