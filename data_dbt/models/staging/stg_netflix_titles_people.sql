{{ config(materialized='table') }}      -- or 'table' if you prefer

with raw_dataset as (
    select *
    from {{ source('NETFLIX_DATA_TEST', 'NETFLIX_DATA') }}
)

select
    "show_id" as show_id,
    "director" as director,
    "cast" as cast,
from raw_dataset
