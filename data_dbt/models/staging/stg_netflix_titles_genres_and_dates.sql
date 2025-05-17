{{ config(materialized='table') }}   

with raw_dataset as (
    select *
    from {{ source('NETFLIX_DATA_TEST', 'NETFLIX_DATA') }}
)

select
    "show_id",
    "listed_in",
    "date_added",
    "description"
from raw_dataset
