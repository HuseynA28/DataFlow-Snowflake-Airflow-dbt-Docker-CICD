{{ config(materialized='table') }}

with base as (
    select distinct rating
    from {{ ref('clean_netflix_titles') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['rating']) }}  as rating_key,
    rating,
    case   
        when rating in ('G','PG','TV-G','TV-Y','TV-Y7')              then 'Kids'
        when rating in ('PG-13','TV-PG')                             then 'Teens'
        when rating in ('R','NC-17','TV-14','TV-MA')                 then 'Adults'
        else 'Unknown'
    end                                             as rating_group
from base
