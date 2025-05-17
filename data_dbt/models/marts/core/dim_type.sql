{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['type']) }} as type_key,
    type
from ( select distinct type from {{ ref('clean_netflix_titles') }} )
