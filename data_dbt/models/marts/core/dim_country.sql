{{ config(materialized='table') }}

with countries as (
  select
    trim(f.value::string) as country
  from {{ ref('clean_netflix_titles') }} as t

    , lateral flatten(input => split(t.country, ',')) as f
  group by 1
)

select
  {{ dbt_utils.generate_surrogate_key(['country']) }} as country_key,
  country
from countries
