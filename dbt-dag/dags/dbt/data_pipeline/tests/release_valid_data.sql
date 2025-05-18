{{ config(
    severity = 'warn'
) }}

select 
    release_year
from {{ ref('stg_netflix_titles_core') }}
where 
    date(release_year) > current_date()
    or date(release_year) < date('2000-01-01')
