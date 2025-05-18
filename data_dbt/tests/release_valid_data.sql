select 
release_year
from {{ref('stg_netflix_titles_core')}}
where 
    date(release_year) > CURRENT_DATE()
    and date(release_year) <data('2000-01-01')