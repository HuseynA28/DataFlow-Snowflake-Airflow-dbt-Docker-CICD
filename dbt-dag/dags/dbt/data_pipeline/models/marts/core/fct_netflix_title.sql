-- models/marts/core/fct_netflix_title.sql
{{ config(materialized='table') }}

with f as (
    select
        show_id,
        title,
        {{ dbt_utils.generate_surrogate_key(['show_id']) }} as title_key,
        d.date_day_key,
        null                     as genre_key,
        c.country_key,
        r.rating_key,
        t.type_key,
        release_year,
        cast(regexp_replace(duration,'[^0-9]','') as int) as raw_duration_num
    from {{ ref('clean_netflix_titles') }} n
    left join {{ ref('dim_date') }}  d
      on d.date_day = to_date(release_year || '-01-01')
    left join {{ ref('dim_country') }} c
      on c.country = split_part(n.country, ',', 1)
    left join {{ ref('dim_rating') }}  r on r.rating = n.rating
    left join {{ ref('dim_type') }}    t on t.type   = n.type
)

select * from f

