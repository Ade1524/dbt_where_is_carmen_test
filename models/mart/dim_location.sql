{{ config(materialized='table') }}

with location_sightings as (
    select 
        latitude,
        longitude,
        city,
        country,
        region
    from {{ ref('int_sightings_all_region') }}
    group by 
        city,
        country,
        region,
        latitude,
        longitude
)
, sur_location_key as (
    select 
        {{ dbt_utils.generate_surrogate_key(['latitude', 'longitude']) }} as dim_location_key,
        *
    from location_sightings
)


select * from sur_location_key