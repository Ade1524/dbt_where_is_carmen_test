with africa_sighting as (
    select * 
    from {{ ref('stg_carmen_sighting_africa') }}
)
, america_sighting as (
    select * 
    from {{ ref('stg_carmen_sighting_america') }}
)
, asia_sighting as (
    select * 
    from {{ ref('stg_carmen_sighting_asia') }}
)
, atlantic_sighting as (
    select * 
    from {{ ref('stg_carmen_sighting_atlantic') }}
)
, australia_sighting as (
    select * 
    from {{ ref('stg_carmen_sighting_australia') }}
)
, europe_sighting as (
    select * 
    from {{ ref('stg_carmen_sighting_europe') }}
)
,indian_sighting as (
    select * 
    from {{ ref('stg_carmen_sighting_indian') }}
)

, pacific_sighting as (
    select * 
    from {{ ref('stg_carmen_sighting_pacific') }}
)
, all_region_sightings as (
    select * 
    from africa_sighting
    union
    select * 
    from america_sighting
    union
    select * 
    from asia_sighting
    union
    select * 
    from atlantic_sighting
    union
    select * 
    from australia_sighting
    union
    select * 
    from europe_sighting
    union
    select * 
    from indian_sighting
    union
    select * 
    from pacific_sighting
    order by city

)


select * from all_region_sightings
