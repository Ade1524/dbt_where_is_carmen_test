{{ config(materialized='table') }}

with agent_sightings as (
    select 
        agent,
        city_agent,
        country,
        region,
        count(agent) as num_agent_sightings
    from {{ ref('int_sightings_all_region') }}
    group by 
        agent, 
        country,
        city_agent,
        region
)
, sur_agent_key as (
    select 
        {{ dbt_utils.generate_surrogate_key(['agent','country','city_agent','region']) }} as dim_agent_key,
        *
    from agent_sightings
)


select * from sur_agent_key