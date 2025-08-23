{{ config(materialized="table") }}

with
    agent_sightings as (
        select 
            distinct agent, 
            count(agent) as num_agent_reports
        from {{ ref("int_sightings_all_region") }}
        group by agent
    )

, sur_agent_key as (
    select 
        {{ dbt_utils.generate_surrogate_key(['agent']) }} as dim_agent_key,
        *
    from agent_sightings
)


select * from sur_agent_key


