with unique_carmen_behavior as (
    select distinct behavior as unique_behavior,
           count (behavior)as num_behavior
    from {{ ref('int_sightings_all_region') }}
    group by 
        behavior
)
, dim_behavior as (
    select
        {{ dbt_utils.generate_surrogate_key(['unique_behavior']) }} as dim_behavior_key,
        unique_behavior as behavior,
        num_behavior
    from unique_carmen_behavior
)


select * from dim_behavior