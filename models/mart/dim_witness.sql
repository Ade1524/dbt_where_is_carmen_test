{{ config(materialized='table') }}

with witness_sightings as (
    select 
        witness,
        count(witness) as num_sightings_for_each_witness
    from {{ ref('int_sightings_all_region') }}
    group by 
        witness       
)

, sur_witness_key as (
    select 
        {{ dbt_utils.generate_surrogate_key(['witness']) }} as dim_witness_key,
        *
    from witness_sightings
)

select * from sur_witness_key

