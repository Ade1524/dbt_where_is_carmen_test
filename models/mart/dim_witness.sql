with witness_sightings as (
    select 
        witness,
        city,
        country,
        region,
        count(witness) as num_witness_sightings
    from {{ ref('int_sightings_all_region') }}
    group by 
        witness,
        city, 
        country,
        region
)
, sur_witness_key as (
    select 
        {{ dbt_utils.generate_surrogate_key(['witness', 'city', 'country','region']) }} as dim_witness_key,
        *
    from witness_sightings
)


select * from sur_witness_key