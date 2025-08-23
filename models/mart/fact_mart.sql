with ordinary_date as (
    select *
    from {{ ref('dim_date') }}
)
, location as (
    select *
    from {{ ref('dim_location') }}
)

, agent as (
    select *
    from {{ ref('dim_agent') }}
)

, witness as (
    select *
    from {{ ref('dim_witness') }}
)
, behavior as (
    select *
    from {{ ref('dim_behavior') }}
)
, classified_sightings as (
    select 
        {{ dbt_utils.generate_surrogate_key(['date_witness']) }} as dim_date_witness_key,
        {{ dbt_utils.generate_surrogate_key(['date_agent']) }} as dim_date_agent_key,
        *,
        case
                when has_weapon = true then 1
                else 0
            end as is_armed,
            case
                when has_jacket = true then 1
                else 0
            end as has_a_jacket,
            case
                when has_hat = true then 1
                else 0
            end as has_a_hat
    from {{ ref('int_sightings_all_region') }}
)
, regions_sightings as (
    select 
            *,
            case
                when is_armed = 1
                and has_a_jacket = 1
                and has_a_hat = 1 then 'Is_armed_with_jacket_hat'
                when is_armed = 1
                and has_a_jacket = 0
                and has_a_hat = 0 then 'Armed_Only'
                when has_a_jacket = 1
                and has_a_hat = 1
                and is_armed = 0 then 'With_jacket_and_hat_but_Unarmed'
                when is_armed = 0
                and has_a_jacket = 0
                and has_a_hat = 0 then 'Unarmed_and_no_jacket_and_hat'
                when is_armed = 1
                and has_a_jacket = 1
                and has_a_hat = 0 then 'Is_armed_but_no_hat'
                else 'Other'
            end as suspect_category
        from classified_sightings
)

select 
    rs.date_witness,
    rs.dim_date_witness_key,
    w.dim_witness_key,
    lo.dim_location_key,
    rs.latitude,
    rs.longitude,
    rs.date_agent,
    rs.dim_date_agent_key,
    a.dim_agent_key,
    b.dim_behavior_key,
    rs.is_armed,
    rs.has_a_jacket,
    rs.has_a_hat,
    rs.suspect_category

from regions_sightings rs
left join behavior b on rs.behavior = b.behavior
left join witness w on rs.witness = w.witness
left join agent a on rs.agent = a.agent
left join location lo on rs.latitude = lo.latitude
left join ordinary_date dd on rs.date_witness = dd.date_day
