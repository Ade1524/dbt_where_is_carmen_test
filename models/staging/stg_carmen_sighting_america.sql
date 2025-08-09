with america_sighting as (
    select * 
    from {{ ref('AMERICA') }}
)
, columns_format as (
    select  cast(date_witness as date) as date_witness
           ,cast(date_agent as date) as date_agent
           ,cast(witness as varchar) as witness
           ,cast(agent as varchar) as agent
           ,cast(latitude as float) as latitude
           ,cast(longitude as float) as longitude
           ,cast(city as varchar) as city
           ,cast(country as varchar) as country
           ,cast(region_hq as varchar) as city_agent
           ,cast(has_weapon as boolean) as has_weapon
           ,cast(has_hat as boolean) as has_hat
           ,cast(has_jacket as boolean) as has_jacket
           ,cast(behavior as varchar) as behavior
           ,'America_region' as region 
      from america_sighting

)



select * from columns_format
