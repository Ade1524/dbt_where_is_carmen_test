with asia_sighting as (
    select * 
    from {{ ref('ASIA') }}
)
, columns_format as (
    select  cast(sighting as date) as date_witness
           ,cast(date_agent as date) as date_agent
           ,cast(citizen as varchar) as witness
           ,cast(officer as varchar) as agent
           ,cast(latitude as float) as latitude
           ,cast(longitude as float) as longitude
           ,cast(city as varchar) as city
           ,cast(nation as varchar) as country
           ,cast(city_interpol as varchar) as city_agent
           ,cast(has_weapon as boolean) as has_weapon
           ,cast(has_hat as boolean) as has_hat
           ,cast(has_jacket as boolean) as has_jacket
           ,cast(behavior as varchar) as behavior
           ,'Asia_region' as region 
      from asia_sighting

)



select * from columns_format

