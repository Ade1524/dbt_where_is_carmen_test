with europe_sighting as (
    select * 
    from {{ ref('EUROPE') }}
)
, columns_format as (
    select  cast(date_witness as date) as date_witness
           ,cast(date_filed as date) as date_agent
           ,cast(witness as varchar) as witness
           ,cast(agent as varchar) as agent
           ,cast(lat_ as float) as latitude
           ,cast(long_ as float) as longitude
           ,cast(city as varchar) as city
           ,cast(country as varchar) as country
           ,cast(region_hq as varchar) as city_agent
           ,cast(armed as boolean) as has_weapon
           ,cast(chapeau as boolean) as has_hat
           ,cast(coat as boolean) as has_jacket
           ,cast(observed_action as varchar) as behavior
           ,'Europe_region' as region 
      from europe_sighting

)



select * from columns_format


