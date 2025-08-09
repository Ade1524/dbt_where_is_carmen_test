with australia_sighting as (
    select * 
    from {{ ref('AUSTRALIA') }}
)
, columns_format as (
    select  cast(witnessed as date) as date_witness
           ,cast(reported as date) as date_agent
           ,cast(observer as varchar) as witness
           ,cast(field_chap as varchar) as agent
           ,cast(lat as float) as latitude
           ,cast(long as float) as longitude
           ,cast(place as varchar) as city
           ,cast(nation as varchar) as country
           ,cast(interpol_spot as varchar) as city_agent
           ,cast(has_weapon as boolean) as has_weapon
           ,cast(has_hat as boolean) as has_hat
           ,cast(has_jacket as boolean) as has_jacket
           ,cast(state_of_mind as varchar) as behavior
           ,'Autsralia_region' as region 
      from australia_sighting

)



select * from columns_format



