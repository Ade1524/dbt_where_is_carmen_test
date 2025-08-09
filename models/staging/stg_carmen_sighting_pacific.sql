with pacific_sighting as (
    select * 
    from {{ ref('PACIFIC') }}
)
, columns_format as (
    select  cast(sight_on as date) as date_witness
           ,cast(file_on as date) as date_agent
           ,cast(sighter as varchar) as witness
           ,cast(filer as varchar) as agent
           ,cast(lat as float) as latitude
           ,cast(long as float) as longitude
           ,cast(town as varchar) as city
           ,cast(nation as varchar) as country
           ,cast(report_office as varchar) as city_agent
           ,cast(has_weapon as boolean) as has_weapon
           ,cast(has_hat as boolean) as has_hat
           ,cast(has_jacket as boolean) as has_jacket
           ,cast(behavior as varchar) as behavior
           ,'Pacific_region' as region 
      from pacific_sighting

)



select * from columns_format


