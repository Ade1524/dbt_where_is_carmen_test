{{ config(
    materialized='table'
) }}

with recursive dates as (
    -- Anchor date
    select cast('1985-01-01' as date) as date_day
    union all
    -- Increment day by 1 until end date
    select dateadd(day, 1, date_day)
    from dates
    where date_day < '2022-12-31'
)
, dim_date as (
        select
            date_day,
            year(date_day) as year,
            month(date_day) as month,
            day(date_day) as day_of_month,
            dayofweek(date_day) as day_of_week,  -- Sunday=1, Saturday=7 in Snowflake
            dayname(date_day) as day_name,
            monthname(date_day) as month_name,
            quarter(date_day) as quarter,
            weekofyear(date_day) as week_of_year,
            dayofyear(date_day) as day_of_year,

            -- Week, month, quarter, year start dates
            date_trunc('week', date_day) as week_start_date,
            date_trunc('month', date_day) as month_start_date,
            date_trunc('quarter', date_day) as quarter_start_date,
            date_trunc('year', date_day) as year_start_date

        from dates
)

select 
    {{ dbt_utils.generate_surrogate_key(['date_day']) }} as dim_date_key,
    * 
from dim_date
