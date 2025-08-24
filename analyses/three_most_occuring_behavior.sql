with behavior as (
    SELECT 
        b.behavior,
        COUNT(fc.*) as frequency
    FROM {{ ref('fact_mart') }} fc
    left join {{ ref('dim_behavior') }} b
    on fc.dim_behavior_key = b.dim_behavior_key
    GROUP BY behavior
    ORDER BY frequency DESC    
)

select * from behavior 
