WITH top_behaviors AS (
  SELECT 
    b.behavior,
    b.dim_behavior_key,
    COUNT(fc.*) as frequency,
  FROM {{ ref('fact_mart') }} fc
  left join {{ ref('dim_behavior') }} b
  on fc.dim_behavior_key = b.dim_behavior_key
  GROUP BY b.behavior,b.dim_behavior_key
  ORDER BY frequency DESC
  LIMIT 3
),
monthly_behavior_stats AS (
  SELECT 
    EXTRACT(MONTH FROM date_witness) as month,
    MONTHNAME(date_witness) as month_name,
    COUNT(*) as total_monthly_sightings,
    COUNT(CASE 
            WHEN fc.dim_behavior_key IN (SELECT dim_behavior_key FROM top_behaviors) 
            THEN 1 
          END) as top_behavior_sightings
  FROM {{ ref('fact_mart') }} fc
  GROUP BY EXTRACT(MONTH FROM date_witness), MONTHNAME(date_witness)
)
SELECT 
  month_name as month,
  total_monthly_sightings as total_Sightings,
  top_behavior_sightings as top_Behavior_Sightings,
  ROUND(
    (top_behavior_sightings * 100.0 / total_monthly_sightings), 2
  ) as probability_Percent
FROM monthly_behavior_stats
ORDER BY month