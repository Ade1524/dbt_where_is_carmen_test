WITH monthly_sightings AS (
  SELECT 
    EXTRACT(MONTH FROM date_witness) as month,
    MONTHNAME(date_witness) as month_name,
    city_agent,
    COUNT(*) as sighting_count
  FROM {{ ref('int_sightings_all_region') }}
  GROUP BY EXTRACT(MONTH FROM date_witness), city_agent, date_witness
),
ranked_regions AS (
  SELECT 
    month,
    month_name,
    city_agent,
    sighting_count,
    ROW_NUMBER() OVER (PARTITION BY month ORDER BY sighting_count DESC) as rank
  FROM monthly_sightings
)
SELECT 
  month_name as month,
  city_agent as most_Likely_Region,
  sighting_count as total_Sightings
FROM ranked_regions 
WHERE rank = 1
ORDER BY month