with monthly_characteristics as (
  select
    EXTRACT(MONTH FROM date_witness) as month,
    MONTHNAME(date_witness) as month_name,
    COUNT(*) as total_sightings,
    COUNT(CASE 
             WHEN is_armed = 1 
             AND has_a_jacket = 1
             AND has_a_hat = 0
             THEN 1 
            END) as armed_jacket_no_hat_count
  FROM {{ ref('fact_mart') }} 
  GROUP BY 
    EXTRACT(MONTH FROM date_witness),
     MONTHNAME(date_witness)
  
)

SELECT 
  month_name as month,
  total_sightings as total_Sightings,
  armed_jacket_no_hat_count,
  ROUND(
    (armed_jacket_no_hat_count * 100.0 / total_sightings), 2
  ) as probability_Percent
FROM monthly_characteristics
ORDER BY month