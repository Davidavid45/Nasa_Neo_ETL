{{ config(materialized='table') }}

WITH raw_data AS (
    SELECT 
        reference_id,
        name,
        close_approach_date,
        estimated_diameter_km,
        velocity_km_h,
        miss_distance_km,
        is_potentially_hazardous
    FROM {{ source('dbo', 'NEO_Data') }}
)
SELECT 
    reference_id,
    name,
    close_approach_date,
    estimated_diameter_km,
    velocity_km_h,
    miss_distance_km,
    CASE 
        WHEN is_potentially_hazardous = 1 THEN 'Hazardous'
        ELSE 'Not Hazardous'
    END AS hazard_status
FROM raw_data
