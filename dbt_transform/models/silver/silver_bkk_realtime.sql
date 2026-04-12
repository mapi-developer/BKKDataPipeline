{{ config(
    unique_key='vehicle_id'
) }}

WITH raw_realtime AS (
    SELECT * FROM {{ source('bronze', 'bkk_realtime') }}
)

SELECT
    vehicle_id,
    trip_id,
    route_id,
    CAST(latitude AS DOUBLE) AS latitude,
    CAST(longitude AS DOUBLE) AS longitude,
    -- Cast UNIX timestamp to Databricks TimestampType
    CAST(from_unixtime(timestamp) AS TIMESTAMP) AS event_timestamp,
    
    -- Time-based features for XGBoost
    HOUR(CAST(from_unixtime(timestamp) AS TIMESTAMP)) AS hour_of_day,
    DAYOFWEEK(CAST(from_unixtime(timestamp) AS TIMESTAMP)) AS day_of_week
FROM raw_realtime
WHERE latitude IS NOT NULL 
  AND longitude IS NOT NULL
  
{% if is_incremental() %}
  -- Only process new records arriving since the last dbt run
  AND timestamp > (SELECT MAX(unix_timestamp(event_timestamp)) FROM {{ this }})
{% endif %}