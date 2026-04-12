WITH silver_rt AS (
    SELECT * FROM {{ ref('silver_bkk_realtime') }}
),
silver_static AS (
    SELECT * FROM {{ ref('silver_bkk_static') }}
)

SELECT
    rt.route_id,
    st.trip_headsign,
    rt.hour_of_day,
    rt.day_of_week,
    
    -- Feature: Fleet density (congestion proxy)
    COUNT(DISTINCT rt.vehicle_id) AS active_vehicles_in_window,
    
    -- Feature: Average geospatial clustering per route
    AVG(rt.latitude) AS avg_route_latitude,
    AVG(rt.longitude) AS avg_route_longitude

    -- (Future Feature: Average Delay)
    -- Once TripUpdates is added to the producer:
    -- AVG(tu.delay) AS target_avg_delay_seconds

FROM silver_rt rt
LEFT JOIN silver_static st 
    ON rt.trip_id = st.trip_id
GROUP BY 
    rt.route_id, 
    st.trip_headsign, 
    rt.hour_of_day, 
    rt.day_of_week