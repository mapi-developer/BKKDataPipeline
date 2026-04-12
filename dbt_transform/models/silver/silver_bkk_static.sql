-- Joining the daily static GTFS data dumped by your Airflow DAG
SELECT
    trip_id,
    route_id,
    service_id,
    trip_headsign
FROM {{ source('bronze', 'static_trips') }}