{{ config(materialized='table') }}

SELECT
    a.sensor_id,
    a.datetime_edt,
    a.temperature_f,
    a.relative_humidity_pct,
    a.dew_point_f,
    a.heat_index_f,
    c.sensor_name,
    c.deployment
FROM {{ source("staging", "stg_outdoor_heat_aligned") }} a
LEFT JOIN {{ source("raw", "outdoor_heat_sensor_config") }} c
    ON a.sensor_id = c.sensor_id
