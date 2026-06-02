SELECT
    sensor_id,
    location,
    CAST(datetime_edt AS DATE) AS reading_date,
    AVG(temperature_c)          AS avg_temperature_c,
    MAX(temperature_c)          AS max_temperature_c,
    MIN(temperature_c)          AS min_temperature_c,
    AVG(relative_humidity_pct)  AS avg_relative_humidity_pct,
    MAX(relative_humidity_pct)  AS max_relative_humidity_pct,
    MIN(relative_humidity_pct)  AS min_relative_humidity_pct,
    AVG(dew_point_c)            AS avg_dew_point_c,
    MIN(dew_point_c)            AS min_dew_point_c,
    MAX(dew_point_c)            AS max_dew_point_c,
    COUNT(*)                    AS num_readings,
    MAX(last_update)            AS last_update
FROM {{ source("staging", "stg_indoor_heat") }}
GROUP BY sensor_id, location, CAST(datetime_edt AS DATE)
