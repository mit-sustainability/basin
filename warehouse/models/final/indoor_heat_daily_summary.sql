SELECT
    sensor_id,
    location,
    CAST(datetime_edt AS DATE)   AS reading_date,
    AVG(temperature_f)           AS avg_temperature_f,
    MAX(temperature_f)           AS max_temperature_f,
    MIN(temperature_f)           AS min_temperature_f,
    AVG(relative_humidity_pct)   AS avg_relative_humidity_pct,
    MAX(relative_humidity_pct)   AS max_relative_humidity_pct,
    MIN(relative_humidity_pct)   AS min_relative_humidity_pct,
    AVG(dew_point_f)             AS avg_dew_point_f,
    MIN(dew_point_f)             AS min_dew_point_f,
    MAX(dew_point_f)             AS max_dew_point_f,
    AVG(heat_index_f)            AS avg_heat_index_f,
    MAX(heat_index_f)            AS max_heat_index_f,
    COUNT(*)                     AS num_readings
FROM {{ source("staging", "stg_indoor_heat_aligned") }}
GROUP BY sensor_id, location, CAST(datetime_edt AS DATE)
