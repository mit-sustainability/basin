SELECT
    a.sensor_id,
    c.floor,
    c.orientation,
    c.window_state,
    c.blinds_state,
    c.note,
    CAST(a.datetime_edt AS DATE)   AS reading_date,
    AVG(a.temperature_f)           AS avg_temperature_f,
    MAX(a.temperature_f)           AS max_temperature_f,
    MIN(a.temperature_f)           AS min_temperature_f,
    AVG(a.relative_humidity_pct)   AS avg_relative_humidity_pct,
    MAX(a.relative_humidity_pct)   AS max_relative_humidity_pct,
    MIN(a.relative_humidity_pct)   AS min_relative_humidity_pct,
    AVG(a.dew_point_f)             AS avg_dew_point_f,
    MIN(a.dew_point_f)             AS min_dew_point_f,
    MAX(a.dew_point_f)             AS max_dew_point_f,
    AVG(a.heat_index_f)            AS avg_heat_index_f,
    MAX(a.heat_index_f)            AS max_heat_index_f,
    COUNT(*)                       AS num_readings
FROM {{ source("staging", "stg_indoor_heat_aligned") }} a
LEFT JOIN {{ source("raw", "indoor_heat_sensor_config") }} c
    ON a.sensor_id = c.sensor_id
GROUP BY
    a.sensor_id, c.floor, c.orientation, c.window_state, c.blinds_state, c.note,
    CAST(a.datetime_edt AS DATE)
