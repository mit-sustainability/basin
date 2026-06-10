SELECT
    a.*, c.hobo_id, c.calibration_id, c.floor, c.orientation, c.window_state, c.blinds_state, c.note, c.sensor_photo, c.window_photo
FROM {{ source("staging", "stg_indoor_heat_aligned") }} a
LEFT JOIN {{ source("raw", "indoor_heat_sensor_config") }} c
    ON lower(a.sensor_id) = c.sensor_id