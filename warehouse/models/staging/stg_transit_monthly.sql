WITH historical AS (
    SELECT
        month,
        local_bus,
        rapid_transit,
        total_taps,
        active_rider,
        unique_rider,
        active_ratio,
        source_type,
        source_filename,
        downloaded_at,
        statement_month
    FROM {{ source('raw', 'historical_transit_monthly') }}
),

new_batch AS (
    SELECT
        month,
        local_bus,
        rapid_transit,
        total_taps,
        active_rider,
        unique_rider,
        active_ratio,
        source_type,
        source_filename,
        downloaded_at,
        statement_month
    FROM {{ source('raw', 'newbatch_transit_monthly') }}
),

combined AS (
    SELECT * FROM historical
    UNION ALL
    SELECT * FROM new_batch
)

SELECT
    month,
    SUM(local_bus) AS local_bus,
    SUM(rapid_transit) AS rapid_transit,
    SUM(total_taps) AS total_taps,
    SUM(active_rider) AS active_rider,
    SUM(unique_rider) AS unique_rider,
    CASE
        WHEN SUM(unique_rider) = 0 THEN 0
        ELSE SUM(active_rider)::FLOAT / SUM(unique_rider)
    END AS active_ratio,
    'combined' AS source_type,
    MAX(downloaded_at) AS downloaded_at,
    MAX(statement_month) AS statement_month
FROM combined
GROUP BY month
ORDER BY month
