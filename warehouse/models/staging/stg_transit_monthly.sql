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
        downloaded_at
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
        downloaded_at
    FROM {{ source('raw', 'newbatch_transit_monthly') }}
),

combined AS (
    SELECT historical.*
    FROM historical
    LEFT JOIN new_batch ON historical.month = new_batch.month
    WHERE new_batch.month IS NULL

    UNION ALL

    SELECT * FROM new_batch
)

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
    downloaded_at
FROM combined
ORDER BY month
