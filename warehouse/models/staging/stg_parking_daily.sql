WITH tracked_lots AS (

    SELECT CAST(parking_lot AS TEXT) AS parking_lot
    FROM {{ ref('parking_tracked_lots') }}
),

lot_total AS (

    SELECT
        CAST(source.date AS DATE) AS date,
        SUM(CASE WHEN t.parking_lot = '1016' THEN source."unique" ELSE 0 END) AS unique_1016,
        SUM(CASE WHEN t.parking_lot = '1018' THEN source."unique" ELSE 0 END) AS unique_1018,
        SUM(CASE WHEN t.parking_lot = '1022' THEN source."unique" ELSE 0 END) AS unique_1022,
        SUM(CASE WHEN t.parking_lot = '1024' THEN source."unique" ELSE 0 END) AS unique_1024,
        SUM(CASE WHEN t.parking_lot = '1025' THEN source."unique" ELSE 0 END) AS unique_1025,
        SUM(CASE WHEN t.parking_lot = '1030' THEN source."unique" ELSE 0 END) AS unique_1030,
        SUM(CASE WHEN t.parking_lot = '1035' THEN source."unique" ELSE 0 END) AS unique_1035,
        SUM(CASE WHEN t.parking_lot = '1037' THEN source."unique" ELSE 0 END) AS unique_1037,
        SUM(CASE WHEN t.parking_lot = '1038' THEN source."unique" ELSE 0 END) AS unique_1038,
        SUM(CASE WHEN t.parking_lot = '1042' THEN source."unique" ELSE 0 END) AS unique_1042,
        SUM(CASE WHEN t.parking_lot = '1046' THEN source."unique" ELSE 0 END) AS unique_1046,
        SUM(source."unique") AS total_unique
    FROM {{ source('raw', 'newbatch_parking_daily') }} AS source
    INNER JOIN tracked_lots AS t ON CAST(source.parking_lot AS TEXT) = t.parking_lot
    GROUP BY CAST(source.date AS DATE)
),

historical AS (
    SELECT
        CAST(date AS DATE) AS date,
        unique_1016,
        unique_1018,
        unique_1022,
        unique_1024,
        unique_1025,
        unique_1030,
        unique_1035,
        unique_1037,
        unique_1038,
        0 AS unique_1042,
        0 AS unique_1046,
        total_unique
    FROM {{ source('raw', 'historical_parking_daily') }}
),

stacked AS (
    SELECT
        *,
        1 AS source_priority
    FROM historical

    UNION ALL

    SELECT
        *,
        2 AS source_priority
    FROM lot_total
),

deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY date ORDER BY source_priority DESC) AS row_num
    FROM stacked
)

SELECT
    date,
    unique_1016,
    unique_1018,
    unique_1022,
    unique_1024,
    unique_1025,
    unique_1030,
    unique_1035,
    unique_1037,
    unique_1038,
    unique_1042,
    unique_1046,
    total_unique
FROM deduped
WHERE row_num = 1
ORDER BY date
