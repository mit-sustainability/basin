WITH lot_total AS (

    SELECT
        date,
        SUM(CASE WHEN parking_lot = '1016' THEN "unique" ELSE 0 END) AS unique_1016,
        SUM(CASE WHEN parking_lot = '1018' THEN "unique" ELSE 0 END) AS unique_1018,
        SUM(CASE WHEN parking_lot = '1022' THEN "unique" ELSE 0 END) AS unique_1022,
        SUM(CASE WHEN parking_lot = '1024' THEN "unique" ELSE 0 END) AS unique_1024,
        SUM(CASE WHEN parking_lot = '1025' THEN "unique" ELSE 0 END) AS unique_1025,
        SUM(CASE WHEN parking_lot = '1030' THEN "unique" ELSE 0 END) AS unique_1030,
        SUM(CASE WHEN parking_lot = '1035' THEN "unique" ELSE 0 END) AS unique_1035,
        SUM(CASE WHEN parking_lot = '1037' THEN "unique" ELSE 0 END) AS unique_1037,
        SUM(CASE WHEN parking_lot = '1038' THEN "unique" ELSE 0 END) AS unique_1038,
        SUM("unique") AS total_unique
    FROM {{ source('raw', 'newbatch_parking_daily') }}
    WHERE
        parking_lot IN ('1010', '1014', '1016', '1018', '1022', '1023', '1024', '1025', '1027', '1030', '1035', '1037', '1038')  -- Replace 'Lot1', 'Lot2', 'Lot3' with your actual lot codes
    GROUP BY date
    ORDER BY date
),

historical AS (
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
        total_unique
    FROM {{ source('raw', 'historical_parking_daily') }}
)

SELECT * FROM historical
UNION ALL
SELECT * FROM lot_total
ORDER BY date
