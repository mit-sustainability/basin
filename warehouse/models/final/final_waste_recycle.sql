WITH hauler AS (SELECT * FROM {{ref('stg_waste_recycle')}}),

small AS (
    SELECT
        service_date::DATE AS service_date,
        customer_name,
        material,
        diverted,
        tons
    FROM {{source('raw', 'small_stream_recycle')}}
    WHERE tons IS NOT NULL
)

SELECT * FROM hauler
UNION ALL
SELECT * FROM small
ORDER BY service_date
