WITH hauler AS (SELECT * FROM {{ref('stg_waste_recycle')}}),

small AS (
    SELECT
        service_date::DATE AS service_date,
        customer_name,
        material,
        diverted,
        tons,
        {{ fiscal_year('service_date') }}  AS fiscal_year
    FROM {{source('raw', 'small_stream_recycle')}}
    WHERE tons IS NOT NULL
),

combined AS (
    SELECT * FROM hauler
    UNION ALL
    SELECT * FROM small
)

-- deduplicate data entry
SELECT DISTINCT * FROM combined
