WITH historical AS (
    SELECT
        service_date::DATE AS service_date,
        customer_name,
        material,
        diverted,
        tons
    FROM {{ source('raw', 'historical_waste_recycle') }}
),

new_batch AS (
    SELECT
        "Service Date"::DATE AS service_date,
        "Customer Name" AS customer_name,
        "Material" AS material,
        CASE
            WHEN
                "Material" IN (
                    'Compost',
                    'Recycling',
                    'Other',
                    'C & D',
                    'Yard Waste',
                    'Hard-to-Recycle Materials'
                )
                THEN "Tons"
            ELSE 0
        END AS diverted,
        "Tons" AS tons
    FROM {{ source('raw', 'newbatch_waste_recycle') }}
    WHERE "Service Date" IS NOT NULL
),

combined AS (
    SELECT * FROM historical
    UNION ALL
    SELECT * FROM new_batch
)

SELECT
    *,
    {{ fiscal_year('service_date') }}  AS fiscal_year
FROM combined
