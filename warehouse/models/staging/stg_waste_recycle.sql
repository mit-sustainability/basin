WITH historical AS (
    SELECT
        "Date (YYYY-MM-DD)" AS service_date,
        "Building Name" AS customer_name,
        "Waste Stream" AS material,
        "Diverted Tonnage" AS diverted,
        "Total Tonnage" AS tons
    FROM {{ source('raw', 'historical_waste_recycle') }}
),

mapped AS (
    SELECT
        service_date::DATE,
        customer_name,
        CASE
            WHEN material = 'MSW or Trash' THEN 'Trash'
            WHEN material = 'Food Waste' THEN 'Compost'
            WHEN material = 'Bulk Waste' THEN 'C & D'
            WHEN material = 'Other Diversion' THEN 'Other'
            ELSE material
        END AS material,
        diverted,
        tons
    FROM historical
    WHERE service_date IS NOT NULL
    ORDER BY service_date
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
    SELECT * FROM mapped
    UNION ALL
    SELECT * FROM new_batch
)

SELECT * FROM combined
