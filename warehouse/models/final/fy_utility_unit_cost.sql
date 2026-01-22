WITH cat AS (
    SELECT
        *,
        CASE
            WHEN
                cost_collector_id IN ('1814000', '1814200', '1814201', '1814202', '1814204')
                THEN 'CUP'
            ELSE 'Non-CUP'
        END AS cup
    FROM {{ source('staging', 'stg_utility_history') }}
    WHERE building_group = 'main'
),


ut_total AS (
    SELECT
        fiscal_year,
        utility_type,
        cup,
        sum(utility_cost) AS total_cost,
        sum(utility_usage) AS total_usage,
        sum(utility_mmbtu) AS total_mmbtu
    FROM cat
    GROUP BY fiscal_year, utility_type, cup
    ORDER BY fiscal_year, utility_type
)


SELECT
    *,
    CASE
        -- Priority 1: Use MMBTU if it exists and is not zero (Energy Utilities)
        WHEN utility_type IN ('#2 Oil', '#6 Oil', 'Gas')
            THEN total_cost / nullif(total_mmbtu, 0)

        -- Priority 2: Fallback to base Usage if MMBTU is missing (Water/Sewer)
        ELSE total_cost / nullif(total_usage, 0)
    END AS unit_cost
FROM ut_total
WHERE utility_type IN ('#2 Oil', '#6 Oil', 'Gas', 'Electricity', 'Water', 'Sewer')
