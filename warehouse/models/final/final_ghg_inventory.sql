{% set last_complete_fy = 2024 %}

WITH all_entries AS (
    SELECT
        category,
        emission,
        fiscal_year,
        "scope",
        current_timestamp::timestamp AS "last_update"
    FROM {{ ref('stg_ghg_inventory')}}
)

SELECT * FROM all_entries
WHERE fiscal_year = {{ last_complete_fy }}
