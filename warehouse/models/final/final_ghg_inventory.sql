WITH all_entries AS (
    SELECT
        category,
        emission,
        fiscal_year,
        "scope",
        current_timestamp AS "last_update"
    FROM {{ ref('stg_ghg_inventory')}}
)

SELECT * FROM all_entries
WHERE fiscal_year = 2023
--WHERE fiscal_year = (SELECT MAX(fiscal_year) FROM all_entries)
