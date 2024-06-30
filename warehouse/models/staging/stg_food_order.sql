WITH cat AS (
    SELECT
        customer_name,
        contract_month,
        CASE
            WHEN EXTRACT(MONTH FROM "contract_month") < 7
                THEN EXTRACT(YEAR FROM "contract_month")
            ELSE EXTRACT(YEAR FROM "contract_month") + 1
        END AS fiscal_year,
        mfr_item_description,
        dist_item_description,
        lbs,
        gal,
        spend,
        wri_category,
        simap_category
    FROM {{ source('staging', 'food_order_categorize') }}
),


attached AS (
    SELECT
        c.*,
        f."spend-based emissions factor (kg CO2e/2021 USD)" AS spend_emission_factor,
        f."weight-based emissions factor (kg CO2e/kg food)" AS weight_emission_factor
    FROM cat AS c
    LEFT JOIN {{ source('raw', 'food_emission_factor') }} AS f
        ON c.simap_category = f.simap_category
),

wgt AS (
    SELECT
        a.*,
        COALESCE(a.lbs, 0) * 0.453592 + COALESCE(a.gal, 0) * 3.78541 AS weight_kg
    FROM attached AS a
),

ghg AS (
    SELECT
        w.*,
        w.weight_kg * w.weight_emission_factor AS co2eq
    FROM wgt AS w

)

SELECT * FROM ghg
