WITH filled AS (
    SELECT
        *,
        CASE
            WHEN EXTRACT(MONTH FROM "invoice_date") < 7
                THEN EXTRACT(YEAR FROM "invoice_date")
            ELSE EXTRACT(YEAR FROM "invoice_date") + 1
        END AS fiscal_year,
        EXTRACT(YEAR FROM "invoice_date") AS invoice_year,
        COALESCE("commodity", "po_line_commodity") AS commodity_filled,
        "total"::FLOAT AS spend
    FROM {{ source('raw', 'purchased_goods_invoice') }}
),

attached AS (
    SELECT
        f.*,
        m.level_1,
        m.level_2,
        m.level_3,
        m.code
    FROM filled AS f
    LEFT JOIN {{ source('raw', 'purchased_goods_mapping') }} AS m
        ON f.commodity_filled = m.level_3
),

ef AS (
    SELECT
        a.*,
        n."Supply Chain Emission Factors with Margins" AS emission_factor
    FROM attached AS a
    LEFT JOIN {{ source('raw', 'emission_factor_naics') }} AS n
        ON a.code = n."Code"
    WHERE a.header_status IN ('Approved') AND a.total > 0
),

current_cpi AS (
    SELECT
        year,
        value
    FROM {{ source('raw', 'annual_cpi_index') }} ORDER BY year DESC LIMIT 1
),

target_cpi AS (
    SELECT
        year,
        value
    FROM {{ source('raw', 'annual_cpi_index') }}
    WHERE "year" = 2021
),


tagged_cpi AS (
    SELECT
        ef.*,
        COALESCE(cpi.value, (SELECT value FROM current_cpi)) AS src
    FROM ef
    LEFT JOIN {{ source('raw', 'annual_cpi_index') }} AS cpi
        ON ef.invoice_year = cpi.year
),

adjusted AS (
    SELECT
        sap_invoice_number,
        invoice_number,
        fiscal_year,
        po_number,
        level_1,
        level_2,
        level_3, -- same AS commidty
        total,
        description,
        billing,
        supplier,
        supplier_number,
        cost_object::INTEGER AS cost_object,
        code,
        emission_factor,
        spend * (SELECT value FROM target_cpi) / src AS adjusted_spend_2021,
        spend * (SELECT value FROM current_cpi) / src AS inflated_spend
    FROM tagged_cpi
),

co2kg AS (
    SELECT
        *,
        emission_factor * adjusted_spend_2021 AS ghg
    FROM adjusted

)


SELECT * FROM co2kg -- kgCO2e/dollar
