SELECT
    fiscal_year,
    customer_name,
    sum(spend) AS total_spend,
    sum(weight_co2eq) AS weight_co2eq,
    sum(combined_co2eq) AS combined_co2eq
FROM {{ref('stg_food_order')}}
GROUP BY customer_name, fiscal_year
ORDER BY fiscal_year, customer_name
