{% macro fiscal_year(service_date) %}
    CASE
        WHEN EXTRACT(MONTH FROM {{ service_date }}) < 7
            THEN EXTRACT(YEAR FROM {{ service_date }})
        ELSE EXTRACT(YEAR FROM {{ service_date }}) + 1
    END
{% endmacro %}
