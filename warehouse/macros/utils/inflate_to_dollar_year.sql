{#
    Convert a spend amount to the dollar year of its assigned emission factor set:
        amount * CPI(dollar_year) / CPI(spend_year)
    using BLS annual-average CPI-U from raw.annual_cpi_index.

    - dollar_year: take it from emission_factor_by_fiscal_year.dollar_year; never hardcode it.
      If CPI for dollar_year is missing the result is NULL (never a silently different year).
    - spend_year: the calendar year of the transaction date when one exists (matches the annual
      CPI average); the fiscal year only for sources that are already annual (construction, DOF).
      If CPI for spend_year is missing (e.g. the current year), the latest available CPI is used.
#}
{% macro inflate_to_dollar_year(amount, spend_year, dollar_year) %}
    (({{ amount }})
    * (SELECT cpi_t.value FROM {{ source('raw', 'annual_cpi_index') }} AS cpi_t WHERE cpi_t.year = {{ dollar_year }})
    / COALESCE(
        (SELECT cpi_s.value FROM {{ source('raw', 'annual_cpi_index') }} AS cpi_s WHERE cpi_s.year = {{ spend_year }}),
        (SELECT cpi_l.value FROM {{ source('raw', 'annual_cpi_index') }} AS cpi_l ORDER BY cpi_l.year DESC LIMIT 1)
    ))
{% endmacro %}
