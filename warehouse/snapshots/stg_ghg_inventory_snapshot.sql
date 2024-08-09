{% snapshot ghg_inventory_snapshot %}

    {{
        config(
          target_schema='snapshots',
          strategy='timestamp',
          unique_key="fiscal_year||'-'||category",
          updated_at='last_update'
        )
    }}

select
    fiscal_year||'-'||category as id,
    *
from {{ ref('stg_ghg_inventory') }}

{% endsnapshot %}
