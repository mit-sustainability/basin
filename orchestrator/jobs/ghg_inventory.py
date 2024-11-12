from dagster import define_asset_job
from dagster_dbt import build_dbt_asset_selection
from orchestrator.assets.postgres import mitos_dbt_assets

dbt_selection = build_dbt_asset_selection([mitos_dbt_assets], dbt_select="stg_ghg_inventory")
ghg_assets = dbt_selection.downstream() | dbt_selection.upstream()

ghg_job = define_asset_job(
    name="ghg_inventory_job",
    selection=ghg_assets,
)
