from dagster import define_asset_job, AssetSelection
from dagster_dbt import build_dbt_asset_selection
from orchestrator.assets.postgres import mitos_dbt_assets

# Test selection individual dbt assets
dbt_selection = build_dbt_asset_selection([mitos_dbt_assets], dbt_select="stg_travel_spending")
# Select both downstream and upstream of the stg_travel_spending dbt model
business_travel = dbt_selection.downstream() | dbt_selection.upstream() | AssetSelection.keys("all_scope_summary")

business_asset_job = define_asset_job(name="business_asset_job", selection=business_travel)
