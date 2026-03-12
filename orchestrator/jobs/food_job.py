from dagster import AssetSelection, define_asset_job
from dagster_dbt import build_dbt_asset_selection
from orchestrator.assets.postgres import mitos_dbt_assets

dbt_selection = build_dbt_asset_selection([mitos_dbt_assets], dbt_select="stg_food_order")
food_assets = dbt_selection.downstream() | dbt_selection.upstream() | AssetSelection.keys("dining_hall_swipes")

food_asset_job = define_asset_job(
    name="food_asset_job",
    selection=food_assets,
)
