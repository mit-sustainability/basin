from dagster import define_asset_job, AssetSelection
from orchestrator.assets.parking import daily_parking_trend

parking_assets = (
    AssetSelection.assets(daily_parking_trend).downstream() | AssetSelection.assets(daily_parking_trend).upstream()
)

parking_asset_job = define_asset_job(
    name="parking_asset_job",
    selection=parking_assets,
)
