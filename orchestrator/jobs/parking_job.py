from dagster import define_asset_job, AssetSelection
from orchestrator.assets.parking import daily_parking_trend
from orchestrator.assets.transit import dhub_transit_monthly_sync

parking_assets = (
    AssetSelection.assets(daily_parking_trend).downstream() | AssetSelection.assets(daily_parking_trend).upstream()
)
transit_assets = (
    AssetSelection.assets(dhub_transit_monthly_sync)
    | AssetSelection.assets(dhub_transit_monthly_sync).downstream()
    | AssetSelection.assets(dhub_transit_monthly_sync).upstream()
)

parking_asset_job = define_asset_job(
    name="parking_asset_job",
    selection=parking_assets | transit_assets,
)
