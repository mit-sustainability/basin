from dagster import AssetSelection, define_asset_job

indoor_heat_job = define_asset_job(
    name="indoor_heat_job",
    selection=AssetSelection.assets("staging/stg_indoor_heat").upstream(),
)
