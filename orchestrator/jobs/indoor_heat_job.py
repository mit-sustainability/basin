from dagster import AssetSelection, define_asset_job

indoor_heat_job = define_asset_job(
    name="indoor_heat_job",
    selection=(
        AssetSelection.from_string('key:"indoor_heat_sensor_config"')
        | AssetSelection.from_string('+key:"staging/stg_indoor_heat_aligned"+')
        | AssetSelection.from_string('+key:"final/final_indoor_heat_combined"')
        | AssetSelection.from_string('key:"indoor_heat_export"')
    ),
)
