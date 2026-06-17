from dagster import AssetSelection, define_asset_job

outdoor_heat_job = define_asset_job(
    name="outdoor_heat_job",
    selection=(
        AssetSelection.from_string('key:"outdoor_heat_sensor_config"')
        | AssetSelection.from_string('+key:"staging/stg_outdoor_heat_aligned"+')
        | AssetSelection.from_string('+key:"final/final_outdoor_heat_combined"')
        | AssetSelection.from_string('key:"agol_outdoor_heat_sync"')
    ),
)
