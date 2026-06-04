from dagster import define_asset_job

indoor_heat_calibration_job = define_asset_job(
    name="indoor_heat_calibration_job",
    selection="indoor_heat_calibration",
)
