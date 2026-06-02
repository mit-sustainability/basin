from dagster import define_asset_job

indoor_heat_job = define_asset_job(name="indoor_heat_job", selection='+key:"staging/stg_indoor_heat"+')
