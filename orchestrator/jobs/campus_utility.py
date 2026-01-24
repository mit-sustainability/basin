from dagster import define_asset_job

campus_utility_job = define_asset_job(name="campus_utility_job", selection='+key:"staging/stg_utility_history"+')
