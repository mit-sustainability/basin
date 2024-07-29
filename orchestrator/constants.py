import os
from pathlib import Path

from dagster import EnvVar
from dagster_dbt import DbtCliResource

dbt_project_dir = Path(__file__).joinpath("..", "..", "warehouse").resolve()
dbt = DbtCliResource(project_dir=os.fspath(dbt_project_dir))
PG_CREDENTIALS = {
    "user": EnvVar("PG_USER"),
    "host": EnvVar("PG_WAREHOUSE_HOST"),
    "password": EnvVar("PG_PASSWORD"),
    "database": "postgres",
}
DWRHS_CREDENTIALS = {
    "user": EnvVar("DWRHS_USER"),
    "host": EnvVar("DWRHS_HOST"),
    "password": EnvVar("DWRHS_PASSWORD"),
    "sid": "DWRHS",
}
dh_api_key = os.getenv("DATAHUB_API_KEY")
PLATFORM_ENV = os.getenv("PLATFORM_ENV")
food_cat_endpoint = os.getenv("FOOD_API_ENDPOINT")

# If DAGSTER_DBT_PARSE_PROJECT_ON_LOAD is set, a manifest will be created at run time.
# Otherwise, we expect a manifest to be present in the project's target directory.
if os.getenv("DAGSTER_DBT_PARSE_PROJECT_ON_LOAD"):
    dbt_parse_invocation = dbt.cli(["parse"]).wait()
    dbt_manifest_path = dbt_parse_invocation.target_path.joinpath("manifest.json")
else:
    dbt_manifest_path = dbt_project_dir.joinpath("target", "manifest.json")
