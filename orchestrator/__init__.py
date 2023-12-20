import os

from dagster import Definitions
from dagster_dbt import DbtCliResource
from orchestrator.resources.postgres_io_manager import PostgreSQLPandasIOManager

from orchestrator.assets.postgres import (
    mitos_dbt_assets,
    output_test_asset,
    input_test_asset,
)
from orchestrator.constants import dbt_project_dir, PG_CREDENTIALS
from orchestrator.schedules.mitos_warehouse import schedules

defs = Definitions(
    assets=[mitos_dbt_assets, output_test_asset, input_test_asset],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
        "postgres_pandas_io": PostgreSQLPandasIOManager(**PG_CREDENTIALS),
    },
)
