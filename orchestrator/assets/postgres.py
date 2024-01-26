from dagster import AssetExecutionContext, asset, AssetIn
from dagster_aws.pipes import PipesLambdaClient
from dagster_dbt import (
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
    DbtCliResource,
    dbt_assets,
)
import pandas as pd

from orchestrator.constants import dbt_manifest_path

dagster_dbt_translator = DagsterDbtTranslator(settings=DagsterDbtTranslatorSettings(enable_asset_checks=True))


@dbt_assets(manifest=dbt_manifest_path, dagster_dbt_translator=dagster_dbt_translator)
def mitos_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


@asset(
    io_manager_key="postgres_pandas_io",
    compute_kind="python",
    group_name="landing",
    key_prefix="raw",
)
def output_test_asset(context: AssetExecutionContext) -> pd.DataFrame:
    # this still works because the resource is still available on the context
    ### Fake dataframe
    df = pd.DataFrame(
        {
            "ID": [1, 2, 3, 4, 5],
            "Name": ["Alice", "Bob", "Charlie", "David", "Eva"],
            "Age": [28, 34, 22, 45, 31],
            "Score": [88.5, 92.3, 67.8, 78.9, 81.0],
        }
    )

    ### Output to postgres
    return df


# AssetIn takes either key_prefix or key
@asset(
    ins={"dbt_table": AssetIn(key=["staging", "stg_orders"], input_manager_key="postgres_pandas_io")},
    compute_kind="python",
    key_prefix="final",
    group_name="final",
)
def input_test_asset(dbt_table) -> None:
    # this still works because the resource is still available on the context
    df = dbt_table.head(5)
    ### Output to postgres
    print(df)


@asset(compute_kind="python", group_name="lambda")
def lambda_pipes_asset(context: AssetExecutionContext, lambda_pipes_client: PipesLambdaClient):
    """Test Dagster's experimental Pipe feature to trigger a lambda function"""
    return lambda_pipes_client.run(
        context=context,
        function_name="convert-xlsx-csv-dir",
        event={"some_parameter_value": 5},
    ).get_materialize_result()
