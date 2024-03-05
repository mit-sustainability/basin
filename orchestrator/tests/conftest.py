import os
import pytest
from pathlib import Path
from dagster_dbt import DbtCliResource


@pytest.fixture(autouse=True)
def dbt_cli_resource(tmp_path):
    # Set the environment variable for the duration of the test session
    # Define the dbt project directory and the target name
    dbt_project_dir = Path(__file__).joinpath("..", "..", "..", "warehouse").resolve()
    dbt_target = "docs"  # Replace with your target name
    # Create an instance of DbtCliResource with the project directory
    dbt_resource = DbtCliResource(project_dir=os.fspath(dbt_project_dir))

    # Generate the dbt manifest if needed
    if os.getenv("DAGSTER_DBT_PARSE_PROJECT_ON_LOAD"):
        dbt_manifest_path = (
            dbt_resource.cli(
                ["--quiet", "parse", "--target", dbt_target],  # Specify the target here
                target_path=tmp_path,  # Use a temporary directory for the target
            )
            .wait()
            .target_path.joinpath("manifest.json")
        )
        assert dbt_manifest_path.exists(), "dbt manifest.json was not created"

    # Yield the DbtCliResource instance for use in tests
    yield dbt_resource
