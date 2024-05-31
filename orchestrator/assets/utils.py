"""Shared objects and functions for all assets."""

from datetime import datetime
from dagster import asset, AssetIn, ResourceParam, get_dagster_logger
import pytz

from typing import List
import pandas as pd
import pandera as pa
from orchestrator.resources.datahub import DataHubResource


logger = get_dagster_logger()


def empty_dataframe_from_model(Model: pa.DataFrameModel) -> pd.DataFrame:
    """An empty dataframe model to ensure pandera check"""
    schema = Model.to_schema()
    return pd.DataFrame(columns=schema.dtypes.keys()).astype({col: str(dtype) for col, dtype in schema.dtypes.items()})


def add_dhub_sync(asset_name: str, table_key: List[str], config: dict):
    """Create the asset that sync a table to DataHub providing config"""

    @asset(
        name=asset_name,
        compute_kind="python",
        group_name="dhub_sync",
        ins={
            "table": AssetIn(
                key=table_key,
                input_manager_key="postgres_replace",
            )
        },
    )
    def dhub_ingest(table, dhub: ResourceParam[DataHubResource]):
        filename = config.get("filename")
        project_name = config.get("project_name")
        title = config.get("title", filename)
        description = config.get("description")
        project_id = dhub.get_project_id(project_name)
        logger.info(f"Sync to project: {project_id}!")
        meta = {
            "name": filename,
            "mimeType": "text/csv",
            "storageContainer": project_id,
            "destination": "shared-project",
            "title": title,
            "description": description,
            "privacy": "public",
            "organizations": ["MITOS"],
        }
        dhub.sync_dataframe_to_csv(table, meta)

    return dhub_ingest


def str2datetime(tstring: str, fmat: str = "%Y-%m-%dT%H:%M:%S") -> datetime:
    """Convert string to datetime"""
    return datetime.strptime(tstring, fmat).replace(tzinfo=pytz.UTC)


def normalize_column_name(col_name):
    """Normalize column name to lowercase and replace special characters"""
    col_name = col_name.lower()
    col_name = col_name.replace(" ", "_")
    col_name = col_name.replace("#", "number")
    return col_name
