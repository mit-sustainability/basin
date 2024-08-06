"""Shared objects and functions for all assets."""

from datetime import datetime
from queue import Queue  # Use Queue for thread-safe results collection
from typing import List

import aiohttp
import asyncio
from dagster import asset, AssetIn, ResourceParam, get_dagster_logger
import pandas as pd
import pandera as pa
import pytz
import regex as re


from orchestrator.resources.datahub import DataHubResource


logger = get_dagster_logger()


def empty_dataframe_from_model(model: pa.DataFrameModel) -> pd.DataFrame:
    """An empty dataframe model to ensure pandera check"""
    schema = model.to_schema()
    return pd.DataFrame(columns=schema.dtypes.keys()).astype({col: str(dtype) for col, dtype in schema.dtypes.items()})


def add_dhub_sync(asset_name: str, table_key: List[str], config: dict):
    """Create the asset that sync a table to DataHub providing config
    Args:
        asset_name: name of the resulting asset.
        table_key: Dagster asset key to be synced to DataHub.
        config: configuration for dhub target object.
    Returns:
        dagster asset syncing the input asset key to the datahub.
    """

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
        ext = config.get("ext", "csv")
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
        if ext != "csv":
            meta.update({"mimeType": "application/gzip"})
        dhub.sync_dataframe(table, meta, ext=ext)

    return dhub_ingest


def str2datetime(tstring: str, fmat: str = "%Y-%m-%dT%H:%M:%S") -> datetime:
    """Convert string to datetime"""
    return datetime.strptime(tstring, fmat).replace(tzinfo=pytz.UTC)


def normalize_column_name(col_name):
    """Normalize column name to lowercase and replace special characters"""
    col_name = col_name.replace("#", "Number")
    col_name = re.sub(r"([a-z])([A-Z])", r"\1_\2", col_name)
    col_name = col_name.replace(" ", "_")
    col_name = col_name.lower()
    # Remove duplicate underscores
    col_name = re.sub(r"_+", "_", col_name)
    return col_name


async def post_api_request(url, data, session):
    """Make API request with async"""
    async with session.post(url, json=data) as response:
        response.raise_for_status()  # Raise an exception for non-2xx status codes
        return await response.json()


async def fetch_all_async(url: str, data_list: List[dict]):
    """Fetch multiple results async using asyncio and aiohttp

    Args:
        url: API endpoint to request from
        data_list: list of data payload dictionaries
    Returns:
        list of results from the API requests.
    """
    results = Queue()  # Thread-safe queue for results
    tasks = []

    async with aiohttp.ClientSession() as session:
        for data in data_list:
            tasks.append(asyncio.ensure_future(post_api_request(url, data, session)))

        # Wait for all tasks to complete and append results in order
        for task in tasks:
            result = await task
            results.put(result)  # Thread-safe addition to queue

    # Collect results from the queue in order
    final_results = []
    while not results.empty():
        final_results.append(results.get())
    return final_results
