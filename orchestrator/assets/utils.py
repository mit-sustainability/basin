"""Shared objects and functions for all assets."""

from datetime import datetime
import random
from queue import Queue  # Use Queue for thread-safe results collection
from typing import List

import aiohttp
import asyncio
from dagster import asset, AssetIn, ResourceParam, get_dagster_logger
import numpy as np
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
        date_stamp = datetime.now().strftime("%Y-%m-%d")
        description = f"{description} - {date_stamp}"
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


async def post_api_request(
    url: str,
    data: dict,
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    max_retries: int = 3,
    base_backoff_seconds: float = 0.5,
):
    """Make API request with retries and bounded concurrency."""
    for attempt in range(max_retries + 1):
        try:
            async with semaphore:
                async with session.post(url, json=data) as response:
                    # Retry only transient server throttling/failures.
                    if response.status in (429, 500, 502, 503, 504):
                        if attempt == max_retries:
                            response.raise_for_status()
                        retry_after = response.headers.get("Retry-After")
                        wait_seconds = (
                            float(retry_after)
                            if retry_after and retry_after.isdigit()
                            else base_backoff_seconds * (2**attempt) + random.uniform(0, 0.2)
                        )
                        await asyncio.sleep(wait_seconds)
                        continue

                    # Non-transient HTTP errors should fail fast (avoid wasting retries).
                    response.raise_for_status()
                    return await response.json()
        except (aiohttp.ClientConnectionError, asyncio.TimeoutError):
            if attempt == max_retries:
                raise
            # Add jitter to reduce synchronized retry storms.
            wait_seconds = base_backoff_seconds * (2**attempt) + random.uniform(0, 0.2)
            await asyncio.sleep(wait_seconds)


async def fetch_all_async(
    url: str,
    data_list: List[dict],
    max_concurrency: int = 10,
    request_timeout_seconds: int = 60,
    connect_timeout_seconds: int = 10,
    max_retries: int = 3,
):
    """Fetch multiple results async using asyncio and aiohttp

    Args:
        url: API endpoint to request from
        data_list: list of data payload dictionaries
        max_concurrency: number of requests allowed in-flight at once
        request_timeout_seconds: overall timeout per request
        connect_timeout_seconds: timeout for connection setup
        max_retries: number of retries per request
    Returns:
        list of results from the API requests.
    """
    if len(data_list) == 0:
        return []

    results = Queue()  # Thread-safe queue for results
    tasks = []
    timeout = aiohttp.ClientTimeout(total=request_timeout_seconds, connect=connect_timeout_seconds)
    semaphore = asyncio.Semaphore(max_concurrency)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        for data in data_list:
            tasks.append(
                asyncio.create_task(
                    post_api_request(
                        url=url,
                        data=data,
                        session=session,
                        semaphore=semaphore,
                        max_retries=max_retries,
                    )
                )
            )

        # Wait for all tasks to complete and append results in order
        for task in tasks:
            result = await task
            results.put(result)  # Thread-safe addition to queue

    # Collect results from the queue in order
    final_results = []
    while not results.empty():
        final_results.append(results.get())
    return final_results


def to_mmbtu(row: pd.Series) -> float:
    """Convert MIT utility usage to MMBtu for downstream allocation charts."""

    conversion_factors = {
        ("Gas", "THM"): 0.1,
        ("Gas", "CCF"): 0.1037,
        ("#2 Oil", "GAL"): 0.139,
        ("#2 Oil", "BBL"): 5.84,  # 42 Gallon/Barrel (but only #6 use Barrel)
        ("#6 Oil", "GAL"): 0.151,
        ("#6 Oil", "BBL"): 6.3,
        ("Electricity", "KWH"): 0.003412,
        ("Electricity", "MWH"): 3.412,
        ("Produced Electricity", "KWH"): 0.003412,
        ("Steam", "MLB"): 1.195,  # MLB = 1000 LB
        ("Water", "CCF"): 0.0,
    }
    return row["utility_usage"] * conversion_factors.get((row["utility_type"], row["utility_unit"]), np.nan)
