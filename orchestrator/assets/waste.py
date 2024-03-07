from dagster import (
    asset,
    get_dagster_logger,
    ResourceParam,
)

import numpy as np
import pandas as pd

from orchestrator.assets.utils import empty_dataframe_from_model, add_dhub_sync
from orchestrator.resources.datahub import DataHubResource

logger = get_dagster_logger()


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def historical_waste_recycle(dhub: ResourceParam[DataHubResource]):
    """This asset ingest the historical_waste_recycle data from the Data Hub"""
    project_id = dhub.get_project_id("Material Matters")
    logger.info(f"Found project id: {project_id}!")
    download_links = dhub.search_files_from_project(project_id, "Historical_Waste_Recycle")
    if len(download_links) == 0:
        logger.error("No download links found!")
        return pd.DataFrame()
    # Load the data
    workbook = pd.ExcelFile(download_links[0], engine="openpyxl")
    df = pd.read_excel(workbook, usecols="A:F", sheet_name="Sheet1")

    return df


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def newbatch_waste_recycle(dhub: ResourceParam[DataHubResource]):
    """This asset ingest the new batch of waste data from the Data Hub"""
    project_id = dhub.get_project_id("Material Matters")
    logger.info(f"Found project id: {project_id}!")
    download_links = dhub.search_files_from_project(project_id, "MIT Data 7.1.23-12.31.23")
    if len(download_links) == 0:
        logger.error("No download links found!")
        return pd.DataFrame()
    # Load the data
    workbook = pd.ExcelFile(download_links[0], engine="openpyxl")
    sheet1 = pd.read_excel(workbook, sheet_name=0)
    sts = pd.read_excel(workbook, sheet_name=1)
    cols = [
        "Customer Key",
        "Customer Name",
        "Service Street",
        "Service Date",
        "Material",
        "Tons",
    ]
    combined = pd.concat([sheet1[cols], sts[cols]], axis=0)
    return combined


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def small_stream_recycle(dhub: ResourceParam[DataHubResource]):
    """This asset ingest the hard-to-recycle data from the Data Hub"""
    project_id = dhub.get_project_id("Material Matters")
    logger.info(f"Found project id: {project_id}!")
    download_links = dhub.search_files_from_project(project_id, "MIT_small_stream.xlsx")
    if len(download_links) == 0:
        logger.error("No download links found!")
        return pd.DataFrame()
    # Load the data
    workbook = pd.ExcelFile(download_links[0], engine="openpyxl")
    df = pd.read_excel(
        workbook,
        usecols="B:M",
        skiprows=13,
        nrows=1,
        sheet_name="2023 small stream recycling",
        header=None,
    )
    df_out = df.T
    df_out.columns = ["tons"]
    df_out["service_date"] = pd.date_range(start="2023-01-01", periods=len(df_out), freq="M")
    df_out["material"] = "Hard-to-Recycle Materials"
    df_out["customer_name"] = "Small Stream Facility"
    df_out["diverted"] = df_out["tons"]

    return df_out


# Sync to datahub using the factory function
dhub_waste_sync = add_dhub_sync(
    asset_name="dhub_waste_sync",
    table_key=["final", "final_waste_recycle"],
    config={
        "filename": "final_waste_update",
        "project_name": "Material Matters",
        "description": "Processed waste data including historical, Casella and small stream",
        "title": "Processed Waste Data till 2023",
    },
)
