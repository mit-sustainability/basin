from datetime import datetime

from dagster import (
    asset,
    get_dagster_logger,
    ResourceParam,
)
from dagster_pandera import pandera_schema_to_dagster_type
import pandera as pa
from pandera.typing import Series, DateTime

import pandas as pd

from orchestrator.assets.utils import (
    empty_dataframe_from_model,
    add_dhub_sync,
    normalize_column_name,
)
from orchestrator.resources.datahub import DataHubResource

logger = get_dagster_logger()


class WasteNewbatchData(pa.SchemaModel):
    """Validate the output data schema of newbatch waste asset"""

    customer_key: Series[str] = pa.Field(alias="Customer Key", description="Waste collection building id")
    customer_name: Series[str] = pa.Field(alias="Customer Name", description="Waste collection building name")
    service_street: Series[str] = pa.Field(alias="Service Street", description="Waste collection site street")
    service_date: Series[DateTime] = pa.Field(alias="Service Date", description="Service Date")
    Material: Series[str] = pa.Field(
        isin=["Trash", "Compost", "Recycling", "C & D", "Yard Waste", "Other"],
        description="Waste category",
    )
    Tons: Series[float] = pa.Field(description="Total tonnage of waste collected")


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def historical_waste_recycle(dhub: ResourceParam[DataHubResource]):
    """This asset ingest the historical_waste_recycle data from the Data Hub"""
    project_id = dhub.get_project_id("Material Matters")
    logger.info(f"Found project id: {project_id}!")
    download_links = dhub.search_files_from_project(project_id, "historical_waste_recycle_june2023")
    if len(download_links) == 0:
        logger.error("No download links found!")
        return pd.DataFrame()
    # Load the data
    workbook = pd.ExcelFile(download_links[0], engine="openpyxl")
    df = pd.read_excel(workbook, usecols="A:F", sheet_name="Sheet1")
    logger.info(f"Loaded historical waste recycle data with shape: {df.shape}!")
    return df


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
    dagster_type=pandera_schema_to_dagster_type(WasteNewbatchData),
)
def newbatch_waste_recycle(dhub: ResourceParam[DataHubResource]):
    """This asset ingest the new batch of waste data from the Data Hub"""
    project_id = dhub.get_project_id("Material Matters")
    logger.info(f"Found project id: {project_id}!")
    download_links = dhub.search_files_from_project(project_id, "MIT Data 7.1.23-12.31.23")
    if len(download_links) == 0:
        logger.error("No download links found!")
        return empty_dataframe_from_model(WasteNewbatchData)
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
    combined.dropna(inplace=True)
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
    download_links = dhub.search_files_from_project(project_id, "MIT_small_stream_waste")
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


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def waste_emission_factors_EPA(dhub: ResourceParam[DataHubResource]):
    """This asset ingest the Commuting Emission Factors by vehicle
    type from EPA Emission Factors Hub."""
    project_id = dhub.get_project_id("Scope3 General")
    logger.info(f"Found project id: {project_id}!")
    download_links = dhub.search_files_from_project(project_id, "EPA_GHG_emission_factors_June_2024")
    if len(download_links) == 0:
        logger.error("No download links found!")
        return pd.DataFrame()
    # Load the data
    workbook = pd.ExcelFile(download_links[0], engine="openpyxl")

    # Get GWP to calculate CO2-eq
    ef = pd.read_excel(
        workbook,
        usecols="C:I",
        skiprows=425,
        nrows=61,
    )
    mapping = {
        "RecycledA": "Recycled",
        "LandfilledB": "Landfilled",
        "CombustedC": "Combusted",
        "CompostedD": "Composted",
        "Anaerobically Digested (Dry Digestate with Curing)E": "Dry Anaerobically Digested",
        "Anaerobically Digested (Wet  Digestate with Curing)E": "Wet Anaerobically Digested",
    }
    df_out = ef.rename(columns=mapping)
    df_out.columns = [normalize_column_name(col) for col in df_out.columns]
    df_out["last_update"] = datetime.now()

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
