from datetime import datetime

from dagster import (
    asset,
    AssetCheckResult,
    AssetCheckSpec,
    Config,
    get_dagster_logger,
    Output,
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


class SmallStreamConfig(Config):
    """Configuration for the small_stream_recycle asset

    Example: change this to the sheetname pointing to the current calendar year
    """

    year_to_ingest: int = 2024


class WasteNewbatchData(pa.DataFrameModel):
    """Validate the output data schema of newbatch waste asset"""

    key: Series[str] = pa.Field(alias="Key", description="Waste collection building id")
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
    download_links = dhub.search_files_from_project(project_id, "historical_waste_recycle")
    if len(download_links) == 0:
        logger.error("No download links found!")
        return pd.DataFrame()
    # Load the data
    df = pd.read_csv(download_links[0])
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
    download_links = dhub.search_files_from_project(project_id, "waste_tonnage_newbatch")
    if len(download_links) == 0:
        logger.error("No download links found!")
        return empty_dataframe_from_model(WasteNewbatchData)
    # Load the data
    workbook = pd.ExcelFile(download_links[0], engine="openpyxl")
    tonnage = pd.read_excel(workbook, sheet_name=0)
    cols = [
        "Key",
        "Customer Name",
        "Service Street",
        "Service Date",
        "Material",
        "Tons",
    ]
    df_out = tonnage[cols]
    df_out.dropna(inplace=True)
    df_out["Material"] = df_out["Material"].str.title()
    return df_out


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def small_stream_recycle(config: SmallStreamConfig, dhub: ResourceParam[DataHubResource]):
    """This asset ingest the hard-to-recycle data from the Data Hub"""
    project_id = dhub.get_project_id("Material Matters")
    logger.info(f"Found project id: {project_id}!")
    download_links = dhub.search_files_from_project(project_id, "MIT_small_stream_waste")
    if len(download_links) == 0:
        logger.error("No download links found!")
        return pd.DataFrame()
    # Load the data
    target_year = config.year_to_ingest
    logger.info(f"Loading small stream data from year {target_year}")
    workbook = pd.ExcelFile(download_links[0], engine="openpyxl")
    df = pd.read_excel(
        workbook,
        usecols="B:M",
        skiprows=13,
        nrows=1,
        sheet_name=f"{target_year} small stream recycling",
        header=None,
    )
    df_out = df.T
    df_out.columns = ["tons"]
    df_out["service_date"] = pd.date_range(start=f"{target_year}-01-01", periods=len(df_out), freq="M")
    df_out["material"] = "Hard-to-Recycle Materials"
    df_out["customer_name"] = "Small Stream Facility"
    df_out["diverted"] = df_out["tons"]

    return df_out


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
    check_specs=[
        AssetCheckSpec(
            name="waste_emission_factor_check_unique",
            asset="waste_emission_factors_epa",
        )
    ],
)
def waste_emission_factors_epa(
    dhub: ResourceParam[DataHubResource],
):
    """This asset ingest the Commuting Emission Factors by waste material streams Table 9
    from EPA Emission Factors Hub."""
    project_id = dhub.get_project_id("Scope3 General")
    logger.info(f"Found project id: {project_id}!")
    download_links = dhub.search_files_from_project(project_id, "EPA_GHG_emission_factors_June_2024")
    if len(download_links) == 0:
        logger.error("No download links found!")
        return pd.DataFrame()
    # Load the emission factors from Table 9
    workbook = pd.ExcelFile(download_links[0], engine="openpyxl")
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
    yield Output(df_out)

    # check it
    yield AssetCheckResult(passed=df_out["material"].is_unique)


# Sync to datahub using the factory function
dhub_waste_sync = add_dhub_sync(
    asset_name="dhub_waste_sync",
    table_key=["final", "final_waste_recycle"],
    config={
        "filename": "final_waste_update",
        "project_name": "Material Matters",
        "description": "Processed waste data including historical, Casella and small stream",
        "title": "Processed Waste Data",
    },
)


dhub_scope3_waste_sync = add_dhub_sync(
    asset_name="dhub_scope3_waste_sync",
    table_key=["final", "final_waste_emission"],
    config={
        "filename": "final_scope3_waste",
        "project_name": "Scope3 Waste",
        "description": "Processed waste data and associated GHG emissions",
        "title": "Scope3 waste emission",
    },
)
