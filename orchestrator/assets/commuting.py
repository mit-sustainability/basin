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

from orchestrator.assets.utils import empty_dataframe_from_model, add_dhub_sync
from orchestrator.resources.datahub import DataHubResource

logger = get_dagster_logger()


class CommutingSurvey(pa.SchemaModel):
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
def commuting_emission_factors_EPA(dhub: ResourceParam[DataHubResource]):
    """This asset ingest the Commuting Emission Factors by vehicle
    type from EPA Emission Factors Hub."""
    project_id = dhub.get_project_id("Scope3 General")
    logger.info(f"Found project id: {project_id}!")
    download_links = dhub.search_files_from_project(project_id, "EPA_GHG_emission_factors_2024")
    if len(download_links) == 0:
        logger.error("No download links found!")
        return pd.DataFrame()
    # Load the data
    workbook = pd.ExcelFile(download_links[0], engine="openpyxl")
    df = pd.read_excel(
        workbook,
        usecols="C:G",
        skiprows=492,
        nrows=12,
    )
    # Clean up column names
    name_map = {
        "Vehicle Type": "vehicle_type",
        "CO2 Factor \n(kg CO2 / unit)": "CO2_kg",
        "CH4 Factor \n(g CH4 / unit)": "CH4_g",
        "N2O Factor \n(g N2O / unit)": "N2O_g",
        "Units": "units",
    }
    df_out = df.rename(columns=name_map)

    # Get GWP to calculate CO2-eq
    gwp = pd.read_excel(
        workbook,
        usecols="C:E",
        skiprows=512,
        nrows=3,
    )
    gwp_100 = gwp["100-Year GWP"].to_list()
    gwp_map = dict(zip(["CO2_kg", "CH4_g", "N2O_g"], gwp_100))
    df_out["CO2eq_kg"] = (
        df_out["CO2_kg"] + df_out["CH4_g"] * gwp_map["CH4_g"] / 1000 + df_out["N2O_g"] * gwp_map["N2O_g"] / 1000
    )
    df_out["last_update"] = datetime.now()

    return df_out


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def commuting_survey_2018(dhub: ResourceParam[DataHubResource]):
    """This asset ingests the Commuting Survey 2018 data conducted by IR."""
    project_id = dhub.get_project_id("Scope3 Commuting")
    logger.info(f"Found project id: {project_id}!")
    download_links = dhub.search_files_from_project(project_id, "commuting_survey_2018")
    if len(download_links) == 0:
        logger.error("No download links found!")
        return pd.DataFrame()
    # Load the data
    workbook = pd.ExcelFile(download_links[0], engine="openpyxl")
    df = pd.read_excel(
        workbook,
        usecols="A:P",
        sheet_name="Survey Data",
        nrows=25,
    )
    # Clean up column names
    name_map = {
        "Unnamed: 0": "commute_time",
        "Drive alone\nthe entire way": "drive_alone",
        "Drive alone, \nthen take public transportation": "drive_alone_and_public_transport",
        "Walk,\nthen take public transportation": "walk_and_public_transport",
        "Share ride/dropped off,\nthen take public transportation": "drop_off_and_public_transport",
        "Bicycle\nand take public transportation": "bike_and_public_transport",
        "Ride in a private car\nwith 1-4 commuters": "carpooled(2-6)",
        "Ride in a vanpool (5 or more commuters)\nor private shuttle (e.g. TechShuttle, SafeRide)": "vanpooled(7+)",
        "Dropped off at work": "dropped_off",
        "Take a taxi\nor ride service (e.g., Uber, Lyft)": "taxi_and_ride_service",
        "Bicycle": "bicycled",
        "Walk": "walked",
        "Work at home\nor other remote location": "Remote",
        "Other": "other",
        "N/A": "unknown",
        "[Did not answer question]": "no_answer",
    }
    df_out = df.rename(columns=name_map)
    df_out["commute_time_average_hours"] = [round(2.5 + 5 * i + 0.2) / 60 for i in range(25)]

    return df_out.fillna(0)


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def commuting_survey_2021(dhub: ResourceParam[DataHubResource]):
    """This asset ingests the Commuting Survey 2021 data conducted by IR."""
    project_id = dhub.get_project_id("Scope3 Commuting")
    logger.info(f"Found project id: {project_id}!")
    download_links = dhub.search_files_from_project(project_id, "commuting_survey_2021")
    if len(download_links) == 0:
        logger.error("No download links found!")
        return pd.DataFrame()
    # Load the data
    workbook = pd.ExcelFile(download_links[0], engine="openpyxl")
    df = pd.read_excel(
        workbook,
        usecols="A:K",
        sheet_name="final_calculation",
        skiprows=1,
        nrows=10,
    )
    # Clean up column names
    df = df.rename(columns={"Unnamed: 0": "role"}).fillna(0)
    df.rename(columns=str.lower, inplace=True)

    travel_time = pd.read_excel(
        workbook,
        usecols="M:N",
        sheet_name="final_calculation",
        skiprows=1,
        nrows=10,
    )
    modes = [
        "drove alone",
        "carpooled(2-6)",
        "vanpooled(7+)",
        "shuttle",
        "public transportation",
    ]
    # Accumulative Travel time in minutes broken down by roles
    df_out = df[modes].apply(lambda x: x * travel_time["time(min)"])
    return df_out.fillna(0)


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def commuting_survey_2023(dhub: ResourceParam[DataHubResource]):
    """This asset ingests the Commuting Survey 2023 data conducted by IR."""
    project_id = dhub.get_project_id("Scope3 Commuting")
    logger.info(f"Found project id: {project_id}!")
    download_links = dhub.search_files_from_project(project_id, "commuting_survey_2023")
    if len(download_links) == 0:
        logger.error("No download links found!")
        return pd.DataFrame()
    # Load the data
    workbook = pd.ExcelFile(download_links[0], engine="openpyxl")
    df = pd.read_excel(
        workbook,
        usecols="A:J",
        sheet_name="time_by_mode_to_count",
        nrows=25,
    )
    # Clean up column names
    name_map = {
        "Unnamed: 0": "commute_time",
        "Drive alone": "drove alone",
        "Ride in a carpool (2-6 commuters), including being dropped off at MIT": "carpooled(2-6)",
        "Ride in a vanpool (7+ commuters)": "vanpooled(7+)",
        "Take public transportation": "public transportation",
        "Ride in a shuttle": "shuttle",
    }
    df_out = df.rename(columns=name_map)
    modes = [
        "drove alone",
        "carpooled(2-6)",
        "vanpooled(7+)",
        "shuttle",
        "public transportation",
    ]
    df_out["commute_time_average_hours"] = [round(2.5 + 5 * i + 0.2) / 60 for i in range(25)]
    out_cols = ["commute_time_average_hours"] + modes
    return df_out[out_cols].fillna(0)


# Sync to datahub using the factory function
dhub_commute_sync = add_dhub_sync(
    asset_name="dhub_commute_sync",
    table_key=["final", "commuting_emission"],
    config={
        "filename": "scope3_commuting_emission",
        "project_name": "Scope3 Commuting",
        "description": "Estimate of CO2eq emissions from commuting survey 2018, 2021, 2023",
        "title": "scope3_commuting_emission",
    },
)
