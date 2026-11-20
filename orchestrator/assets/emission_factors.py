"""Emission factor and CPI assets.

Legacy single-release factor assets (moved here unchanged from construction.py,
commuting.py, waste.py, food.py and business_travel.py) live alongside the
versioned emission factor archive. See specs/009-emission-factor-archive/.
"""

from datetime import datetime

import cpi
from dagster import (
    asset,
    AssetCheckResult,
    AssetCheckSpec,
    get_dagster_logger,
    Output,
    ResourceParam,
)
import numpy as np
import pandas as pd

from orchestrator.assets.utils import normalize_column_name
from orchestrator.resources.postgres_io_manager import PostgreConnResources
from orchestrator.resources.datahub import DataHubResource


logger = get_dagster_logger()


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def annual_cpi_index():
    """Get annual CPI index from python cpi library"""
    DEFAULT_SERIES_ID = cpi.defaults.DEFAULT_SERIES_ID
    logger.info("Ectract the annual CPI data using the python cpi library ")
    cpi_df = cpi.series.get_by_id(DEFAULT_SERIES_ID).to_dataframe()
    cpi_sub = cpi_df[cpi_df["period_code"] == "M13"][["year", "value", "series_id"]].sort_values("year")
    cpi_sub["last_update"] = datetime.now()

    return cpi_sub


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def emission_factor_useeio_v2(dhub: ResourceParam[DataHubResource]):
    """This asset ingest the USEEIOv2.0.1 emission factor data from the Data Hub"""
    project_id = dhub.get_project_id("Scope3 General")
    logger.info(f"Found project id: {project_id}!")
    download_links = dhub.search_files_from_project(project_id, "USEEIOv2.0.1")
    if len(download_links) == 0:
        logger.error("No download links found!")
        return pd.DataFrame()
    # Load the emission factor
    workbook = pd.ExcelFile(download_links[0], engine="openpyxl")
    coef = pd.read_excel(workbook, sheet_name="N", index_col=0)
    ef = coef.loc["Greenhouse Gases"]
    ef_df = ef.reset_index()
    ef_df.columns = ["ID", "emission_factor"]

    # Load the category to emission factor code mapping
    df_code = pd.read_excel(workbook, sheet_name="commodities_meta", index_col=0)
    merged = pd.merge(ef_df, df_code, on="ID", how="inner")
    columns = ["ID", "emission_factor", "Name", "Code", "Category", "Description"]
    return merged[columns]


@asset(
    deps=[emission_factor_useeio_v2],
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def emission_factor_naics(dhub: ResourceParam[DataHubResource], pg_engine: PostgreConnResources):
    """Ingest and combine NAICS and USEEIO emission factors for Scope3 purchased goods"""
    project_id = dhub.get_project_id("Scope3 General")
    download_links = dhub.search_files_from_project(project_id, "SupplyChainGHGEmissionFactors_v1.2_NAICS_CO2e_USD2021")
    if len(download_links) == 0:
        logger.info("No download links found!")
        return pd.DataFrame()
    # Load the NAICS_v1.2 emission factors related maintenance cost
    df_naics = pd.read_csv(download_links[0])
    df_naics.drop(columns=["GHG", "Unit"], inplace=True)

    # Load USEEIO emission factors
    engine = pg_engine.create_engine()
    df_eeio = pd.read_sql_query("SELECT * FROM raw.emission_factor_useeio_v2", engine)

    # Expand USEEIO code from NAICS table
    ref1 = df_naics.copy()
    ref1["Reference USEEIO Code"] = (
        ref1["Reference USEEIO Code"].str.split(",").map(lambda elements: [e.strip() for e in elements])
    )
    ref1 = ref1.explode("Reference USEEIO Code", ignore_index=True)

    explode = ref1.copy().iloc[:, 2:6]
    explode.drop_duplicates(inplace=True, ignore_index=True)
    explode.sort_values(by=["Reference USEEIO Code"], inplace=True, ignore_index=True)
    explode["freq"] = explode.groupby("Reference USEEIO Code")["Reference USEEIO Code"].transform("count")
    explode["NAICS"] = np.nan
    for f in explode.query("freq > 1").index:
        naics = ref1.loc[
            (ref1["Reference USEEIO Code"] == explode.iloc[f, 3])
            & (ref1["Supply Chain Emission Factors with Margins"] == explode.iloc[f, 2])
        ]["2017 NAICS Title"]
        explode.iloc[f, 5] = ",".join(map(str, naics))

    # Drop the naics categories that may be included in others(Land Subdvision),
    # and average the emission factor of the rest naics categories of the same eeio code
    ref2 = explode.copy()
    print(len(ref2))
    ref2 = ref2[ref2["NAICS"] != ref2.iloc[23, 5]].reset_index(drop=True)
    ref2 = ref2[ref2["NAICS"] != "Land Subdivision"].reset_index(drop=True)
    ref2.drop(columns=["freq", "NAICS"], inplace=True)  # drop non-numeric columns
    ref2 = ref2.groupby("Reference USEEIO Code").mean().reset_index()
    factor_eeio_new = df_eeio.copy()
    factor_eeio_new = factor_eeio_new.merge(ref2, left_on="Code", right_on="Reference USEEIO Code", how="left")
    factor_eeio_new.drop(columns=["Reference USEEIO Code"], inplace=True)
    return factor_eeio_new


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


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def food_emission_factor(dhub: ResourceParam[DataHubResource]):
    """This asset ingests the food emission factors
    from the Data Hub"""
    project_id = dhub.get_project_id("Scope3 Food")
    logger.info(f"Found project id: {project_id}!")

    download_links = dhub.search_files_from_project(project_id, "food_emission_factors_SIMAP.xlsx")
    if len(download_links) == 0:
        logger.error("No download links found!")
    df = pd.read_excel(download_links[0], sheet_name="Final Table")
    name_mapping = {
        "Food Category": "simap_category",
        "Spend-Based Emissions Factor (kg CO2e/2021 USD, purchaser price)": "spend-based emissions factor (kg CO2e/2021 USD)",
        "Weight-Based Emissions Factors (kg CO2e/kg food)": "weight-based emissions factor (kg CO2e/kg food)",
    }
    return df.rename(columns=name_mapping)
