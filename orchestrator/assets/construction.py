from dagster import (
    AssetExecutionContext,
    asset,
    AssetIn,
    get_dagster_logger,
    ResourceParam,
)
from dagster_pandera import pandera_schema_to_dagster_type
import numpy as np
import pandas as pd
from sqlalchemy import text

from resources.postgres_io_manager import PostgreConnResources
from resources.datahub import DataHubResource
from resources.mit_warehouse import MITWHRSResource


logger = get_dagster_logger()


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def emission_factor_useeio_v2(dhub: ResourceParam[DataHubResource]):
    """This asset ingest the USEEIOv2.0.1 emission factor data from the Data Hub"""
    project_id = dhub.get_project_id("Scope3 General")
    logger.info(f"Found project id: {project_id}!")
    download_links = dhub.search_files_from_project(
        project_id, "USEEIOv2.0.1"
    )  # some special charater will fail the search request
    if download_links is None:
        logger.info("No download links found!")
        return pd.DataFrame()
    # Load the emission factor
    workbook = pd.ExcelFile(download_links[0], engine="openpyxl")
    coef = pd.read_excel(workbook, sheet_name="N", index_col=0)
    ef = coef.loc["Greenhouse Gases"]
    ef_df = ef.reset_index()
    ef_df.columns = ["ID", "emission_factor"]

    # Load the category to emission facto code mapping
    df_code = pd.read_excel(workbook, sheet_name="commodities_meta", index_col=0)
    merged = pd.merge(ef_df, df_code, on="ID", how="inner")
    columns = ["ID", "emission_factor", "Name", "Code", "Category", "Description"]
    return merged[columns]


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def construction_expense(dhub: ResourceParam[DataHubResource]):
    """This asset ingest the construction expense data from the Data Hub"""
    project_id = dhub.get_project_id("Scope3 Construction")
    logger.info(f"Found project id: {project_id}!")
    download_links = dhub.search_files_from_project(project_id, "Expense project FY16-FY23")
    if download_links is None:
        logger.info("No download links found!")
        return pd.DataFrame()
    # Load the Construction Expense excel file
    workbook = pd.ExcelFile(download_links[0], engine="openpyxl")
    df_expense = pd.read_excel(
        workbook,
        usecols="G:O",  # Only load columns B, C, D
        skiprows=8,  # Skip the first 2 rows
        nrows=2,
    )
    # Prepare for output
    df_out = df_expense.T.reset_index(drop=True)
    df_out.columns = ["New Construction", "Renovations and Renewal*"]
    df_out["fiscal_year"] = range(2015, 2024)
    return df_out


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def dof_maintenance_cost(dhub: ResourceParam[DataHubResource]):
    """This asset ingest the maintenance cost data from the Data Hub"""
    project_id = dhub.get_project_id("Scope3 Construction")
    logger.info(f"Found project id: {project_id}!")
    download_links = dhub.search_files_from_project(project_id, "MITOSRequest_DOFOpsCostsv2")
    if download_links is None:
        logger.info("No download links found!")
        return pd.DataFrame()
    workbook = pd.ExcelFile(download_links[0], engine="openpyxl")
    # Load the DoF related maintenance cost
    dof1 = pd.read_excel(
        workbook,
        usecols="B:F",
        sheet_name="Summary",  # Only load columns B:F
        skiprows=33,
        nrows=1,
        header=None,
    )
    dof2 = pd.read_excel(
        workbook,
        usecols="B:F",
        sheet_name="Summary",  # Only load columns B:F
        skiprows=83,
        nrows=1,
        header=None,
    )
    dof3 = pd.read_excel(
        workbook,
        usecols="P:T",
        sheet_name="Summary",  # Only load columns B, C, D
        skiprows=89,  # Skip the first 2 rows
        nrows=1,
        header=None,
    )

    # Prepare for output
    df_out = pd.concat(
        [dff.iloc[0].reset_index(drop=True) for dff in [dof1, dof2, dof3]],
        axis=1,
        keys=[
            "Work Orders Within DOF",
            "Sales Work Orders",
            "DOF Ops Costs Outside of Wos",
        ],
    )
    df_out["fiscal_year"] = range(2019, 2024)
    return df_out


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def emission_factor_naics(dhub: ResourceParam[DataHubResource], pg_engine: PostgreConnResources):
    """Ingest and combine NAICS and USEEIO emission factors for Scope3 purchased goods"""
    project_id = dhub.get_project_id("Scope3 General")
    download_links = dhub.search_files_from_project(project_id, "SupplyChainGHGEmissionFactors_v1.2_NAICS_CO2e_USD2021")
    if download_links is None:
        logger.info("No download links found!")
        return pd.DataFrame()
    # Load the NAICS_v1.2 emission factors related maintenance cost
    df_naics = pd.read_csv(download_links[0])
    df_naics.drop(columns=["GHG", "Unit"], inplace=True)

    # Load EEIO emission factors
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
    ref2 = ref2[ref2["NAICS"] != ref2.iloc[23, 5]].reset_index(drop=True)
    ref2 = ref2[ref2["NAICS"] != "Land Subdivision"].reset_index(drop=True)
    ref2.drop(columns=["freq", "NAICS"], inplace=True)  # drop non-numeric columns
    ref2 = ref2.groupby("Reference USEEIO Code").mean().reset_index()
    # get updated supply chain GHG emisison factor by eeio categories
    factor_eeio_new = df_eeio.copy()
    factor_eeio_new = factor_eeio_new.merge(ref2, left_on="Code", right_on="Reference USEEIO Code", how="left")
    factor_eeio_new.drop(columns=["Reference USEEIO Code"], inplace=True)
    return factor_eeio_new
