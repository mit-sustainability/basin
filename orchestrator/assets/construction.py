from dagster import (
    AssetExecutionContext,
    asset,
    AssetIn,
    get_dagster_logger,
    ResourceParam,
)
from dagster_aws.s3 import S3Resource
from dagster_pandera import pandera_schema_to_dagster_type
import pandas as pd

from resources.postgres_io_manager import PostgreConnResources
from resources.datahub import DataHubResource
from resources.mit_warehouse import MITWHRSResource


logger = get_dagster_logger()


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def emission_factor_USEEIOv2(dhub: ResourceParam[DataHubResource]):
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
        sheet_name="Summary",  # Only load columns B, C, D
        skiprows=32,  # Skip the first 2 rows
        nrows=1,
        header=None,
    )
    dof2 = pd.read_excel(
        workbook,
        usecols="B:F",
        sheet_name="Summary",  # Only load columns B, C, D
        skiprows=83,  # Skip the first 2 rows
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
