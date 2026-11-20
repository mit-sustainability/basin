from dagster import (
    asset,
    get_dagster_logger,
    ResourceParam,
)
from dagster_pandera import pandera_schema_to_dagster_type
import pandas as pd
import pandera as pa
from pandera.typing import Series

from orchestrator.assets.utils import empty_dataframe_from_model, add_dhub_sync
from orchestrator.resources.datahub import DataHubResource


logger = get_dagster_logger()


class ConstructionExpenseData(pa.DataFrameModel):
    """Validate the output data schema of construction cost asset"""

    new_construction: Series[int] = pa.Field(description="New Construction Cost in Million")
    renovation_and_renewal: Series[int] = pa.Field(description="Renovation Cost in Million")
    fiscal_year: Series[int] = pa.Field(lt=2025, description="Fiscal Year")


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
    dagster_type=pandera_schema_to_dagster_type(ConstructionExpenseData),
)
def construction_expense(dhub: ResourceParam[DataHubResource]):
    """This asset ingest the construction expense data from the Data Hub"""
    project_id = dhub.get_project_id("Scope3 Construction")
    logger.info(f"Found project id: {project_id}!")
    download_links = dhub.search_files_from_project(project_id, "Expense project FY16-FY24", tags=["construction"])
    if download_links is None:
        logger.info("No download links found!")
        return empty_dataframe_from_model(ConstructionExpenseData)
    # Load the Construction Expense excel file
    workbook = pd.ExcelFile(download_links[0], engine="openpyxl")
    df_expense = pd.read_excel(
        workbook,
        usecols="E:N",  # need to adjust load range for column and rows
        skiprows=7,
        nrows=2,
    )
    # Prepare for output
    df_out = df_expense.T.reset_index(drop=True)
    df_out.columns = ["new_construction", "renovation_and_renewal"]
    df_out["fiscal_year"] = range(2015, 2025)
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
    download_links = dhub.search_files_from_project(project_id, "DOFOpsCostsSummary_mitos")
    if len(download_links) == 0:
        logger.info("No download links found!")
        return pd.DataFrame()
    workbook = pd.ExcelFile(download_links[0], engine="openpyxl")
    # Load the DoF related maintenance cost
    dof1 = pd.read_excel(
        workbook,
        usecols="B:G",
        sheet_name="WO_within_DOF",
        skiprows=29,
        nrows=1,
        header=None,
    )
    dof2 = pd.read_excel(
        workbook,
        usecols="B:G",
        sheet_name="Sales_WO",
        skiprows=44,
        nrows=1,
        header=None,
    )
    dof3 = pd.read_excel(
        workbook,
        usecols="B:G",
        sheet_name="Non_WO",
        skiprows=72,
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
    df_out["fiscal_year"] = range(2019, 2025)
    return df_out


# Sync to datahub using the factory function
dhub_construction_cost = add_dhub_sync(
    asset_name="dhub_construction_cost",
    table_key=["final", "construction_expense_emission"],
    config={
        "filename": "final_construction_cost.csv",
        "project_name": "Scope3 Construction",
        "description": "Construction data with expense type and GHG emission",
        "title": "Processed Construction Cost data",
    },
)
