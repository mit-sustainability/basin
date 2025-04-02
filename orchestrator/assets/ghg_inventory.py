from datetime import datetime
from dagster import asset, Output, get_dagster_logger, MetadataValue, ResourceParam
from dagster_aws.s3 import S3Resource
from dagster_pandera import pandera_schema_to_dagster_type
import pandas as pd
import pandera as pa
from pandera.typing import Series, DateTime

from orchestrator.assets.utils import (
    empty_dataframe_from_model,
    add_dhub_sync,
    normalize_column_name,
)
from orchestrator.resources.datahub import DataHubResource
from orchestrator.resources.postgres_io_manager import PostgreConnResources


logger = get_dagster_logger()


class EnergySchema(pa.DataFrameModel):
    """Validate the output data schema of travel spending asset"""

    building_number: Series[str] = pa.Field(description="MIT Building Number")
    gl_account_key: Series[str] = pa.Field(description="Expense Amount")
    start_date: Series[object] = pa.Field(description="Transaction date")
    start_date_use: Series[object] = pa.Field(description="Energy use date")
    billing_fy: Series[int] = pa.Field(description="Billing Fiscal Year")
    use_fy: Series[int] = pa.Field(description="Use Fiscal Year")
    ghg: Series[float] = pa.Field(description="GHG Emissions in metric tons")
    unit_of_measure: Series[str] = pa.Field(description="Unit of Measure")
    number_of_units: Series[float] = pa.Field(description="Number of energy units")
    building_group_level1: Series[str] = pa.Field(isin=["Non-CUP", "CUP"], description="Building Group category")
    level1_category: Series[str] = pa.Field(description="Level 1 category")
    level2_category: Series[str] = pa.Field(eq="Purchased", description="Level 2 category")
    level3_category: Series[str] = pa.Field(
        isin=["Gas", "Fuel Oil #2", "Electricity", "Fuel Oil #6"],
        description="Level 3 category",
    )
    last_update: Series[DateTime] = pa.Field(description="Date of last update")


EnergySchemaType = pandera_schema_to_dagster_type(EnergySchema)


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def ghg_manual_entries(s3: S3Resource) -> Output[pd.DataFrame]:
    """Load manually entried GHG emission from S3 bucket"""
    source_bucket = "mitos-landing-zone"
    # Get the data from S3
    s3_client = s3.get_client()
    obj = s3_client.get_object(Bucket=source_bucket, Key="all-scopes-sync/quickbase_data.csv")
    df = pd.read_csv(obj["Body"])

    metadata = {
        "total_entries": len(df),
    }
    sel_columns = ["category", "emission", "fiscal_year", "scope", "Date Modified"]
    df_out = df[sel_columns]
    df_out.rename({"Date Modified": "last_update"}, axis=1, inplace=True)

    return Output(value=df_out, metadata=metadata)


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
    dagster_type=EnergySchemaType,
)
def purchased_energy(em_connect: PostgreConnResources) -> Output[pd.DataFrame]:
    """Load purchased energy numbers from energy-cdr in energize-mit database"""
    engine = em_connect.create_engine()
    logger.info("Connect to energize_mit database to ingest Scope 1 and 2 emissions")
    query = """
            SELECT
                "BUILDING_NUMBER",
                "GL_ACCOUNT_KEY",
                "START_DATE",
                "START_DATE_USE",
                "BILLING_FY",
                "USE_FY",
                "GHG",
                "UNIT_OF_MEASURE",
                "NUMBER_OF_UNITS",
                "BUILDING_GROUP_LEVEL1",
                "LEVEL1_CATEGORY",
                "LEVEL2_CATEGORY",
                "LEVEL3_CATEGORY"
            FROM public.energy_cdr
            WHERE "BUILDING_GROUP_LEVEL1" IN ('CUP','Non-CUP')
            AND "LEVEL2_CATEGORY" = 'Purchased'
            AND "LEVEL3_CATEGORY" IN ('Electricity', 'Gas', 'Fuel Oil #2', 'Fuel Oil #6')
            """
    try:
        with engine.connect() as conn:
            df = pd.read_sql_query(query, conn)
        logger.info("Query executed successfully. Scope 1 and 2 data fetched from energize-mit.")
    except Exception as e:
        logger.error("An error occurred:", e)
        return empty_dataframe_from_model(EnergySchema)
    finally:
        engine.dispose()

    metadata = {
        "total_entries": len(df),
        "last_entry_date": MetadataValue.text(df["START_DATE"].max().strftime("%Y-%m-%d")),
    }
    df["last_update"] = datetime.now()
    # Convert FY columns to integer
    for col in ["BILLING_FY", "USE_FY"]:
        df[col] = df[col].astype("Int64")

    df.columns = [normalize_column_name(col) for col in df.columns]
    return Output(value=df, metadata=metadata)


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def ghg_categories(dhub: ResourceParam[DataHubResource]) -> Output[pd.DataFrame]:
    """Load GHG categories from datahub"""
    project_id = dhub.get_project_id("GHG_Inventory")
    logger.info(f"Found project id: {project_id}!")

    download_links = dhub.search_files_from_project(project_id, "ghg_protocol_categories")
    if len(download_links) == 0:
        logger.error("No download links found!")
    df = pd.read_csv(download_links[0])
    metadata = {
        "total_categories": len(df),
    }

    return Output(value=df, metadata=metadata)


# Sync processed table back to datahub
dhub_ghg_inventory = add_dhub_sync(
    asset_name="dhub_ghg_inventory",
    table_key=["staging", "stg_ghg_inventory"],
    config={
        "filename": "ghg_inventory.csv",
        "project_name": "GHG_Inventory",
        "description": "Aggregated GHG inventory emissions by categories",
        "title": "Aggregated GHG Inventory",
    },
)
