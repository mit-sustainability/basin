from datetime import datetime
from dagster import (
    asset,
    get_dagster_logger,
    Output,
    ResourceParam,
)
from dagster_pandera import pandera_schema_to_dagster_type
import pandas as pd
import pandera as pa
from pandera.typing import Series, DateTime

from orchestrator.assets.utils import (
    normalize_column_name,
    empty_dataframe_from_model,
)
from orchestrator.resources.datahub import DataHubResource


logger = get_dagster_logger()


class TreeSchema(pa.DataFrameModel):
    """Validate the output data schema of travel spending asset"""

    tree_id: Series[int] = pa.Field(description="Tree ID")
    common_name: Series[str] = pa.Field(description="Expense Amount")
    longitude: Series[float] = pa.Field(description="Transaction date")
    latitude: Series[float] = pa.Field(description="Energy use date")
    age_class: Series[str] = pa.Field(
        isin=["Mature", "Semi-mature", "Over-mature", "New planting", "Young"],
        description="Tree age",
    )
    condition_class: Series[str] = pa.Field(isin=["Good", "Poor", "Fair", "Dead"], description="Tree condition")
    canopy_radius: Series[float] = pa.Field(description="Tree Canopy Radius")
    height_class: Series[str] = pa.Field(isin=["Large", "Medium", "Small"], description="Tree height class")
    last_update: Series[DateTime] = pa.Field(description="Date of last update")


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
    dagster_type=pandera_schema_to_dagster_type(TreeSchema),
)
def campus_tree_catalog(dhub: ResourceParam[DataHubResource]):
    """This asset ingest tree Inventory from Data Hub"""
    project_id = dhub.get_project_id("Grounds")
    logger.info(f"Found project id: {project_id}!")
    download_links = dhub.search_files_from_project(project_id, "tree_inventory_April2025")
    if len(download_links) == 0:
        logger.error("No download links found!")
        return empty_dataframe_from_model(TreeSchema)
    # Load the tree catalog
    df = pd.read_csv(download_links[0])

    # Load the category to emission factor code mapping
    cols = [
        "Tree ID",
        "Common Name",
        "Longitude",
        "Latitude",
        "Age Class",
        "Condition Class",
        "Canopy Radius",
        "Height Class",
    ]
    output_df = df[cols].copy()
    output_df.columns = [normalize_column_name(col) for col in output_df.columns]
    output_df["last_update"] = datetime.now()
    unique_trees = len(output_df["tree_id"].unique())
    metadata = {
        "unique_tree_counts": unique_trees,
    }

    return Output(value=output_df, metadata=metadata)
