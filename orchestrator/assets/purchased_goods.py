import re
from typing import Optional, List
from dagster import (
    asset,
    Config,
    get_dagster_logger,
    ResourceParam,
)
from dagster_pandera import pandera_schema_to_dagster_type
import numpy as np
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


def parse_billing(row):
    """Parse the billing column to extract the cost object and total amount if
    multiple accounts are present in the same row."""
    if pd.isna(row["Billing"]):
        return [{"Total": np.nan, "cost_object": np.nan}]

    parts = re.split(r" ; ", row["Billing"]) if ";" in row["Billing"] else [row["Billing"]]
    results = []

    for part in parts:
        amount = re.search(r"(\d+\.\d+)(?= USD)", part)
        cost_obj = re.search(r"CostObj: (\d{7})", part)  # First try to find "CostObj:" followed by 7 digits
        if not cost_obj:
            # Fallback to match the later 7-digit number after '-'
            cost_obj = re.search(r"(?<=-)\d{7}\b", part)
        new_row = row.to_dict()
        if amount and cost_obj:
            new_row["Total"] = amount.group(1)
            new_row["cost_object"] = cost_obj.group(1)
        elif cost_obj:
            new_row["cost_object"] = cost_obj.group(0)
        results.append(new_row)

    return results


class InvoiceConfig(Config):
    """Configuration for the invoice asset"""

    files_to_download: List[str] = [
        "PurchasedGoods_Invoice_FY2019",
        "PurchasedGoods_Invoice_FY2020",
    ]


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def purchased_goods_invoice(config: InvoiceConfig, dhub: ResourceParam[DataHubResource]):
    """This asset ingest the specified pruchased goods invoice files
    from the Data Hub"""
    project_id = dhub.get_project_id("Scope3 Purchased Goods")
    logger.info(f"Found project id: {project_id}!")
    dfs = []
    for file in config.files_to_download:
        download_links = dhub.search_files_from_project(project_id, file, tags=["invoice"])
        if len(download_links) == 0:
            logger.error("No download links found!")
        # Load the data
        logger.info(f"Downloading {file}...")
        df = pd.read_csv(download_links[0])
        logger.info(f"Loading {len(df)} entries from {file}")
        df.dropna(subset=["Billing"], inplace=True)
        expanded = df.apply(parse_billing, axis=1).explode().reset_index(drop=True)
        df_fy = pd.DataFrame(expanded.tolist())
        df_fy["Total"] = pd.to_numeric(df_fy["Total"])
        dfs.append(df_fy)
    combined = pd.concat(dfs, axis=0)
    combined["Invoice Date"] = pd.to_datetime(combined["Invoice Date"])
    combined["PO Order Date"] = pd.to_datetime(combined["PO Order Date"])
    combined.sort_values("Invoice Date", inplace=True)
    combined.columns = [normalize_column_name(col) for col in combined.columns]
    return combined


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def purchased_goods_mapping(dhub: ResourceParam[DataHubResource]):
    """This asset ingest the purchase goods category mapping table
    from the Data Hub"""
    project_id = dhub.get_project_id("Scope3 Purchased Goods")
    logger.info(f"Found project id: {project_id}!")

    download_links = dhub.search_files_from_project(project_id, "purchased_goods_eeio_mapping_2024")
    if len(download_links) == 0:
        logger.error("No download links found!")
    # Load the data
    df = pd.read_csv(download_links[0])
    cols_mapping = {
        "Level 1 Categories": "level_1",
        "Level 2 Categories": "level_2",
        "Level 3 Categories": "level_3",
        "Name": "name",
        "Code": "code",
    }
    return df.rename(columns=cols_mapping)
