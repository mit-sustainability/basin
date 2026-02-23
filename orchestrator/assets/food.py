import json
from typing import List

import asyncio
from dagster import (
    asset,
    AssetIn,
    Config,
    Failure,
    Output,
    get_dagster_logger,
    MetadataValue,
    ResourceParam,
)
import pandas as pd

from orchestrator.constants import food_cat_endpoint
from orchestrator.assets.utils import (
    add_dhub_sync,
    normalize_column_name,
    fetch_all_async,
)
from orchestrator.resources.datahub import DataHubResource

logger = get_dagster_logger()

sel_cols = [
    "customer_name",
    "contract_month",
    "mfr_item_parent_category_name",
    "mfr_item_category_name",
    "mfr_item_code",
    "mfr_item_description",
    "distributor_name",
    "dist_item_description",
    "din",
    "dist_item_manufacturer_item_id",
    "lbs",
    "gal",
    "dist_qty",
    "dist_item_uom",
    "reporting_qty",
    "mfr_item_reporting_uom",
    "case_qty",
    "spend",
    "wri_category",
]


class FoodOrderConfig(Config):
    """Configuration for the food order asset

    Example: change this from the asset launchpad to specify the files to load to the data platform.
    """

    files_to_download: List[str] = ["BA_food_invoice_items_FY21_FY25"]


@asset(
    io_manager_key="postgres_replace",  # only use this when loading from scratch, else "postgres_append"
    compute_kind="python",
    group_name="raw",
)
def ba_food_orders(config: FoodOrderConfig, dhub: ResourceParam[DataHubResource]):
    """This asset ingest the food order data Bon Appetit provided
    from the Data Hub"""
    project_id = dhub.get_project_id("Scope3 Food")
    logger.info(f"Found project id: {project_id}!")
    dfs = []
    for file in config.files_to_download:
        download_links = dhub.search_files_from_project(project_id, file)
        if len(download_links) == 0:
            logger.info("No download links found!")
            raise Failure("No download links found!")
        # Load the data
        url = download_links[0]
        try:
            df = pd.read_csv(url)
        except Exception:
            df = pd.read_excel(url)
        logger.info(f"Loading {len(df)} entries from {file}")
        dfs.append(df)
    combined = pd.concat(dfs, axis=0)
    combined.columns = [normalize_column_name(col) for col in combined.columns]
    combined["contract_month"] = pd.to_datetime(combined["contract_month"])
    combined.sort_values("contract_month", inplace=True)
    # Map dinning hall names
    dinning_hall_mapping = {
        "MIT Baker  15912": "Baker",
        "MIT McCormick 15915": "McCormick",
        "MIT Simmons 15914": "Simmons",
        "Massachusetts Institute of Technology New Vassar 55692 Bon Appetit": "New Vessar",
        "MIT Next House 15913": "Next House",
        "MIT Maseeh Hall": "Maseeh Hall",
        "MIT - Forbes Family CafÃ©": "Forbes Family",
        "MIT - Forbes Family Café": "Forbes Family",
        "MIT - Bosworth's Cafe": "Bosworth's",
        "MIT Catering": "Catering",
    }
    combined["customer_name"] = combined["customer_name"].replace(dinning_hall_mapping)
    metadata = {
        "total_entries": len(combined),
        "last_updated": MetadataValue.text(combined["contract_month"].max().strftime("%Y-%m-%d")),
    }
    return Output(value=combined[sel_cols], metadata=metadata)


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


@asset(
    compute_kind="python",
    group_name="staging",
    key_prefix="staging",
    io_manager_key="postgres_replace",
    ins={
        "df": AssetIn(key=["ba_food_orders"], input_manager_key="postgres_replace"),
    },
)
def food_order_categorize(df: pd.DataFrame):
    """This asset takes in the food order data from Bon Appetit
    and use Categorizer API to add SIMAP categories"""

    # Concatenate Title for Categorization
    filled = df[
        [
            "mfr_item_parent_category_name",
            "mfr_item_category_name",
            "mfr_item_description",
            "dist_item_description",
            "wri_category",
        ]
    ].fillna(value="")
    df["title"] = filled.agg(" ".join, axis=1)
    unique_titles = pd.unique(df["title"])
    inputs = [{"body": entry} for entry in unique_titles]

    # Predict food category through API with bounded concurrency to avoid Lambda/API timeouts.
    results = asyncio.run(
        fetch_all_async(
            food_cat_endpoint,
            inputs,
            max_concurrency=64,
            request_timeout_seconds=30,
            connect_timeout_seconds=5,
            max_retries=2,
        )
    )

    for item in results:
        item["body"] = json.loads(item["body"])

    bodies = [item["body"] for item in results]
    predict = pd.DataFrame(bodies)
    category_by_title = dict(zip(unique_titles, predict["Cat1"]))
    df["simap_category"] = df["title"].map(category_by_title)

    # Select output columns
    out_cols = sel_cols.copy()
    out_cols.append("simap_category")

    metadata = {
        "unique_titles": len(unique_titles),
    }
    return Output(value=df[out_cols], metadata=metadata)


# Sync back to DataHub
dhub_food_orders = add_dhub_sync(
    asset_name="dhub_food_order_categorize",
    table_key=["staging", "stg_food_order"],
    config={
        "filename": "food_order_ghg.parquet.gz",
        "project_name": "Scope3 Food",
        "description": "Processed Food Order data with GHG emissions",
        "title": "Food Order GHG emissions",
        "ext": "parquet",
    },
)
