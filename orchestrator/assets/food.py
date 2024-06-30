import json
from typing import List

import asyncio
from dagster import (
    asset,
    AssetIn,
    Config,
    get_dagster_logger,
    ResourceParam,
)
from dagster_pandera import pandera_schema_to_dagster_type
import pandas as pd
import pandera as pa
from pandera.typing import Series, DateTime, Float

from orchestrator.constants import food_categorizer
from orchestrator.assets.utils import (
    add_dhub_sync,
    empty_dataframe_from_model,
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
    "dist_item_umo",
    "reporting_qty",
    "mfr_item_reporting_uom",
    "case_qty",
    "spend",
    "wri_category",
]

# class FoodOrderSchema(pa.SchemaModel):


class FoodOrderConfig(Config):
    """Configuration for the food order asset

    Example: change this from the asset launchpad to specify the files to load to the data platform.
    """

    files_to_download: List[str] = [
        "MIT_v2_JUL2021_JUN2022",
        "MIT_v2_JUL2022_JUN2023",
    ]


@asset(
    io_manager_key="postgres_replace",  # only use this when loading from scratch, else "postgres_append"
    compute_kind="python",
    group_name="raw",
    # dagster_type=pandera_schema_to_dagster_type(InvoiceSchema),
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
            return  # empty_dataframe_from_model(InvoiceSchema)
        # Load the data
        logger.info(f"Downloading {file}...")
        df = pd.read_excel(download_links[0])
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
    }
    combined["customer_name"] = combined["customer_name"].map(dinning_hall_mapping)
    return combined[sel_cols]


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def food_emission_factor(dhub: ResourceParam[DataHubResource]):
    """This asset ingest the food emission factors
    from the Data Hub"""
    project_id = dhub.get_project_id("Scope3 Food")
    logger.info(f"Found project id: {project_id}!")

    download_links = dhub.search_files_from_project(project_id, "food_emission_factors_SIMAP.xlsx")
    if len(download_links) == 0:
        logger.error("No download links found!")
    # Load the data
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
    print(df["title"])
    inputs = [{"body": entry} for entry in df["title"].values]
    # Predict food category through API
    results = asyncio.run(fetch_all_async(food_categorizer, inputs))

    for item in results:
        item["body"] = json.loads(item["body"])

    bodies = [item["body"] for item in results]
    predict = pd.DataFrame(bodies)
    print(predict.head())
    df["simap_category"] = predict["Cat1"]
    out_cols = sel_cols.copy()
    out_cols.append("simap_category")
    return df[out_cols]


dhub_food_orders = add_dhub_sync(
    asset_name="dhub_food_order_categorize",
    table_key=["staging", "food_order_categorize"],
    config={
        "filename": "food_order_categorize.parquet.gz",
        "project_name": "Scope3 Food",
        "description": "Processed Food Order data with SIMAP categories",
        "title": "Processed Food Order Data with SIMAP categories",
        "ext": "parquet",
    },
)
