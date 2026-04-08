import os
import re
from typing import List, Literal
from dagster import (
    AssetExecutionContext,
    asset,
    Config,
    Failure,
    get_dagster_logger,
    ResourceParam,
    MaterializeResult,
)
from dagster_aws.pipes import PipesECSClient
from dagster_pandera import pandera_schema_to_dagster_type
import pandas as pd
import pandera as pa
from pandera.typing import Series, DateTime, Float
from sqlalchemy import text

from orchestrator.assets.utils import (
    add_dhub_sync,
    normalize_column_name,
)
from orchestrator.resources.datahub import DataHubResource
from orchestrator.resources.ecs import build_ecs_run_task_params
from orchestrator.resources.postgres_io_manager import connect_postgresql

logger = get_dagster_logger()

PURCHASED_GOODS_INVOICE_ECS_COMMAND = [
    "python",
    "-m",
    "orchestrator.pipes.purchased_goods_invoice",
]
PURCHASED_GOODS_INVOICE_TABLE = "purchased_goods_invoice"


class InvoiceSchema(pa.DataFrameModel):
    sap_invoice_number: Series[Float] = pa.Field(description="SAP Invoice Number", nullable=True)
    invoice_number: Series[str] = pa.Field(description="Invoice Number", nullable=True)
    invoice_date: Series[DateTime] = pa.Field(description="Date of Invoice")
    header_status: Series[str] = pa.Field(description="Status of the Invoice Header")
    po_number: Series[Float] = pa.Field(description="Purchase Order Number", nullable=True)
    po_order_date: Series[DateTime] = pa.Field(description="Purchase Order Date", nullable=True)
    po_status: Series[str] = pa.Field(description="Status of the Purchase Order", nullable=True)
    commodity: Series[str] = pa.Field(description="Commodity Type", nullable=True)
    po_line_commodity: Series[str] = pa.Field(description="Commodity Type at PO Line", nullable=True)
    category: Series[str] = pa.Field(description="Category of the Item", nullable=True)
    line_number: Series[int] = pa.Field(description="Line Number")
    total: Series[float] = pa.Field(description="Total Amount")
    po_line_number: Series[Float] = pa.Field(description="Purchase Order Line Number", nullable=True)
    po_line_total: Series[float] = pa.Field(description="Total at PO Line")
    description: Series[str] = pa.Field(description="Description of the Item", nullable=True)
    supplier: Series[str] = pa.Field(description="Supplier Name", nullable=True)
    supplier_number: Series[Float] = pa.Field(description="Supplier Number", nullable=True)
    billing: Series[str] = pa.Field(description="Billing Description")
    cost_object: Series[str] = pa.Field(description="Cost Object ID", nullable=True)


def parse_billing(row: pd.Series):
    """Parse the Billing information in the invoice dataset to attribute to specific accounts"""
    billing_str = str(row["Billing"]) if pd.notna(row["Billing"]) else ""
    original_total = row.get("Total", 0.0)
    results = []

    # Multi-account Allocation
    if "USD" in billing_str:
        parts = re.split(r"\s*;\s*", billing_str)
        for part in parts:
            if not part.strip():
                continue
            new_row = row.to_dict()

            # Extract Amount, allow negative, and handle comma
            amount_match = re.search(r"(-?[\d,]+\.\d+)\s*USD", part)
            if amount_match:
                clean_amount = amount_match.group(1).replace(",", "")
                new_row["Allocated Amount"] = float(clean_amount)
            else:
                new_row["Allocated Amount"] = 0.0

            # Extract Cost Object
            cost_obj_match = re.search(r"CostObj:\s*(\d{7})", part)
            if cost_obj_match:
                new_row["Cost Object"] = cost_obj_match.group(1)
            else:
                fallback_match = re.search(r"\b(\d{7})\b", part)
                new_row["Cost Object"] = fallback_match.group(1) if fallback_match else None

            results.append(new_row)

    # Single Account Fallback and extract Cost Object
    elif pd.notna(row["Billing"]):
        new_row = row.to_dict()
        cost_obj_match = re.search(r"\b(\d{7})\b", billing_str)
        new_row["Cost Object"] = cost_obj_match.group(1) if cost_obj_match else None
        new_row["Allocated Amount"] = original_total  # Take the full original total
        results.append(new_row)

    else:
        new_row = row.to_dict()
        new_row["Allocated Amount"] = 0.0
        results.append(new_row)

    return results


class InvoiceConfig(Config):
    """Configuration for the invoice asset

    Example: change this from the asset launchpad to specify the files to load to the data platform.
    """

    files_to_download: List[str] = ["PurchasedGoods_Invoice_FY2025"]
    execution_mode: Literal["local", "ecs"] = "local"
    write_mode: Literal["append", "replace"] = "append"


def _postgres_config() -> dict[str, str | int]:
    return {
        "user": _require_env("PG_USER"),
        "host": _require_env("PG_WAREHOUSE_HOST"),
        "password": _require_env("PG_PASSWORD"),
        "database": "postgres",
        "port": 5432,
    }


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if value:
        return value
    raise Failure(f"Missing required environment variable `{name}`.")


def load_purchased_goods_invoice_data(
    config: InvoiceConfig,
    dhub: DataHubResource,
) -> tuple[pd.DataFrame, dict[str, float | int]]:
    """Load and validate purchased goods invoice data before it is materialized."""
    project_id = dhub.get_project_id("Scope3 Purchased Goods")
    logger.info(f"Found project id: {project_id}!")
    dfs = []
    raw_total_amount = 0.0
    for file in config.files_to_download:
        download_links = dhub.search_files_from_project(project_id, file, tags=["invoice"])
        if len(download_links) == 0:
            logger.error("No download links found!")
            raise Failure("No download links found!")
        logger.info(f"Downloading {file}...")
        df = pd.read_csv(download_links[0])
        logger.info(f"Loading {len(df)} entries from {file}")
        df.dropna(subset=["Billing"], inplace=True)
        df["Total"] = pd.to_numeric(df["Total"], errors="coerce").fillna(0.0)
        raw_total_amount += df["Total"].sum()
        expanded = df.apply(parse_billing, axis=1).explode()
        df_fy = pd.DataFrame(expanded.tolist())
        df_fy["Allocated Amount"] = pd.to_numeric(df_fy["Allocated Amount"], errors="coerce").fillna(0.0)
        dfs.append(df_fy)
    combined = pd.concat(dfs, axis=0)
    combined["Invoice Date"] = pd.to_datetime(combined["Invoice Date"])
    combined["PO Order Date"] = pd.to_datetime(combined["PO Order Date"])
    combined.sort_values("Invoice Date", inplace=True)
    combined.columns = [normalize_column_name(col) for col in combined.columns]

    RELATIVE_TOLERANCE = 0.001
    allocated_total_amount = combined["allocated_amount"].sum()
    diff = abs(raw_total_amount - allocated_total_amount)
    percent_diff = (diff / raw_total_amount) if raw_total_amount != 0 else 0
    combined["total"] = combined["allocated_amount"]
    combined.drop(columns=["allocated_amount"], inplace=True)

    metadata: dict[str, float | int] = {
        "raw_total_amount": float(raw_total_amount),
        "allocated_total_amount": float(allocated_total_amount),
        "difference": float(diff),
        "percent_diff": float(percent_diff),
        "total_entries": int(len(combined)),
    }

    if percent_diff > RELATIVE_TOLERANCE:
        error_msg = (
            f"CRITICAL DATA INTEGRITY FAILURE: Allocations do not match Invoices.\n"
            f"Original Invoice Total: {raw_total_amount:,.2f}\n"
            f"Allocated Split Total:  {allocated_total_amount:,.2f}\n"
            f"Percent Difference:     {percent_diff:,.4f}"
        )
        logger.error(error_msg)
        raise Failure(error_msg)

    validated = InvoiceSchema.validate(combined)
    return validated, metadata


def write_purchased_goods_invoice_to_postgres(df: pd.DataFrame, write_mode: str) -> None:
    with connect_postgresql(_postgres_config()) as conn:
        if write_mode == "replace":
            with conn.begin():
                table_exists = (
                    conn.execute(
                        text(
                            """
                            SELECT 1
                            FROM information_schema.tables
                            WHERE table_schema = 'raw' AND table_name = :table_name
                            """
                        ),
                        {"table_name": PURCHASED_GOODS_INVOICE_TABLE},
                    ).first()
                    is not None
                )
                if table_exists:
                    conn.execute(text(f'DELETE FROM raw."{PURCHASED_GOODS_INVOICE_TABLE}";'))
        df.to_sql(
            con=conn,
            name=PURCHASED_GOODS_INVOICE_TABLE,
            schema="raw",
            if_exists="append",
            chunksize=500,
            index=False,
        )


def run_purchased_goods_invoice_on_ecs(
    context: AssetExecutionContext,
    config: InvoiceConfig,
    ecs_pipes_client: PipesECSClient,
):
    logger.info("Offloading `purchased_goods_invoice` materialization to ECS.")
    return ecs_pipes_client.run(
        context=context,
        run_task_params=build_ecs_run_task_params(command=PURCHASED_GOODS_INVOICE_ECS_COMMAND),
        extras={
            "files_to_download": config.files_to_download,
            "write_mode": config.write_mode,
        },
    ).get_materialize_result()


@asset(
    compute_kind="python",
    group_name="raw",
    dagster_type=pandera_schema_to_dagster_type(InvoiceSchema),
)
def purchased_goods_invoice(
    context: AssetExecutionContext,
    config: InvoiceConfig,
    dhub: ResourceParam[DataHubResource],
    ecs_pipes_client: PipesECSClient,
):
    """This asset ingest the specified pruchased goods invoice files
    from the Data Hub"""
    if config.execution_mode == "ecs":
        return run_purchased_goods_invoice_on_ecs(context, config, ecs_pipes_client)

    combined, metadata = load_purchased_goods_invoice_data(config, dhub)
    write_purchased_goods_invoice_to_postgres(combined, config.write_mode)
    return MaterializeResult(
        metadata={
            **metadata,
            "execution_mode": config.execution_mode,
            "write_mode": config.write_mode,
        }
    )


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
        raise Failure("No download links found!")
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


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def purchased_goods_duplicated_category(dhub: ResourceParam[DataHubResource]):
    """This asset ingest the specified duplicated categories (level_3 commidity) for filtering"""
    project_id = dhub.get_project_id("Scope3 Purchased Goods")
    logger.info(f"Found project id: {project_id}!")

    download_links = dhub.search_files_from_project(project_id, "duplicated_pns_category")
    if len(download_links) == 0:
        logger.error("No download links found!")
        raise Failure("No download links found!")
    df = pd.read_csv(download_links[0])
    df["id"] = df.index
    return df


# sync the processed table back to datahub
dhub_purchased_goods_invoice = add_dhub_sync(
    asset_name="dhub_purchased_goods_invoice",
    table_key=["staging", "stg_purchased_goods_invoice"],
    config={
        "filename": "purchased_goods_invoice.parquet.gz",
        "project_name": "Scope3 Purchased Goods",
        "description": "Processed Purchased Goods Invoice line data with emission factors and GHG",
        "title": "Processed Purchased Goods Invoice data",
        "ext": "parquet",
    },
)
