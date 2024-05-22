from datetime import datetime

from dagster import (
    asset,
    get_dagster_logger,
    ResourceParam,
)
from dagster_pandera import pandera_schema_to_dagster_type
import pandera as pa
from pandera.typing import Series, DateTime

import pandas as pd

from orchestrator.assets.utils import add_dhub_sync
from orchestrator.resources.datahub import DataHubResource
from orchestrator.resources.mit_warehouse import MITWHRSResource

logger = get_dagster_logger()


class ParkingNewbatchData(pa.SchemaModel):
    """Validate the output data schema of newbatch parking data"""

    date: Series[DateTime] = pa.Field(description="Waste collection building id")
    parking_lot: Series[str] = pa.Field(description="Waste collection building name")
    total: Series[int] = pa.Field(description="Waste collection site street")
    unique: Series[int] = pa.Field(description="Service Date")
    last_update: Series[DateTime] = pa.Field(description="Date of last update")


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def historical_parking_daily(dhub: ResourceParam[DataHubResource]):
    """This asset ingest the daily historical parking data from the Data Hub"""
    project_id = dhub.get_project_id("Parking")
    logger.info(f"Found project id: {project_id}!")
    download_links = dhub.search_files_from_project(project_id, "historical_parking_daily")
    if len(download_links) == 0:
        logger.error("No download links found!")
        return pd.DataFrame()
    # Load the data
    df = pd.read_csv(download_links[0])
    df["date"] = pd.to_datetime(df["date"])
    logger.info(f"Loaded historical parking data till: {df.date.max()}!")
    return df


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
    dagster_type=pandera_schema_to_dagster_type(ParkingNewbatchData),
)
def newbatch_parking_daily(dwhrs: MITWHRSResource):
    """This asset ingest the new batch of parking activity data from MIT Warehouse"""
    query = """
            SELECT ENTRY_DATE AS "date",
                PARKING_LOT_ID AS parking_lot,
                COUNT(PARKER_ACCOUNT_ID) AS "total",
                COUNT(DISTINCT MIT_ID) AS "unique"
            FROM WAREUSER.PARK_TX_ACTIVITY
            WHERE (PARK_ACTIVITY_TYPE_KEY = 'G' OR PARK_ACTIVITY_TYPE_KEY = 'E')
                AND ENTRY_DATE > TO_DATE('2021-06-25', 'YYYY-MM-DD')
                AND TO_CHAR(ENTRY_DATE, 'DY', 'NLS_DATE_LANGUAGE = AMERICAN') NOT IN ('SAT', 'SUN')
            GROUP BY ENTRY_DATE, PARKING_LOT_ID
            """
    rows = dwhrs.execute_query(query, chunksize=100000)
    columns = [
        "date",
        "parking_lot",
        "total",
        "unique",
    ]
    df = pd.DataFrame(rows, columns=columns)
    df["last_update"] = datetime.now()
    logger.info(f"Last updated batch parking transaction data since date: {df.date.min()}")
    return df


# # Sync to datahub using the factory function
# dhub_waste_sync = add_dhub_sync(
#     asset_name="dhub_waste_sync",
#     table_key=["final", "final_waste_recycle"],
#     config={
#         "filename": "final_waste_update",
#         "project_name": "Material Matters",
#         "description": "Processed waste data including historical, Casella and small stream",
#         "title": "Processed Waste Data till 2023",
#     },
# )
