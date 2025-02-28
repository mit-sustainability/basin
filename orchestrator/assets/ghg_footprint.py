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
from orchestrator.resources.mit_warehouse import MITWHRSResource


logger = get_dagster_logger()


@asset(io_manager_key="postgres_replace", compute_kind="python", group_name="raw")
def dlc_person_share(dwrhs: MITWHRSResource) -> Output[pd.DataFrame]:
    """Load DLC personnel count from data warehouse"""
    logger.info("Connect to MIT data warehouse to ingest Personnel count for each DLC")
    query = """
            WITH hc AS (
                SELECT DEPARTMENT_NUMBER, COUNT(*) AS headcount
                FROM WAREUSER.EMPLOYEE_DIRECTORY
                WHERE DEPARTMENT_NAME NOT LIKE 'Lincoln%'
                AND DEPARTMENT_NAME NOT LIKE 'Haystack%'
                GROUP BY DEPARTMENT_NUMBER
            ),

            org_dlc AS (
                SELECT HR_DEPARTMENT_CODE_OLD, DLC_KEY
                FROM WAREUSER.HR_ORG_UNIT_NEW
                GROUP BY HR_DEPARTMENT_CODE_OLD, DLC_KEY
            ),

            dlc_hc AS (
                SELECT
                    fo.DLC_KEY,
                    SUM(hc.headcount) AS total_employee
                FROM hc
                LEFT JOIN org_dlc fo
                ON hc.DEPARTMENT_NUMBER = fo.HR_DEPARTMENT_CODE_OLD
                GROUP BY DLC_KEY
            )

            SELECT
                DLC_KEY,
                TOTAL_EMPLOYEE,
                TOTAL_EMPLOYEE / (SUM(TOTAL_EMPLOYEE) OVER ()) AS percentage
            FROM dlc_hc
            ORDER BY TOTAL_EMPLOYEE DESC
        """
    rows = dwrhs.execute_query(query, chunksize=100000)
    columns = ["dlc_key", "total_employee", "percentage"]
    df = pd.DataFrame(rows, columns=columns)
    logger.info("Query executed successfully. Extract Personnel Count for DLCs")
    metadata = {
        "number_of_DLCs": len(df),
    }
    # Add an id column, and a timestamp column to the dataframe
    df.insert(0, "id", df.index + 1)
    df["last_update"] = datetime.now()
    # Convert FY columns to integer
    return Output(value=df, metadata=metadata)


@asset(io_manager_key="postgres_replace", compute_kind="python", group_name="raw")
def dlc_floor_share(dwrhs: MITWHRSResource) -> Output[pd.DataFrame]:
    """Load Building DLC share from data warehouse"""
    logger.info("Connect to MIT data warehouse to ingest floor area share for each DLC and building")
    query = """
            WITH DistinctDLC AS (
                    SELECT DISTINCT FCLT_ORGANIZATION_KEY, DLC_KEY
                    FROM WAREUSER.FCLT_ORG_DLC_KEY
                ),

                BuildingTotal AS (
                    SELECT
                        FCLT_BUILDING_KEY,
                        SUM(AREA) AS total_floor_area
                    FROM WAREUSER.FCLT_ROOMS r
                    LEFT JOIN DistinctDLC d
                        ON r.FCLT_ORGANIZATION_KEY = d.FCLT_ORGANIZATION_KEY
                    GROUP BY FCLT_BUILDING_KEY
                ),

                bd_breakdown AS (
                    SELECT
                        r.FCLT_BUILDING_KEY,
                        d.DLC_KEY,
                        SUM(r.AREA) AS total_area
                    FROM WAREUSER.FCLT_ROOMS r
                    LEFT JOIN DistinctDLC d
                        ON r.FCLT_ORGANIZATION_KEY = d.FCLT_ORGANIZATION_KEY
                    WHERE d.DLC_KEY IS NOT NULL
                    GROUP BY r.FCLT_BUILDING_KEY, d.DLC_KEY
                ),

                bd_share AS (
                    SELECT
                        bd.*,
                        bt.total_floor_area,
                        bd.total_area / bt.total_floor_area AS dlc_share
                    FROM bd_breakdown bd
                    LEFT JOIN BuildingTotal bt
                        ON bt.FCLT_BUILDING_KEY = bd.FCLT_BUILDING_KEY
                )

                SELECT FCLT_BUILDING_KEY, DLC_KEY, TOTAL_AREA, DLC_SHARE FROM bd_share
            """

    rows = dwrhs.execute_query(query, chunksize=100000)
    columns = [
        "fclt_building_key",
        "dlc_key",
        "total_area",
        "dlc_share",
    ]
    df = pd.DataFrame(rows, columns=columns)
    logger.info("Query executed successfully. Extract Assigned Space Share for DLCs")
    number_dlc = df["dlc_key"].nunique()
    metadata = {
        "number_of_DLCs": number_dlc,
    }
    # Add an id column, and a timestamp column to the dataframe
    df["last_update"] = datetime.now()
    # Convert FY columns to integer
    return Output(value=df, metadata=metadata)


# Sync processed table back to datahub
# dhub_ghg_inventory = add_dhub_sync(
#     asset_name="dhub_ghg_inventory",
#     table_key=["staging", "stg_ghg_inventory"],
#     config={
#         "filename": "ghg_inventory.csv",
#         "project_name": "GHG_Inventory",
#         "description": "Aggregated GHG inventory emissions by categories",
#         "title": "Aggregated GHG Inventory",
#     },
# )
