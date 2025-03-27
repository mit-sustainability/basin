from datetime import datetime
from dagster import asset, Output, get_dagster_logger, MetadataValue
import pandas as pd
from pandera.typing import Series, DateTime


from orchestrator.assets.utils import (
    add_dhub_sync,
    normalize_column_name,
)
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
def dlc_area_share(dwrhs: MITWHRSResource) -> Output[pd.DataFrame]:
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


@asset(io_manager_key="postgres_replace", compute_kind="python", group_name="raw")
def energy_distribution(em_connect: PostgreConnResources) -> Output[pd.DataFrame]:
    """Load purchased energy numbers from energy-cdr in energize-mit database"""
    engine = em_connect.create_engine()
    logger.info("Connect to energize_mit database to ingest the full energy_cdr table")
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
            """
    try:
        with engine.connect() as conn:
            df = pd.read_sql_query(query, conn)
        logger.info("Query executed successfully. Energy distribution data fetched from energize-mit.")
    except Exception as e:
        logger.error("An error occurred:", e)
        return
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


# Sync processed table back to datahub
dhub_dlc_footprint = add_dhub_sync(
    asset_name="dhub_dlc_footprint",
    table_key=["final", "final_footprint_records"],
    config={
        "filename": "dlc_footprint.csv",
        "project_name": "GHG_Inventory",
        "description": "GHG footprint broken down by DLCs and fiscal year",
        "title": "DLC-based GHG footprint",
        "ext": "parquet",
    },
)
