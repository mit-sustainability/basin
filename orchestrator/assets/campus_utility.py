from datetime import datetime
import textwrap
from dagster import (
    asset,
    Failure,
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
    to_mmbtu,
)
from orchestrator.resources.datahub import DataHubResource
from orchestrator.resources.mit_warehouse import MITWHRSResource
from orchestrator.resources.postgres_io_manager import PostgreConnResources

logger = get_dagster_logger()


class BuildingMappingSchema(pa.DataFrameModel):
    """Validate the output data schema of Cost Collector to Building Mapping"""

    cost_collector_id: Series[int] = pa.Field(description="Cost Collector Id")
    cost_collector_name: Series[str] = pa.Field(description="Cost Collector Name")
    profit_center_id: Series[str] = pa.Field(description="Profit Center Id", nullable=True)
    profit_center_name: Series[str] = pa.Field(description="Profit Center Name", nullable=True)
    building_identifier: Series[str] = pa.Field(description="Building Identifier", nullable=True)

    # District Utility Columns
    district_steam: Series[bool] = pa.Field(description="District Steam connection")
    district_chilledwater: Series[bool] = pa.Field(description="District Chilled Water connection")
    district_electricity: Series[bool] = pa.Field(description="District Electricity connection")
    district_hotwater: Series[bool] = pa.Field(description="District Hot Water connection")

    # Building Groups and metadata
    building_group: Series[str] = pa.Field(description="Building Group")
    combined_building_number: Series[str] = pa.Field(description="Combined Building Number", nullable=True)
    agg_bldg: Series[str] = pa.Field(description="Aggregate Building", nullable=True)
    ext_gross_area: Series[float] = pa.Field(description="External Gross Area", nullable=True)
    last_update: Series[DateTime] = pa.Field(description="Date of last update")


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
    dagster_type=pandera_schema_to_dagster_type(BuildingMappingSchema),
)
def campus_building_mapping(dhub: ResourceParam[DataHubResource]):
    """This asset ingests Cost Collector to Building Mapping from Data Hub"""
    project_id = dhub.get_project_id("Energize-MIT")
    logger.info(f"Found project id: {project_id}!")
    download_links = dhub.search_files_from_project(project_id, "cost_collector_building_groups")
    if len(download_links) == 0:
        raise Failure("No download links found!")
    df = pd.read_csv(download_links[0])

    cols = [
        "Cost Collector Id",
        "Cost Collector Name",
        "Profit Center Id",
        "Profit Center Name",
        "Building Identifier",
        "district_steam",
        "district_chilledwater",
        "district_electricity",
        "district_hotwater",
        "Building Group",
        "combined_Building_Number",
        "agg_bldg",
        "EXT_GROSS_AREA",
    ]
    output_df = df[cols].copy()
    output_df.columns = [normalize_column_name(col) for col in output_df.columns]
    output_df["last_update"] = datetime.now()
    unique_buildings = len(output_df["agg_bldg"].unique())
    total_buildings = len(output_df)

    metadata = {
        "unique_building_counts": unique_buildings,
        "total_buildings": total_buildings,
    }

    return Output(value=output_df, metadata=metadata)


@asset(io_manager_key="postgres_replace", compute_kind="python", group_name="raw")
def utility_usage_cost(dwrhs: MITWHRSResource) -> Output[pd.DataFrame]:
    """Load MIT monthly utility usage and cost since fiscal year 2006 from data warehouse"""
    logger.info("Connect to MIT data warehouse to ingest utility usage and cost")
    query = textwrap.dedent(
        """\
        WITH grouped AS (
            SELECT
                ut.POSTING_DATE,
                ut.UTILITY_USAGE_AMOUNT,
                ut.NUMBER_OF_UNITS,
                ut.UNIT_OF_MEASURE,
                ut.NUMBER_OF_BTU,
                ucc.COST_COLLECTOR_ID,
                ucc.COST_COLLECTOR_NAME,
                ucc.PROFIT_CENTER_NAME,
                ucc.ON_OFF_CAMPUS_STATUS,
                tm.FISCAL_YEAR,
                tm.START_DATE,
                tm.CALENDAR_MONTH,
                gl.GL_ACCOUNT_ID,
                gl.GL_ACCOUNT_NAME,
                CASE
                    WHEN gl.GL_ACCOUNT_ID IN ('421112', '421114') THEN '#2 Oil'
                    WHEN gl.GL_ACCOUNT_ID IN ('421113') THEN '#6 Oil'
                    WHEN gl.GL_ACCOUNT_ID IN ('421108', '421120', '421139', '421148', '421162', '421118') THEN 'Gas'
                    WHEN gl.GL_ACCOUNT_ID IN ('421111', '421110') THEN 'Electricity'
                    WHEN gl.GL_ACCOUNT_ID = '421109' THEN 'Water'x
                    WHEN gl.GL_ACCOUNT_ID = '421107' THEN 'Sewer'
                    WHEN gl.GL_ACCOUNT_ID = '600751' THEN 'Chilled Water'
                    WHEN gl.GL_ACCOUNT_ID = '600752' THEN 'Produced Electricity'
                    WHEN gl.GL_ACCOUNT_ID = '600750' THEN 'Steam'
                    ELSE 'Other'
                END AS UTILITY_TYPE
            FROM WAREUSER.UTILITY_USAGE_DETAIL_WITH_CUP ut
            JOIN WAREUSER.UTILITY_COST_COLLECTOR ucc ON ut.COST_COLLECTOR_KEY = ucc.COST_COLLECTOR_KEY
            JOIN WAREUSER.TIME_MONTH tm ON ut.TIME_MONTH_KEY = tm.TIME_MONTH_KEY
            JOIN WAREUSER.GL_ACCOUNT gl ON ut.GL_ACCOUNT_KEY = gl.GL_ACCOUNT_KEY
            WHERE tm.FISCAL_YEAR > 2005
        ),

        --- Exclude Commodity GL_Accounts avoiding Usage double count
        ut_usage AS (
            SELECT
                FISCAL_YEAR,
                CALENDAR_MONTH,
                COST_COLLECTOR_ID,
                COST_COLLECTOR_NAME,
                PROFIT_CENTER_NAME,
                UTILITY_TYPE,
                SUM(NUMBER_OF_BTU) AS total_btu,
                SUM(NUMBER_OF_UNITS) AS utility_usage,
                SUM(UTILITY_USAGE_AMOUNT) AS UTILITY_COST,
                MAX(UNIT_OF_MEASURE) AS UTILITY_UNIT
            FROM grouped
            WHERE UTILITY_TYPE != 'Other'
            AND GL_ACCOUNT_ID NOT IN ('421162', '421118', '421110')
            GROUP BY FISCAL_YEAR, COST_COLLECTOR_ID, COST_COLLECTOR_NAME, PROFIT_CENTER_NAME, UTILITY_TYPE, CALENDAR_MONTH
        ),

        --- Cost includes both delivery/supply GL_Accounts
        ut_cost AS (
            SELECT
                FISCAL_YEAR,
                CALENDAR_MONTH,
                COST_COLLECTOR_ID,
                COST_COLLECTOR_NAME,
                PROFIT_CENTER_NAME,
                UTILITY_TYPE,
                SUM(UTILITY_USAGE_AMOUNT) AS UTILITY_COST,
                SUM(NUMBER_OF_UNITS) AS utility_usage
            FROM grouped
            WHERE UTILITY_TYPE != 'Other'
            GROUP BY FISCAL_YEAR, COST_COLLECTOR_ID, COST_COLLECTOR_NAME, PROFIT_CENTER_NAME, UTILITY_TYPE, CALENDAR_MONTH
        )

        SELECT
            utu.FISCAL_YEAR,
            utu.CALENDAR_MONTH,
            utu.COST_COLLECTOR_ID,
            utu.COST_COLLECTOR_NAME,
            utu.PROFIT_CENTER_NAME,
            utu.UTILITY_TYPE,
            utu.UTILITY_UNIT,
            utu.total_btu,
            utu.utility_usage,
            utc.UTILITY_COST AS utility_cost
        FROM ut_usage utu
        LEFT JOIN ut_cost utc
            ON  utu.FISCAL_YEAR = utc.FISCAL_YEAR
            AND utu.CALENDAR_MONTH = utc.CALENDAR_MONTH
            AND utu.COST_COLLECTOR_ID = utc.COST_COLLECTOR_ID
            AND utu.UTILITY_TYPE = utc.UTILITY_TYPE
        ORDER BY FISCAL_YEAR, CALENDAR_MONTH
    """
    )
    rows = dwrhs.execute_query(query, chunksize=100000)
    if len(rows) == 0:
        raise Failure(description="Faile to load any data", metadata={"num_rows": len(rows)})
    columns = [
        "FISCAL_YEAR",
        "CALENDAR_MONTH",
        "COST_COLLECTOR_ID",
        "COST_COLLECTOR_NAME",
        "PROFIT_CENTER_NAME",
        "UTILITY_TYPE",
        "UTILITY_UNIT",
        "TOTAL_BTU",
        "UTILITY_USAGE",
        "UTILITY_COST",
    ]
    df = pd.DataFrame(rows, columns=columns)
    df.columns = [normalize_column_name(col) for col in df.columns]
    logger.info("Query executed successfully. Load monthly utility usage and cost data")
    logger.info(f"we got {len(df)} entries for utility since 2006")
    metadata = {
        "number_of_unique_cost_collectors": df["cost_collector_id"].nunique(),
        "beginning_fy": df["fiscal_year"].min(),
        "ending_fy": df["fiscal_year"].max(),
        "total_entries": len(df),
    }
    # Add an id column, and a timestamp column to the dataframe
    df.insert(0, "id", df.index + 1)
    df["last_update"] = datetime.now()
    return Output(value=df, metadata=metadata)


@asset(
    deps=[utility_usage_cost, campus_building_mapping],
    io_manager_key="postgres_replace",
    compute_kind="python",
    key_prefix="staging",
    group_name="staging",
)
def stg_utility_history(pg_engine: PostgreConnResources):
    """Combine utility usage and cost with building groups mapping"""

    engine = pg_engine.create_engine()
    udf = pd.read_sql_query("SELECT * FROM raw.utility_usage_cost", engine)
    bg_mapping = pd.read_sql_query("SELECT * FROM raw.campus_building_mapping", engine)

    # Type Casting for selected columns
    udf["cost_collector_id"] = udf["cost_collector_id"].astype(str)
    udf["fiscal_year"] = udf["fiscal_year"].astype(int)
    udf["calendar_month"] = udf["calendar_month"].astype(int)
    bg_mapping["cost_collector_id"] = bg_mapping["cost_collector_id"].astype(str)
    bg_mapping["district_electricity"] = bg_mapping["district_electricity"].astype(bool)

    # Merge to append building information
    mdf = udf.merge(
        bg_mapping[
            [
                "cost_collector_id",
                "district_electricity",
                "building_group",
                "agg_bldg",
                "ext_gross_area",
            ]
        ],
        on="cost_collector_id",
        how="left",
    )

    # Adjust 2021 June gas usage (ensure matching column dtypes)
    mask = (mdf["fiscal_year"] == 2021) & (mdf["calendar_month"] == 6) & (mdf["cost_collector_id"] == "1814201")
    mdf.loc[mask, ["calendar_month", "utility_cost", "total_btu", "utility_usage"]] = [
        6,  # move record to June
        707448.70,  # replacement cost
        0,  # replacement BTU
        1934740,  # replacement usage units
    ]

    # Calculate MMBtu for various utility types
    mdf["utility_mmbtu"] = mdf.apply(to_mmbtu, axis=1)
    # Select necessary Columns
    out_cols = [
        "fiscal_year",
        "calendar_month",
        "cost_collector_id",
        "cost_collector_name",
        "profit_center_name",
        "utility_type",
        "utility_cost",
        "utility_usage",
        "utility_unit",
        "utility_mmbtu",
        "district_electricity",
        "building_group",
        "agg_bldg",
        "ext_gross_area",
    ]
    out_df = mdf[out_cols].copy()
    metadata = {"total_entries": len(out_df)}
    # Add a timestamp column to the dataframe
    out_df["last_update"] = datetime.now()
    return Output(value=out_df, metadata=metadata)
