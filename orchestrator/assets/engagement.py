from datetime import datetime
from dagster import (
    asset,
    get_dagster_logger,
    Output,
)
from dagster_aws.s3 import S3Resource
from dagster_pandera import pandera_schema_to_dagster_type
import pandas as pd
import pandera as pa
from pandera.typing import Series, DateTime

from orchestrator.resources.mit_warehouse import MITWHRSResource


logger = get_dagster_logger()


class AttendanceSchema(pa.DataFrameModel):
    """Validate the output data schema of attendance record"""

    status: Series[str] = pa.Field(description="Attendance status (e.g., Present, Absent)", nullable=True)
    first_name: Series[str] = pa.Field(alias="first_name", description="Attendee's first name")
    last_name: Series[str] = pa.Field(alias="last_name", description="Attendee's last name")
    email: Series[str] = pa.Field(alias="email", description="Attendee's email address")
    event_date: Series[DateTime] = pa.Field(description="Date of the event")
    event: Series[str] = pa.Field(description="Name of the event")
    record_id: Series[int] = pa.Field(description="Record ID number")
    last_update: Series[DateTime] = pa.Field(description="Date of last update")


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
    dagster_type=pandera_schema_to_dagster_type(AttendanceSchema),
)
def attendance_records(s3: S3Resource) -> Output[pd.DataFrame]:
    """Load synced attendance records from S3"""
    source_bucket = "mitos-landing-zone"
    # Get the data from S3
    s3_client = s3.get_client()
    obj = s3_client.get_object(Bucket=source_bucket, Key="attendance/attendance_record_quickbase.csv")
    df = pd.read_csv(obj["Body"])

    output_df = df.copy()
    rename_map = {
        "Attendance Status": "status",
        "Attendee - First Name": "first_name",
        "Attendee - Last Name": "last_name",
        "Email (Temp)": "email",
        "Event Date": "event_date",
        "Event Name": "event",
        "Record ID#": "record_id",
    }

    output_df["last_update"] = datetime.now()
    output_df = output_df.rename(columns=rename_map)
    output_df["event_date"] = pd.to_datetime(output_df["event_date"], errors="coerce")
    output_df["status"] = output_df["status"].map({True: "Present", False: "Absent"})
    output_df.dropna(subset=["first_name", "last_name", "email"], inplace=True)
    metadata = {
        "total_entries": len(output_df),
    }
    return Output(value=output_df, metadata=metadata)


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def employee_directory(dwrhs: MITWHRSResource) -> Output[pd.DataFrame]:
    """Load employee directory from data warehouse"""
    # Define query from DWRHS and save to Dataframe
    logger.info("Connect to MIT data warehouse to ingest employee directory")
    query = """
            SELECT
                DIRECTORY_TITLE,
                DEPARTMENT_NAME,
                lower(EMAIL_ADDRESS) AS EMAIL
            FROM WAREUSER.EMPLOYEE_DIRECTORY
        """
    rows = dwrhs.execute_query(query, chunksize=100000)
    columns = [
        "title_category",
        "department_name",
        "email",
    ]
    df = pd.DataFrame(rows, columns=columns)

    output_df = df.copy()

    output_df["last_update"] = datetime.now()
    output_df.dropna(subset=["department_name", "email"], inplace=True)
    metadata = {
        "total_entries": len(output_df),
    }
    return Output(value=output_df, metadata=metadata)


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def student_directory(dwrhs: MITWHRSResource) -> Output[pd.DataFrame]:
    """Load employee directory from data warehouse"""
    # Define query from DWRHS and save to Dataframe
    logger.info("Connect to MIT data warehouse to ingest student directory")
    query = """
            SELECT
                LOWER(EMAIL_ADDRESS) AS EMAIL,
                DEPARTMENT_NAME,
                STUDENT_YEAR,
                WAREHOUSE_LOAD_DATE
            FROM WAREUSER.MIT_STUDENT_DIRECTORY
        """
    rows = dwrhs.execute_query(query, chunksize=100000)
    columns = [
        "email",
        "department_name",
        "student_year",
        "last_update",
    ]
    df = pd.DataFrame(rows, columns=columns)

    output_df = df.copy()
    output_df["last_update"] = pd.to_datetime(output_df["last_update"], errors="coerce")
    output_df.dropna(subset=["department_name", "email"], inplace=True)
    metadata = {
        "total_student_counts": len(output_df),
        "unique_department_name": len(output_df["department_name"].unique()),
    }

    return Output(value=output_df, metadata=metadata)
