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

from orchestrator.assets.utils import (
    normalize_column_name,
)


logger = get_dagster_logger()


class AttendanceSchema(pa.DataFrameModel):
    """Validate the output data schema of attendance record"""

    attendance_status: Series[str] = pa.Field(description="Attendance status (e.g., Present, Absent)")
    first_name: Series[str] = pa.Field(alias="first_name", description="Attendee's first name")
    last_name: Series[str] = pa.Field(alias="last_name", description="Attendee's last name")
    email: Series[str] = pa.Field(alias="email", description="Attendee's email address")
    event_date: Series[datetime] = pa.Field(description="Date of the event")
    event: Series[str] = pa.Field(description="Name of the event")
    record_id: Series[str] = pa.Field(description="Record ID number")
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

    metadata = {
        "total_entries": len(df),
    }

    output_df = df.copy()
    output_df.columns = [normalize_column_name(col) for col in output_df.columns]
    rename_map = {
        'Attendance Status","Attendee': "status",
        "Attendee - First Name": "first_name",
        "Attendee - Last Name": "last_name",
        "Email (Temp)": "email",
        "Event Date": "event_date",
        "Event Name": "event",
        "Record ID#": "record_id",
    }
    output_df["last_update"] = datetime.now()
    output_df = output_df.rename(columns=rename_map)

    return Output(value=output_df, metadata=metadata)
