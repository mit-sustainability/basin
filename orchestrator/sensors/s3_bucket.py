"""S3 related sensors"""

from dagster import RunRequest, sensor, AssetKey, SkipReason
from dagster_aws.s3 import S3Resource

from orchestrator.jobs.ghg_inventory import ghg_job


def create_s3_file_sensor(bucket_name, file_key, asset_name, tag, job):
    """Create a dagster sensor that trigger asset materialization by checking S3 bucket updates"""

    @sensor(name=f"s3_file_update_sensor_{tag}", job=job)
    def s3_file_update_sensor(context, s3: S3Resource):
        """
        A Dagster sensor that detects updates to a specific file in the specified S3 bucket and triggers asset materialization.
        """
        s3_client = s3.get_client()
        try:
            # Retrieve the metadata of the specified file from S3
            response = s3_client.head_object(Bucket=bucket_name, Key=file_key)
            last_modified = response["LastModified"].isoformat()
        except s3_client.exceptions.NoSuchKey:
            yield SkipReason(f"File '{file_key}' not found in bucket '{bucket_name}'.")
            return

        # Get the last known modification time from the sensor's cursor
        last_known_modification = context.cursor
        if last_known_modification != last_modified:
            # Update the cursor with the current last modified time
            context.update_cursor(last_modified)
            # Trigger a run request to materialize the asset
            yield RunRequest(
                run_key=f"s3_file_update_{file_key}_{last_modified}",
                asset_selection=[AssetKey(asset_name)],
            )
        else:
            # Skip if the file has not been modified since the last check
            yield SkipReason(f"No updates detected for file '{file_key}' in S3 {bucket_name}")

    return s3_file_update_sensor


# Initiate sensors
sensor_ghg_manual = create_s3_file_sensor(
    "mitos-landing-zone",
    "all-scopes-sync/quickbase_data.csv",
    "ghg_manual_entries",
    "ghg_manual",
    job=ghg_job,
)
