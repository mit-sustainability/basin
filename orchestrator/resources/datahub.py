"""This resource interact with data.mit.edu through API,
and requires authorization to get temporary credentials following the official doc
http://dsg-datahub-apidoc.s3-website-us-east-1.amazonaws.com/
"""

from io import BytesIO, StringIO, IOBase
import requests

from dagster import get_dagster_logger
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from requests.exceptions import SSLError

logger = get_dagster_logger()
default_timeout = 10


@retry(
    stop=stop_after_attempt(5),
    wait=wait_fixed(2),
    retry=retry_if_exception_type(SSLError),
)
def upload_data_to_dhub(url: str, data: IOBase, ext: str = "csv"):
    """Upload data to datahub using the given url with retries for SSL error.
    Args:
        url: An AWS presigned URL to upload the data.
        data: StringIO or BytesIO object containing the data to be uploaded.
        ext: file extension of the data to be uploaded, default to "csv".
    """
    if ext == "csv":
        payload = {
            "data": data.getvalue().encode("utf-8"),
            "headers": {"Content-Type": "text/csv"},
        }
    elif ext == "parquet":
        payload = {
            "data": data,
            "headers": {"Content-Type": "application/gzip"},
        }
    else:
        logger.error("Unsupported file extension")
        return

    res = requests.put(url, timeout=1000, **payload)
    if res.status_code == 200:
        print("Upload Successful")
    else:
        logger.error(f"Failed to upload. Status code: {res.status_code}")


def data_hub_authorize(auth_token):
    """This function will return a temporary token to access datahub"""
    url = "https://data.mit.edu/api/auth"
    headers = {"accept": "application/json", "Content-Type": "application/json"}
    data = {"token": auth_token}
    res = requests.post(url, headers=headers, json=data, timeout=10)
    if res.status_code == 200:
        return res.json()["data"]["jwt"]
    else:
        logger.error("Fail to authorize on Data Hub")
        return


class DataHubResource:
    """This resource contains methods interacting with MIT Data Hub API"""

    def __init__(self, auth_token):
        logger.info("Instantiate the DHub resource")
        self.auth_token = auth_token
        self.api_endpoint = "https://data.mit.edu/api"
        self.jwt = data_hub_authorize(self.auth_token)
        self.headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {self.jwt}",
        }

    def list_projects(self):
        """Return a list of projects the user has access to."""
        url = f"{self.api_endpoint}/user"
        res = requests.get(url, headers=self.headers, timeout=default_timeout)
        if res.status_code == 200:
            logger.info("Successfully connected to Data Hub")
            return res.json()["data"]["projects"]
        logger.error("Fail to list projects.")
        return None

    def get_project_id(self, project_name):
        """Return the project_id for the project with the given name."""
        projects = self.list_projects()
        for project in projects:
            if project["display_name"] == project_name:
                return project["project_id"]

    def get_download_link(self, file_id):
        """Return a download link for the file"""
        url = f"{self.api_endpoint}/file/{file_id}"
        res = requests.get(url, headers=self.headers, timeout=default_timeout)
        if res.status_code == 200:
            return res.json()["data"]["temporarily_download_url"]
        return None

    def search_files_from_project(self, project_id, search_term, **kwargs):
        """Return a list of file download links matching the search term in the project"""
        url = f"{self.api_endpoint}/search"
        data = {
            "term": search_term,
            "projects": [project_id],
            "paging": {"start": 0, "size": 50},
        }
        # Allow advance search with additional parameters
        data.update(kwargs)
        res = requests.post(url, headers=self.headers, json=data, timeout=default_timeout)
        if res.status_code == 200:
            files = res.json()["data"]
        else:
            logger.error(f"Fail to find the file with name {search_term} in project {project_id}.")
        try:
            download_links = [self.get_download_link(file["hash_id"]) for file in files]
            logger.info(f"Successfully obtained {len(download_links)} download links.")
            return download_links
        except UnboundLocalError:
            logger.error("Fail to find any downloadable links.")
            return None

    def get_upload_link(self, meta):
        """Get upload link to datahub"""
        url = f"{self.api_endpoint}/file"
        res = requests.post(url, headers=self.headers, json=meta, timeout=default_timeout)
        if res.status_code == 200:
            return res.json()["data"]["temporarily_upload_url"]

    def sync_dataframe(self, df: pd.DataFrame, meta: dict, ext: str):
        """Sync the dataframe to target csv on datahub
        Args:
            df: dataframe to be uploaded.
            meta: datahub file configuration, including project, title, and filenames.
            ext: file extension of the data to be uploaded.

        """
        upload_link = self.get_upload_link(meta)
        if ext == "csv":
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            upload_data_to_dhub(upload_link, csv_buffer)
        else:
            out_buffer = BytesIO()
            df.to_parquet(out_buffer, index=False, compression="gzip")
            out_buffer.seek(0)
            upload_data_to_dhub(upload_link, out_buffer, ext=ext)
