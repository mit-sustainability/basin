"""This resource interact with data.mit.edu through API,
and requires authorization to get temporary credentials following the official doc
http://dsg-datahub-apidoc.s3-website-us-east-1.amazonaws.com/
"""

import requests

# from typing import Iterator, Optional, Sequence

from dagster import (
    get_dagster_logger,
)


logger = get_dagster_logger()
default_timeout = 5


def data_hub_authorize(auth_token):
    """This function will return a temporary token to access datahub"""
    url = "https://data.mit.edu/api/auth"
    headers = {"accept": "application/json", "Content-Type": "application/json"}
    data = {"token": auth_token}
    res = requests.post(url, headers=headers, json=data, timeout=10)
    if res.status_code == 200:
        return res.json()["data"]["jwt"]
    else:
        logger.error("Fail to authorize on Data Hub.")
        return


class DataHubResource:
    """This resource will create a postgresql connection engine."""

    def __init__(self, auth_token):
        self.auth_token = auth_token
        self.api_endpoint = "https://data.mit.edu/api"
        self.jwt = data_hub_authorize(self.auth_token)

    def list_projects(self):
        url = f"{self.api_endpoint}/user"
        headers = {"accept": "application/json", "Authorization": f"Bearer {self.jwt}"}
        res = requests.get(url, headers=headers, timeout=default_timeout)
        if res.status_code == 200:
            return res.json()["data"]["projects"]
        else:
            logger.error("Fail to list projects.")
            return None

    def get_project_id(self, project_name):
        projects = self.list_projects()
        for project in projects:
            if project["display_name"] == project_name:
                return project["project_id"]

    def get_download_link(self, file_id):
        """Return a download link for the file"""
        url = f"{self.api_endpoint}/file/{file_id}"
        headers = {"accept": "application/json", "Authorization": f"Bearer {self.jwt}"}
        res = requests.get(url, headers=headers, timeout=default_timeout)
        if res.status_code == 200:
            return res.json()["data"]["temporarily_download_url"]
        else:
            logger.error("Fail to list projects.")
            return None

    def search_files_from_project(self, project_id, search_term):
        """Return a list of file download links matching the search term in the project"""
        url = f"{self.api_endpoint}/search"
        headers = {"accept": "application/json", "Authorization": f"Bearer {self.jwt}"}
        data = {
            "term": search_term,
            "projects": [project_id],
            "paging": {"start": 0, "size": 20},
        }
        res = requests.post(url, headers=headers, json=data, timeout=default_timeout)
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
