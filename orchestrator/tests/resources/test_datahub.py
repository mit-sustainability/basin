"""Unit tests for the utitlity to connect to MIT Datahub"""

from unittest.mock import patch, Mock

import pytest
import requests_mock

from orchestrator.resources.datahub import (
    data_hub_authorize,
    DataHubResource,
)


def test_data_hub_authorize_success():
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": {"jwt": "fake_jwt_token"}}

    with patch("requests.post", return_value=mock_response):
        result = data_hub_authorize("dummy_auth_token")
        assert result == "fake_jwt_token"


def test_data_hub_authorize_failure():
    mock_response = Mock()
    mock_response.status_code = 403  # Forbidden or any non-200 code
    with patch("requests.post", return_value=mock_response):
        result = data_hub_authorize("dummy_auth_token")
        assert result is None


@pytest.fixture
def mock_data_hub_resource():
    with patch(
        "orchestrator.resources.datahub.data_hub_authorize",
        return_value="mock_jwt_token",
    ):
        yield DataHubResource("dummy_auth_token")


def test_list_projects_success(mock_data_hub_resource):
    projects_response = {"data": {"projects": [{"project_id": "123", "display_name": "Test Project"}]}}
    with requests_mock.Mocker() as m:
        m.get(
            f"{mock_data_hub_resource.api_endpoint}/user",
            json=projects_response,
            status_code=200,
        )
        result = mock_data_hub_resource.list_projects()
        assert result == [{"project_id": "123", "display_name": "Test Project"}]


def test_get_project_id(mock_data_hub_resource):
    with patch.object(
        mock_data_hub_resource,
        "list_projects",
        return_value=[{"project_id": "123", "display_name": "Test Project"}],
    ):
        result = mock_data_hub_resource.get_project_id("Test Project")
        assert result == "123"
