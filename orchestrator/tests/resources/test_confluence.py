from unittest.mock import Mock, patch

import pytest
from dagster import Failure
import requests

from orchestrator.resources.confluence import ConfluenceResource


def test_confluence_headers_use_bearer_token():
    resource = ConfluenceResource(auth_token="secret-token")

    assert resource._headers()["Authorization"] == "Bearer secret-token"


def test_confluence_list_pages_flattens_paginated_api_results():
    resource = ConfluenceResource(
        auth_token="secret-token",
        base_url="https://wikis.mit.edu/confluence",
        space_key="MITOS",
        page_limit=2,
    )

    paginated_responses = [
        {
            "results": [
                {
                    "id": "1",
                    "title": "One",
                    "_links": {"webui": "/display/MITOS/One"},
                    "body": {"storage": {"value": "<p>One</p>"}},
                    "history": {"createdBy": {"displayName": "Ada"}},
                    "metadata": {"labels": {"results": [{"name": "alpha"}]}},
                    "version": {"number": 1, "when": "2026-04-01T00:00:00.000Z"},
                },
                {
                    "id": "2",
                    "title": "Two",
                    "_links": {"webui": "/display/MITOS/Two"},
                    "body": {"storage": {"value": "<p>Two</p>"}},
                    "history": {"createdBy": {"displayName": "Ada"}},
                    "metadata": {"labels": {"results": []}},
                    "version": {"number": 1, "when": "2026-04-01T00:00:00.000Z"},
                },
            ]
        },
        {
            "results": [
                {
                    "id": "3",
                    "title": "Three",
                    "_links": {"webui": "/display/MITOS/Three"},
                    "body": {"storage": {"value": "<p>Three</p>"}},
                    "history": {"createdBy": {"displayName": "Ada"}},
                    "metadata": {"labels": {"results": [{"name": "beta"}]}},
                    "version": {"number": 2, "when": "2026-04-02T00:00:00.000Z"},
                }
            ]
        },
    ]

    with patch.object(resource, "_request_json", side_effect=paginated_responses) as request_mock:
        pages = resource.list_pages()

    # The resource should treat the two paginated responses as one logical page list.
    assert [page.page_id for page in pages] == ["1", "2", "3"]
    assert request_mock.call_count == 2
    assert request_mock.call_args_list[0].args[1]["start"] == 0
    assert request_mock.call_args_list[1].args[1]["start"] == 2


def test_confluence_request_errors_raise_failure():
    resource = ConfluenceResource(auth_token="secret-token")
    response = Mock()
    response.raise_for_status.side_effect = requests.HTTPError("boom")

    with patch("orchestrator.resources.confluence.requests.get", return_value=response):
        with pytest.raises(Failure):
            resource._request_json("/rest/api/content", params={})
