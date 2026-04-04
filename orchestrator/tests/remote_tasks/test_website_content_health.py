from unittest.mock import MagicMock, patch

import pandas as pd

from orchestrator.remote_tasks.website_content_health import main


def test_remote_website_content_health_task_scans_writes_and_reports_metadata():
    pages_df = pd.DataFrame(
        [
            {
                "page_url": "https://sustainability.mit.edu/resources/example",
                "page_path": "/resources/example",
                "page_title": "Example",
                "category": "resources",
                "topic": "example",
                "last_update": None,
                "http_status": 200,
                "page_health": "healthy",
                "meta_description": "Example page",
                "content_text": "Example",
                "content_hash": "abc123",
                "link_count": 1,
                "scanned_at": "2026-04-03T00:00:00+00:00",
            }
        ]
    )
    link_refs_df = pd.DataFrame(
        [
            {
                "source_page_url": "https://sustainability.mit.edu/resources/example",
                "link_url": "https://example.com",
                "link_text": "Example link",
                "is_internal": False,
                "is_document": False,
                "scanned_at": "2026-04-03T00:00:00+00:00",
            }
        ]
    )
    mock_context = MagicMock()

    with patch("orchestrator.remote_tasks.website_content_health.open_dagster_pipes") as open_pipes_mock, patch(
        "orchestrator.remote_tasks.website_content_health._scan_site",
        return_value=(pages_df, link_refs_df),
    ) as scan_mock, patch(
        "orchestrator.remote_tasks.website_content_health.get_postgres_env_config",
        return_value={"user": "postgres", "host": "warehouse", "password": "pw", "database": "postgres", "port": 5432},
    ), patch(
        "orchestrator.remote_tasks.website_content_health.write_dataframe_to_table"
    ) as write_mock:
        open_pipes_mock.return_value.__enter__.return_value = mock_context

        main()

    scan_mock.assert_called_once()
    assert write_mock.call_count == 2
    mock_context.report_custom_message.assert_called_once()
    payload = mock_context.report_custom_message.call_args.args[0]
    assert payload["pages"]["page_count"] == 1
    assert payload["link_refs"]["link_ref_count"] == 1
