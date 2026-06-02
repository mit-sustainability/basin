from io import BytesIO
from unittest.mock import MagicMock, patch

import dropbox.exceptions
import pytest

from orchestrator.resources.dropbox import DropboxResource


def _make_file_entry(name, path):
    entry = MagicMock()
    entry.name = name
    entry.path_lower = path
    return entry


def test_list_excel_files_returns_xlsx_files():
    resource = DropboxResource(access_token="fake-token")
    mock_result = MagicMock()
    mock_result.entries = [
        _make_file_entry("sensor1.xlsx", "/folder/sensor1.xlsx"),
        _make_file_entry("notes.txt", "/folder/notes.txt"),
        _make_file_entry("sensor2.xls", "/folder/sensor2.xls"),
    ]
    mock_result.has_more = False

    with patch("orchestrator.resources.dropbox.dropbox.Dropbox") as mock_cls:
        mock_cls.return_value.files_list_folder.return_value = mock_result
        files = resource.list_excel_files("/folder")

    assert len(files) == 2
    assert files[0] == ("sensor1.xlsx", "/folder/sensor1.xlsx")
    assert files[1] == ("sensor2.xls", "/folder/sensor2.xls")


def test_download_file_returns_bytes_io():
    resource = DropboxResource(access_token="fake-token")
    mock_response = MagicMock()
    mock_response.content = b"fake excel content"

    with patch("orchestrator.resources.dropbox.dropbox.Dropbox") as mock_cls:
        mock_cls.return_value.files_download.return_value = (MagicMock(), mock_response)
        result = resource.download_file("/folder/sensor1.xlsx")

    assert isinstance(result, BytesIO)
    assert result.read() == b"fake excel content"
    mock_response.close.assert_called_once()


def test_list_excel_files_paginates_when_has_more():
    resource = DropboxResource(access_token="fake-token")

    first_result = MagicMock()
    first_result.entries = [_make_file_entry("page1.xlsx", "/folder/page1.xlsx")]
    first_result.has_more = True
    first_result.cursor = "cursor-abc"

    second_result = MagicMock()
    second_result.entries = [_make_file_entry("page2.xlsx", "/folder/page2.xlsx")]
    second_result.has_more = False

    with patch("orchestrator.resources.dropbox.dropbox.Dropbox") as mock_cls:
        mock_dbx = mock_cls.return_value
        mock_dbx.files_list_folder.return_value = first_result
        mock_dbx.files_list_folder_continue.return_value = second_result
        files = resource.list_excel_files("/folder")

    mock_dbx.files_list_folder_continue.assert_called_once_with("cursor-abc")
    assert len(files) == 2
    assert files[0] == ("page1.xlsx", "/folder/page1.xlsx")
    assert files[1] == ("page2.xlsx", "/folder/page2.xlsx")


def test_list_excel_files_propagates_api_error():
    resource = DropboxResource(access_token="fake-token")
    api_error = dropbox.exceptions.ApiError(
        request_id="req-1",
        error=MagicMock(),
        user_message_text="not found",
        user_message_locale="en",
    )

    with patch("orchestrator.resources.dropbox.dropbox.Dropbox") as mock_cls:
        mock_cls.return_value.files_list_folder.side_effect = api_error
        with pytest.raises(dropbox.exceptions.ApiError):
            resource.list_excel_files("/nonexistent")
