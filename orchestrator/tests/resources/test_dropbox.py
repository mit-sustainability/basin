from io import BytesIO
from unittest.mock import MagicMock, patch

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
