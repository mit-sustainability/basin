from io import BytesIO

import dropbox
from dagster import ConfigurableResource, get_dagster_logger

logger = get_dagster_logger()


class DropboxResource(ConfigurableResource):
    """Dagster ConfigurableResource wrapping the Dropbox SDK.

    Configure via EnvVar("DROPBOX_ACCESS_TOKEN") in Definitions.
    """

    access_token: str

    def _list_files(self, folder_path: str, extensions: tuple[str, ...]) -> list[tuple[str, str]]:
        dbx = dropbox.Dropbox(oauth2_access_token=self.access_token)
        result = dbx.files_list_folder(folder_path)
        entries = list(result.entries)
        while result.has_more:
            result = dbx.files_list_folder_continue(result.cursor)
            entries.extend(result.entries)
        return [
            (e.name, e.path_lower)
            for e in entries
            if hasattr(e, "name") and e.name.lower().endswith(extensions)
        ]

    def list_excel_files(self, folder_path: str) -> list[tuple[str, str]]:
        """Return (filename, path) for all .xlsx/.xls files in folder_path."""
        files = self._list_files(folder_path, (".xlsx", ".xls"))
        logger.info(f"Found {len(files)} Excel files in {folder_path}")
        return files

    def list_sensor_files(self, folder_path: str) -> list[tuple[str, str]]:
        """Return (filename, path) for all .xlsx/.xls/.csv files in folder_path."""
        files = self._list_files(folder_path, (".xlsx", ".xls", ".csv"))
        logger.info(f"Found {len(files)} sensor files in {folder_path}")
        return files

    def download_file(self, path: str) -> BytesIO:
        """Download a file from Dropbox and return it as a BytesIO buffer."""
        dbx = dropbox.Dropbox(oauth2_access_token=self.access_token)
        _, response = dbx.files_download(path)
        try:
            return BytesIO(response.content)
        finally:
            response.close()
