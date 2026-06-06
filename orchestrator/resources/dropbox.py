import os
import re
from io import BytesIO

import dropbox
from dropbox.common import PathRoot
from dagster import ConfigurableResource, Failure, get_dagster_logger

logger = get_dagster_logger()

_NS_RE = re.compile(r"^ns:(\d+)(/.*)? *$")


class DropboxResource(ConfigurableResource):
    """Dagster resource wrapping the Dropbox SDK.

    Reads DROPBOX_REFRESH_TOKEN, DROPBOX_APP_KEY, DROPBOX_APP_SECRET from the
    environment. The SDK auto-refreshes the short-lived access token as needed.
    """

    def _client(self) -> dropbox.Dropbox:
        return dropbox.Dropbox(
            oauth2_refresh_token=os.environ["DROPBOX_REFRESH_TOKEN"],
            app_key=os.environ["DROPBOX_APP_KEY"],
            app_secret=os.environ["DROPBOX_APP_SECRET"],
        )

    def _resolve(self, folder_path: str) -> tuple[dropbox.Dropbox, str]:
        """Return (client, path) — routes ns: paths via with_path_root."""
        m = _NS_RE.match(folder_path.strip())
        if m:
            ns_id = m.group(1)          # ✅ Keep as string
            subpath = m.group(2) or "/"  # ✅ Default to root "/"
            return (
                self._client().with_path_root(PathRoot.namespace_id(ns_id)),
                subpath
            )
        return self._client(), folder_path

    def _list_files(self, folder_path: str, extensions: tuple[str, ...]) -> list[tuple[str, str]]:
        dbx, path = self._resolve(folder_path)
        # Preserve ns: prefix so download_file also routes through the same namespace
        m = _NS_RE.match(folder_path.strip())
        ns_prefix = f"ns:{m.group(1)}" if m else ""

        result = dbx.files_list_folder(path)
        entries = list(result.entries)
        while result.has_more:
            result = dbx.files_list_folder_continue(result.cursor)
            entries.extend(result.entries)
        return [
            (e.name, f"{ns_prefix}{e.path_lower}")
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
        dbx, resolved_path = self._resolve(path)
        _, response = dbx.files_download(resolved_path)
        try:
            return BytesIO(response.content)
        finally:
            response.close()
