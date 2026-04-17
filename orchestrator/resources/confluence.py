from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests
from dagster import ConfigurableResource, Failure, get_dagster_logger


logger = get_dagster_logger()
DEFAULT_EXPAND = "body.storage,history,metadata.labels,version"


@dataclass(frozen=True)
class ConfluencePage:
    page_id: str
    title: str
    page_url: str
    author: str | None
    updated_at: str | None
    labels: list[str]
    storage_xhtml: str
    version: int


class ConfluenceResource(ConfigurableResource):
    """Minimal Confluence REST client for PAT-authenticated page snapshots."""

    base_url: str = "https://wikis.mit.edu/confluence"
    auth_token: str
    space_key: str = "MITOS"
    page_limit: int = 100
    timeout_seconds: int = 30

    def _validate_configuration(self) -> None:
        missing_env_vars = [
            env_var
            for field_value, env_var in (
                (self.base_url, "CONFLUENCE_BASE_URL"),
                (self.auth_token, "CONFLUENCE_PAT"),
                (self.space_key, "CONFLUENCE_SPACE_KEY"),
            )
            if not (field_value or "").strip()
        ]
        if missing_env_vars:
            missing_vars = ", ".join(missing_env_vars)
            raise Failure(
                "Confluence configuration is incomplete. " f"Set the required environment variable(s): {missing_vars}."
            )

    def _headers(self) -> dict[str, str]:
        """Confluence PATs are sent as bearer tokens."""

        return {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.auth_token}",
        }

    def _request_json(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        self._validate_configuration()
        url = f"{self.base_url.rstrip('/')}{path}"
        try:
            response = requests.get(
                url,
                headers=self._headers(),
                params=params,
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            raise Failure(f"Confluence request failed for {url}: {exc}") from exc
        return response.json()

    def _normalize_page(self, payload: dict[str, Any]) -> ConfluencePage:
        links = payload.get("_links", {})
        page_path = links.get("webui") or f"/pages/viewpage.action?pageId={payload['id']}"
        if page_path.startswith("/"):
            page_url = f"{self.base_url.rstrip('/')}{page_path}"
        else:
            page_url = f"{self.base_url.rstrip('/')}/{page_path.lstrip('/')}"

        history = payload.get("history") or {}
        version = payload.get("version") or {}
        labels = payload.get("metadata", {}).get("labels", {}).get("results", [])

        author = history.get("createdBy", {}).get("displayName") or version.get("by", {}).get("displayName")
        updated_at = version.get("when") or history.get("lastUpdated", {}).get("when")
        storage_xhtml = payload.get("body", {}).get("storage", {}).get("value", "") or ""

        return ConfluencePage(
            page_id=str(payload["id"]),
            title=payload.get("title", ""),
            page_url=page_url,
            author=author,
            updated_at=updated_at,
            labels=sorted({label.get("name", "") for label in labels if label.get("name")}),
            storage_xhtml=storage_xhtml,
            version=int(version.get("number") or 0),
        )

    def list_pages(self, expand: str = DEFAULT_EXPAND) -> list[ConfluencePage]:
        """Page through the configured space and flatten all results into one list."""

        pages: list[ConfluencePage] = []
        start = 0

        while True:
            payload = self._request_json(
                "/rest/api/content",
                {
                    "spaceKey": self.space_key,
                    "type": "page",
                    "expand": expand,
                    "start": start,
                    "limit": self.page_limit,
                },
            )
            results = payload.get("results", [])
            if not results:
                break

            pages.extend(self._normalize_page(result) for result in results)
            logger.info("Fetched %s Confluence pages from %s starting at %s", len(results), self.space_key, start)

            if len(results) < self.page_limit:
                break
            start += len(results)

        return pages
