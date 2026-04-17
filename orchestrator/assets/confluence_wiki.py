"""Confluence wiki extraction for the local RAG pipeline.

This module intentionally does not share HTML parsing code with the website
content health assets. The website scanner works against already-rendered public
HTML pages, while this pipeline receives Confluence's storage XHTML, which has
Confluence-specific tags such as ``ac:structured-macro`` and relative wiki
links. If BASIN grows another storage-XHTML ingestion path later, the right
extraction point would be a dedicated markup module rather than `assets/utils.py`.
"""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import md5
from html import unescape
from html.parser import HTMLParser
import json

from dagster import Output, ResourceParam, asset, get_dagster_logger
import pandas as pd
from sqlalchemy import text

from orchestrator.resources.confluence import ConfluencePage, ConfluenceResource
from orchestrator.resources.postgres_io_manager import PostgreConnResources


logger = get_dagster_logger()
CONFLUENCE_SNAPSHOT_COLUMNS = [
    "page_id",
    "title",
    "page_url",
    "author",
    "updated_at",
    "labels",
    "storage_xhtml",
    "markdown_content",
    "content_hash",
    "version",
    "synced_at",
]
FULLY_SKIPPED_MACRO_NAMES = {"toc"}
WRAPPER_BLOCK_TAGS = {"ac:layout", "ac:layout-section", "ac:layout-cell"}
BLOCK_TAGS = {"p", "div", "section", "article", "table", "tr"}
LIST_TAGS = {"ul", "ol"}
HEADING_TAGS = {"h1", "h2", "h3", "h4", "h5", "h6"}
INLINE_MARKERS = {
    "strong": "**",
    "b": "**",
    "em": "_",
    "i": "_",
    "code": "`",
}
BLOCK_PREFIXES = {
    "blockquote": "> ",
}


class ConfluenceStorageToMarkdown(HTMLParser):
    """Narrow converter from Confluence storage XHTML to markdown-like text.

    This is deliberately conservative. We keep `storage_xhtml` alongside the
    derived markdown in the raw snapshot so a later migration to a richer
    converter does not lose the original Confluence payload.
    """

    def __init__(self, base_url: str):
        super().__init__(convert_charrefs=True)
        self.base_url = base_url.rstrip("/")
        self.parts: list[str] = []
        self.list_stack: list[str] = []
        self.link_stack: list[str] = []
        self.skip_stack: list[str] = []

    def convert(self, storage_xhtml: str) -> str:
        self.parts = []
        self.list_stack = []
        self.link_stack = []
        self.skip_stack = []
        self.feed(storage_xhtml or "")
        self.close()

        markdown = "".join(self.parts)
        markdown = markdown.replace("\r", "")
        while "\n\n\n" in markdown:
            markdown = markdown.replace("\n\n\n", "\n\n")
        return markdown.strip()

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]):
        attr_map = dict(attrs)
        if tag == "ac:structured-macro" and attr_map.get("ac:name") in FULLY_SKIPPED_MACRO_NAMES:
            self.skip_stack.append(tag)
            return
        if self.skip_stack:
            return

        if tag in HEADING_TAGS:
            self._start_heading(tag)
            return
        if tag in WRAPPER_BLOCK_TAGS:
            self._ensure_block_break()
            return
        if tag in BLOCK_TAGS:
            self._ensure_block_break()
            return
        if tag in INLINE_MARKERS:
            self.parts.append(INLINE_MARKERS[tag])
            return
        if tag == "br":
            self.parts.append("\n")
            return
        if tag == "pre":
            self._ensure_block_break()
            self.parts.append("```\n")
            return
        if tag in LIST_TAGS:
            self.list_stack.append(tag)
            self._ensure_block_break()
            return
        if tag == "li":
            self._start_list_item()
            return
        if tag == "a":
            self._start_link(attr_map.get("href"))
            return
        if tag in BLOCK_PREFIXES:
            self._ensure_block_break()
            self.parts.append(BLOCK_PREFIXES[tag])

    def handle_endtag(self, tag: str):
        if self.skip_stack:
            if tag == self.skip_stack[-1]:
                self.skip_stack.pop()
            return

        if tag in HEADING_TAGS | BLOCK_TAGS | WRAPPER_BLOCK_TAGS | BLOCK_PREFIXES.keys():
            self._ensure_block_break()
            return
        if tag in INLINE_MARKERS:
            self.parts.append(INLINE_MARKERS[tag])
            return
        if tag == "pre":
            self.parts.append("\n```\n")
            self._ensure_block_break()
            return
        if tag in LIST_TAGS:
            if self.list_stack and self.list_stack[-1] == tag:
                self.list_stack.pop()
            self._ensure_block_break()
            return
        if tag == "a":
            href = self.link_stack.pop() if self.link_stack else ""
            self.parts.append(f"]({href})" if href else "]")

    def handle_data(self, data: str):
        if self.skip_stack:
            return
        text_value = unescape(data)
        if not text_value.strip():
            if self.parts and not self.parts[-1].endswith((" ", "\n")):
                self.parts.append(" ")
            return
        self.parts.append(text_value)

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]):
        self.handle_starttag(tag, attrs)
        self.handle_endtag(tag)

    def _normalize_href(self, href: str | None) -> str | None:
        if not href:
            return None
        href = href.strip()
        if not href:
            return None
        if href.startswith(("http://", "https://", "mailto:", "#")):
            return href
        if href.startswith("/"):
            return f"{self.base_url}{href}"
        return f"{self.base_url}/{href.lstrip('/')}"

    def _start_heading(self, tag: str):
        level = int(tag[1])
        self._ensure_block_break()
        self.parts.append("#" * level + " ")

    def _start_list_item(self):
        indent = "  " * max(len(self.list_stack) - 1, 0)
        bullet = "1. " if self.list_stack and self.list_stack[-1] == "ol" else "- "
        self.parts.append(f"\n{indent}{bullet}")

    def _start_link(self, href: str | None):
        self.parts.append("[")
        self.link_stack.append(self._normalize_href(href) or "")

    def _ensure_block_break(self):
        if not self.parts:
            return
        if self.parts[-1].endswith("\n\n"):
            return
        if self.parts[-1].endswith("\n"):
            self.parts.append("\n")
            return
        self.parts.append("\n\n")


def _load_existing_snapshot(pg_engine: PostgreConnResources, table_name: str) -> pd.DataFrame:
    """Read the current raw snapshot using Dagster's injected Postgres resource."""

    engine = pg_engine.create_engine()
    with engine.connect() as conn:
        exists = conn.execute(
            text(
                "SELECT EXISTS ("
                "SELECT FROM information_schema.tables "
                "WHERE table_schema = 'raw' AND table_name = :table_name)"
            ),
            {"table_name": table_name},
        ).scalar()
        if not exists:
            return pd.DataFrame(columns=CONFLUENCE_SNAPSHOT_COLUMNS)
        return pd.read_sql(f"SELECT * FROM raw.{table_name}", con=conn)


def _page_requires_refresh(page: ConfluencePage, existing_row: pd.Series | None) -> bool:
    if existing_row is None:
        return True

    existing_version = int(existing_row.get("version") or 0)
    if page.version > existing_version:
        return True

    incoming_updated = pd.to_datetime(page.updated_at, utc=True, errors="coerce")
    existing_updated = pd.to_datetime(existing_row.get("updated_at"), utc=True, errors="coerce")
    if pd.isna(incoming_updated):
        return False
    if pd.isna(existing_updated):
        return True
    return incoming_updated > existing_updated


def _build_snapshot_row(
    page: ConfluencePage,
    converter: ConfluenceStorageToMarkdown,
    synced_at: datetime,
) -> dict[str, object]:
    markdown_content = converter.convert(page.storage_xhtml)
    return {
        "page_id": page.page_id,
        "title": page.title,
        "page_url": page.page_url,
        "author": page.author,
        "updated_at": page.updated_at,
        "labels": json.dumps(page.labels),
        "storage_xhtml": page.storage_xhtml,
        "markdown_content": markdown_content,
        "content_hash": md5(markdown_content.encode("utf-8")).hexdigest(),
        "version": page.version,
        "synced_at": synced_at.isoformat(),
    }


def build_confluence_snapshot(
    pages: list[ConfluencePage],
    existing_snapshot: pd.DataFrame,
    base_url: str,
    synced_at: datetime | None = None,
) -> tuple[pd.DataFrame, dict[str, int]]:
    synced_at = synced_at or datetime.now(timezone.utc)
    converter = ConfluenceStorageToMarkdown(base_url=base_url)
    existing_by_id: dict[str, pd.Series] = {}
    if not existing_snapshot.empty:
        indexed_snapshot = existing_snapshot.copy()
        indexed_snapshot["page_id"] = indexed_snapshot["page_id"].astype(str)
        existing_by_id = {row["page_id"]: row for _, row in indexed_snapshot.iterrows()}

    rows: list[dict[str, object]] = []
    refreshed_pages = 0
    reused_pages = 0

    for page in pages:
        existing_row = existing_by_id.get(page.page_id)

        if _page_requires_refresh(page, existing_row):
            rows.append(_build_snapshot_row(page=page, converter=converter, synced_at=synced_at))
            refreshed_pages += 1
        else:
            rows.append(existing_row[CONFLUENCE_SNAPSHOT_COLUMNS].to_dict())
            reused_pages += 1

    snapshot = pd.DataFrame(rows, columns=CONFLUENCE_SNAPSHOT_COLUMNS)
    if not snapshot.empty:
        snapshot["page_id"] = snapshot["page_id"].astype(str)
        snapshot["version"] = snapshot["version"].astype(int)
        snapshot = snapshot.sort_values(["title", "page_id"]).reset_index(drop=True)

    metadata = {
        "total_pages": len(snapshot.index),
        "refreshed_pages": refreshed_pages,
        "reused_pages": reused_pages,
    }
    return snapshot, metadata


@asset(
    name="confluence_mitos_pages",
    key_prefix="raw",
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def confluence_mitos_pages(
    confluence: ResourceParam[ConfluenceResource],
    pg_engine: PostgreConnResources,
) -> Output[pd.DataFrame]:
    """Build the latest Confluence snapshot from two separately injected resources."""

    existing_snapshot = _load_existing_snapshot(pg_engine=pg_engine, table_name="confluence_mitos_pages")
    pages = confluence.list_pages()
    snapshot, metadata = build_confluence_snapshot(
        pages=pages,
        existing_snapshot=existing_snapshot,
        base_url=confluence.base_url,
    )
    logger.info(
        "Prepared %s Confluence pages for raw snapshot (%s refreshed, %s reused)",
        metadata["total_pages"],
        metadata["refreshed_pages"],
        metadata["reused_pages"],
    )
    return Output(snapshot, metadata=metadata)
