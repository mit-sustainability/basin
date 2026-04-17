from datetime import datetime, timezone

import pandas as pd

from orchestrator.assets.confluence_wiki import (
    CONFLUENCE_SNAPSHOT_COLUMNS,
    ConfluenceStorageToMarkdown,
    build_confluence_snapshot,
)
from orchestrator.resources.confluence import ConfluencePage


def test_storage_to_markdown_strips_macros_and_rewrites_links():
    converter = ConfluenceStorageToMarkdown(base_url="https://wikis.mit.edu/confluence")

    markdown = converter.convert(
        """
        <h1>Overview</h1>
        <p>Read the <a href="/display/MITOS/Runbook">runbook</a>.</p>
        <ac:structured-macro ac:name="toc"><ac:parameter ac:name="printable">true</ac:parameter></ac:structured-macro>
        <ul><li>Alpha</li><li>Beta</li></ul>
        """
    )

    assert "Overview" in markdown
    assert "[runbook](https://wikis.mit.edu/confluence/display/MITOS/Runbook)" in markdown
    assert "printable" not in markdown
    assert "- Alpha" in markdown


def test_storage_to_markdown_keeps_text_inside_non_toc_macros_and_layout_wrappers():
    converter = ConfluenceStorageToMarkdown(base_url="https://wikis.mit.edu/confluence")

    markdown = converter.convert(
        """
        <ac:layout>
          <ac:layout-section>
            <ac:layout-cell>
              <p>Panel intro</p>
              <ac:structured-macro ac:name="expand">
                <ac:parameter ac:name="title">Details</ac:parameter>
                <ac:rich-text-body><p>Hidden body</p></ac:rich-text-body>
              </ac:structured-macro>
            </ac:layout-cell>
          </ac:layout-section>
        </ac:layout>
        """
    )

    assert "Panel intro" in markdown
    assert "Details" in markdown
    assert "Hidden body" in markdown


def test_build_confluence_snapshot_refreshes_changed_pages_and_reuses_existing_rows():
    synced_at = datetime(2026, 4, 2, tzinfo=timezone.utc)
    pages = [
        ConfluencePage(
            page_id="1",
            title="Home",
            page_url="https://wikis.mit.edu/confluence/display/MITOS/Home",
            author="Ada",
            updated_at="2026-04-01T00:00:00.000Z",
            labels=["alpha"],
            storage_xhtml="<p>Updated body</p>",
            version=2,
        ),
        ConfluencePage(
            page_id="2",
            title="Runbook",
            page_url="https://wikis.mit.edu/confluence/display/MITOS/Runbook",
            author="Ben",
            updated_at="2026-04-01T00:00:00.000Z",
            labels=["ops"],
            storage_xhtml="<p>Same body</p>",
            version=1,
        ),
    ]
    existing_snapshot = pd.DataFrame(
        [
            {
                "page_id": "1",
                "title": "Home",
                "page_url": "https://wikis.mit.edu/confluence/display/MITOS/Home",
                "author": "Ada",
                "updated_at": "2026-03-01T00:00:00.000Z",
                "labels": '["alpha"]',
                "storage_xhtml": "<p>Old body</p>",
                "markdown_content": "Old body",
                "content_hash": "old-hash",
                "version": 1,
                "synced_at": "2026-03-01T00:00:00+00:00",
            },
            {
                "page_id": "2",
                "title": "Runbook",
                "page_url": "https://wikis.mit.edu/confluence/display/MITOS/Runbook",
                "author": "Ben",
                "updated_at": "2026-04-01T00:00:00.000Z",
                "labels": '["ops"]',
                "storage_xhtml": "<p>Same body</p>",
                "markdown_content": "Same body",
                "content_hash": "same-hash",
                "version": 1,
                "synced_at": "2026-04-01T00:00:00+00:00",
            },
        ]
    )

    snapshot, metadata = build_confluence_snapshot(
        pages=pages,
        existing_snapshot=existing_snapshot,
        base_url="https://wikis.mit.edu/confluence",
        synced_at=synced_at,
    )

    assert list(snapshot.columns) == CONFLUENCE_SNAPSHOT_COLUMNS
    assert metadata == {"total_pages": 2, "refreshed_pages": 1, "reused_pages": 1}
    refreshed = snapshot[snapshot["page_id"] == "1"].iloc[0]
    reused = snapshot[snapshot["page_id"] == "2"].iloc[0]
    assert refreshed["version"] == 2
    assert refreshed["markdown_content"] == "Updated body"
    assert reused["content_hash"] == "same-hash"
    assert reused["synced_at"] == "2026-04-01T00:00:00+00:00"


def test_build_confluence_snapshot_includes_new_pages():
    pages = [
        ConfluencePage(
            page_id="10",
            title="New Page",
            page_url="https://wikis.mit.edu/confluence/display/MITOS/New+Page",
            author="Ada",
            updated_at="2026-04-02T00:00:00.000Z",
            labels=[],
            storage_xhtml="<p>Hello</p>",
            version=1,
        )
    ]

    snapshot, metadata = build_confluence_snapshot(
        pages=pages,
        existing_snapshot=pd.DataFrame(columns=CONFLUENCE_SNAPSHOT_COLUMNS),
        base_url="https://wikis.mit.edu/confluence",
    )

    assert metadata["refreshed_pages"] == 1
    assert snapshot.iloc[0]["markdown_content"] == "Hello"
