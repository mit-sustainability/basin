"""Smoke-check MIT Sustainability ingestion for website content health assets.

This script runs a single partition scan and verifies both multi-asset outputs
contain ingested rows.
"""

from __future__ import annotations

import os


def _set_default_env() -> None:
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    os.environ.setdefault("PG_WAREHOUSE_HOST", "localhost")
    os.environ.setdefault("PG_USER", "local")
    os.environ.setdefault("PG_PASSWORD", "local")


def main() -> int:
    _set_default_env()

    from orchestrator.assets.website_content_health import _scan_partition
    from orchestrator.resources.playwright import PlaywrightBrowserResource

    resource = PlaywrightBrowserResource(
        base_url="https://sustainability.mit.edu",
        browser_name="chromium",
        headless=True,
    )

    pages_df, links_df = _scan_partition("bucket_0", resource)

    print(f"pages_rows={len(pages_df)}")
    print(f"links_rows={len(links_df)}")

    if pages_df.empty:
        raise RuntimeError("No page rows ingested from bucket_0")
    if links_df.empty:
        raise RuntimeError("No link rows ingested from bucket_0")

    print("sample_page_url=", pages_df.iloc[0]["page_url"])
    print("sample_link_url=", links_df.iloc[0]["link_url"])
    print("Website content health ingestion smoke-check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
