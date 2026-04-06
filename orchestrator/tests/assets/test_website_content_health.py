import pandas as pd

from orchestrator.assets.website_content_health import (
    ALLOWED_SECTION_ROOTS,
    BASE_URL,
    LINK_COLUMNS,
    PAGE_COLUMNS,
    SCAN_SCOPE,
    SECTION_SEED_URLS,
    UNIQUE_LINK_COLUMNS,
    _failed_page_snapshot,
    _canonicalize_crawl_url,
    _build_links_df,
    _build_unique_links_df,
    _classify_link_health,
    _crawl_internal_urls,
    _derive_category_topic,
    _is_internal_url,
    _is_allowed_section_url,
    _is_document_url,
    _check_link_health,
    _resolve_last_update,
    _should_crawl_url,
    _should_track_link_url,
    _validate_unique_links,
)


def test_derive_category_topic():
    assert _derive_category_topic(f"{BASE_URL}/") == ("home", None)
    assert _derive_category_topic(f"{BASE_URL}/resources/blog") == ("resources", "blog")


def test_resolve_last_update_prefers_parseable_meta_value():
    result = _resolve_last_update(
        meta_values=["2025-03-14T10:15:00Z", None],
        time_values=["invalid"],
        header_value="Wed, 15 Mar 2025 12:00:00 GMT",
    )

    assert result == pd.Timestamp("2025-03-14T10:15:00Z").isoformat()


def test_classify_link_health_without_status_is_unknown():
    assert _classify_link_health(None, None, is_internal=False) == "unknown"
    assert _classify_link_health(None, None, is_internal=True) == "unknown"


def test_classify_link_health_uses_status_families_not_all_4xx_as_broken():
    assert _classify_link_health(403, None, is_internal=True) == "restricted"
    assert _classify_link_health(404, None, is_internal=True) == "broken"
    assert _classify_link_health(429, None, is_internal=False) == "rate_limited"
    assert _classify_link_health(503, None, is_internal=False) == "server_error"
    assert _classify_link_health(999, None, is_internal=False) == "blocked"


def test_classify_link_health_kerberos_style_disconnect_is_restricted_for_internal():
    error = "('Connection aborted.', RemoteDisconnected('Remote end closed connection without response'))"
    assert _classify_link_health(None, error, is_internal=True) == "restricted"
    assert _classify_link_health(None, error, is_internal=False) == "blocked"


def test_failed_page_snapshot_marks_page_unreachable():
    snapshot = _failed_page_snapshot(f"{BASE_URL}/bad-page")

    assert snapshot.page_url == f"{BASE_URL}/bad-page"
    assert snapshot.page_health == "unreachable"
    assert snapshot.links == []
    assert snapshot.content_text == ""


def test_canonicalize_crawl_url_drops_query_and_fragment():
    assert (
        _canonicalize_crawl_url(f"{BASE_URL}/resources/blog?utm_source=test#overview") == f"{BASE_URL}/resources/blog"
    )


def test_is_allowed_section_url_only_accepts_owned_sections():
    assert _is_allowed_section_url(f"{BASE_URL}/resources/toolkit") is True
    assert _is_allowed_section_url(f"{BASE_URL}/climate-action/project") is True
    assert _is_allowed_section_url(f"{BASE_URL}/news/story") is False
    assert _is_allowed_section_url(f"{BASE_URL}/events/calendar") is False
    assert _is_allowed_section_url(BASE_URL) is False


def test_should_crawl_url_only_accepts_internal_html_pages():
    base_domain = "sustainability.mit.edu"

    assert _is_internal_url(f"{BASE_URL}/resources/blog", base_domain) is True
    assert _should_crawl_url(f"{BASE_URL}/resources/blog", base_domain) is True
    assert _should_crawl_url(f"{BASE_URL}/about-us/team", base_domain) is True
    assert _should_crawl_url(f"{BASE_URL}/news/story", base_domain) is False
    assert _should_crawl_url(f"{BASE_URL}/events/calendar", base_domain) is False
    assert _should_crawl_url(f"{BASE_URL}/files/report.pdf", base_domain) is False
    assert _should_crawl_url("https://example.com/resources/blog", base_domain) is False


def test_should_track_link_url_keeps_external_and_owned_internal_links():
    base_domain = "sustainability.mit.edu"

    assert _should_track_link_url(f"{BASE_URL}/partners/case-study", base_domain) is True
    assert _should_track_link_url(f"{BASE_URL}/news/story", base_domain) is False
    assert _should_track_link_url(f"{BASE_URL}/sites/default/files/report.pdf", base_domain) is True
    assert _should_track_link_url("https://example.com/resource", base_domain) is True


def test_document_urls_are_detected_without_affecting_crawl_scope():
    assert _is_document_url(f"{BASE_URL}/sites/default/files/report.pdf") is True
    assert _is_document_url("https://example.com/files/spreadsheet.xlsx") is True
    assert _is_document_url(f"{BASE_URL}/about-us/team") is False


def test_check_link_health_validates_generic_external_links():
    from unittest.mock import Mock, patch

    response = Mock(status_code=200, url="https://example.com/resource")

    with patch("orchestrator.assets.website_content_health.requests.head", return_value=response) as head_mock:
        result = _check_link_health(
            "https://example.com/resource",
            base_domain="sustainability.mit.edu",
        )

    assert result == (
        "healthy",
        200,
        "https://example.com/resource",
        None,
    )
    head_mock.assert_called_once()


def test_check_link_health_validates_google_docs_links():
    from unittest.mock import Mock, patch

    response = Mock(status_code=200, url="https://docs.google.com/document/d/abc123/edit")

    with patch("orchestrator.assets.website_content_health.requests.head", return_value=response) as head_mock:
        result = _check_link_health(
            "https://docs.google.com/document/d/abc123/edit",
            base_domain="sustainability.mit.edu",
        )

    assert result == (
        "healthy",
        200,
        "https://docs.google.com/document/d/abc123/edit",
        None,
    )
    head_mock.assert_called_once()


def test_check_link_health_retries_ambiguous_head_with_get():
    from unittest.mock import Mock, patch

    head_response = Mock(status_code=403, url="https://example.com/protected")
    get_response = Mock(status_code=200, url="https://example.com/protected")

    with patch(
        "orchestrator.assets.website_content_health.requests.head",
        return_value=head_response,
    ) as head_mock, patch(
        "orchestrator.assets.website_content_health.requests.get",
        return_value=get_response,
    ) as get_mock:
        result = _check_link_health(
            "https://example.com/protected",
            base_domain="sustainability.mit.edu",
        )

    assert result == ("healthy", 200, "https://example.com/protected", None)
    head_mock.assert_called_once()
    get_mock.assert_called_once()


def test_section_seed_urls_match_allowed_section_roots():
    assert sorted(url.rsplit("/", 1)[-1] for url in SECTION_SEED_URLS) == sorted(ALLOWED_SECTION_ROOTS)


def test_crawl_internal_urls_recursively_discovers_same_site_pages():
    snapshots = {
        f"{BASE_URL}/resources": type(
            "Snapshot",
            (),
            {
                "links": [
                    {"link_url": f"{BASE_URL}/resources/blog", "link_text": "Blog"},
                    {"link_url": f"{BASE_URL}/about-us/team", "link_text": "Team"},
                    {"link_url": f"{BASE_URL}/news/story", "link_text": "News"},
                    {"link_url": f"{BASE_URL}/files/report.pdf", "link_text": "PDF"},
                    {"link_url": "https://example.com/offsite", "link_text": "Offsite"},
                ]
            },
        )(),
        f"{BASE_URL}/resources/blog": type(
            "Snapshot",
            (),
            {
                "links": [
                    {"link_url": f"{BASE_URL}/about-us/team?utm_source=test", "link_text": "Team"},
                ]
            },
        )(),
        f"{BASE_URL}/about-us/team": type(
            "Snapshot",
            (),
            {"links": []},
        )(),
    }

    def fake_scrape(_browser_context, page_url: str):
        return snapshots[page_url]

    from unittest.mock import patch

    with patch("orchestrator.assets.website_content_health._scrape_page_snapshot", side_effect=fake_scrape):
        urls = _crawl_internal_urls(object(), [f"{BASE_URL}/resources"])

    assert urls == [
        f"{BASE_URL}/about-us/team",
        f"{BASE_URL}/resources",
        f"{BASE_URL}/resources/blog",
    ]


def test_scrape_page_snapshot_returns_unreachable_snapshot_when_navigation_fails():
    from unittest.mock import MagicMock

    from orchestrator.assets.website_content_health import _scrape_page_snapshot

    page = MagicMock()
    page.goto.side_effect = Exception("ERR_TOO_MANY_REDIRECTS")
    browser_context = MagicMock()
    browser_context.new_page.return_value = page

    snapshot = _scrape_page_snapshot(browser_context, f"{BASE_URL}/redirect-loop")

    assert snapshot.page_url == f"{BASE_URL}/redirect-loop"
    assert snapshot.page_health == "unreachable"
    assert snapshot.links == []
    page.close.assert_called_once()


def test_validate_unique_links_checks_each_url_once():
    from unittest.mock import patch

    checked_urls: list[str] = []

    def fake_check(link_url: str, base_domain: str):
        checked_urls.append(link_url)
        return ("healthy", 200, link_url, None)

    with patch("orchestrator.assets.website_content_health._check_link_health", side_effect=fake_check):
        results = _validate_unique_links(
            [f"{BASE_URL}/about-us", f"{BASE_URL}/about-us", f"{BASE_URL}/resources/blog"],
            base_domain="sustainability.mit.edu",
        )

    assert sorted(checked_urls) == sorted([f"{BASE_URL}/about-us", f"{BASE_URL}/resources/blog"])
    assert results[f"{BASE_URL}/about-us"] == ("healthy", 200, f"{BASE_URL}/about-us", None)


def test_build_unique_links_df_validates_deduped_link_refs():
    from unittest.mock import patch

    link_refs_df = pd.DataFrame(
        [
            {
                "source_page_url": f"{BASE_URL}/about-us",
                "link_url": "https://example.com/resource",
                "link_text": "Example",
                "is_internal": False,
                "is_document": False,
                "scanned_at": "2026-03-24T00:00:00+00:00",
            },
            {
                "source_page_url": f"{BASE_URL}/resources/toolkit",
                "link_url": "https://example.com/resource",
                "link_text": "Example again",
                "is_internal": False,
                "is_document": False,
                "scanned_at": "2026-03-24T00:00:00+00:00",
            },
        ]
    )

    with patch(
        "orchestrator.assets.website_content_health._validate_unique_links",
        return_value={"https://example.com/resource": ("healthy", 200, "https://example.com/resource", None)},
    ) as validate_mock:
        unique_links_df = _build_unique_links_df(link_refs_df)

    assert list(unique_links_df["link_url"]) == ["https://example.com/resource"]
    assert int(unique_links_df.loc[0, "source_page_count"]) == 2
    validate_mock.assert_called_once()


def test_build_links_df_joins_current_validation_results():
    link_refs_df = pd.DataFrame(
        [
            {
                "source_page_url": f"{BASE_URL}/about-us",
                "link_url": "https://example.com/resource",
                "link_text": "Example",
                "is_internal": False,
                "is_document": False,
                "scanned_at": "2026-03-24T00:00:00+00:00",
            }
        ]
    )
    unique_links_df = pd.DataFrame(
        [
            {
                "link_url": "https://example.com/resource",
                "is_internal": False,
                "is_document": False,
                "health": "healthy",
                "status_code": 200,
                "final_url": "https://example.com/resource",
                "error": None,
                "source_page_count": 1,
                "scanned_at": "2026-03-25T00:00:00+00:00",
            }
        ]
    )

    links_df = _build_links_df(link_refs_df, unique_links_df)

    assert list(links_df.columns) == LINK_COLUMNS
    assert links_df.loc[0, "health"] == "healthy"
    assert links_df.loc[0, "scanned_at"] == "2026-03-25T00:00:00+00:00"


def test_scan_scope_is_full_site():
    assert SCAN_SCOPE == "full_site"


def test_output_columns_drop_scan_partition_and_mark_documents():
    assert "scan_partition" not in PAGE_COLUMNS
    assert "scan_partition" not in LINK_COLUMNS
    assert "scan_partition" not in UNIQUE_LINK_COLUMNS
    assert "is_document" in LINK_COLUMNS
    assert "is_document" in UNIQUE_LINK_COLUMNS
