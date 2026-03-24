from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from collections import deque
from datetime import datetime, timezone
from hashlib import md5
from typing import Iterable
from urllib.parse import urljoin, urlparse

from dagster import (
    AssetOut,
    MetadataValue,
    Output,
    ResourceParam,
    asset,
    get_dagster_logger,
    multi_asset,
)
import pandas as pd
import requests

from orchestrator.resources.playwright import PlaywrightBrowserResource


BASE_URL = "https://sustainability.mit.edu"
ALLOWED_SECTION_ROOTS = {
    "climate-action",
    "metrics-goals",
    "resources",
    "partners",
    "about-us",
    "article",
}
SECTION_SEED_URLS = [f"{BASE_URL}/{section}" for section in sorted(ALLOWED_SECTION_ROOTS)]
REQUEST_TIMEOUT_SECONDS = 20
PLAYWRIGHT_TIMEOUT_MS = 20_000
PROGRESS_LOG_EVERY = 10
MAX_LINK_CHECK_WORKERS = 12
SCAN_SCOPE = "full_site"
DOCUMENT_SUFFIXES = {
    ".pdf",
    ".csv",
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".svg",
    ".zip",
    ".doc",
    ".docx",
    ".ppt",
    ".pptx",
    ".xls",
    ".xlsx",
}
SKIPPED_CRAWL_SUFFIXES = DOCUMENT_SUFFIXES
PAGE_COLUMNS = [
    "page_url",
    "page_path",
    "page_title",
    "category",
    "topic",
    "last_update",
    "http_status",
    "page_health",
    "meta_description",
    "content_text",
    "content_hash",
    "link_count",
    "scanned_at",
]
LINK_COLUMNS = [
    "source_page_url",
    "link_url",
    "link_text",
    "is_internal",
    "is_document",
    "health",
    "status_code",
    "final_url",
    "error",
    "scanned_at",
]
LINK_REF_COLUMNS = [
    "source_page_url",
    "link_url",
    "link_text",
    "is_internal",
    "is_document",
    "scanned_at",
]
UNIQUE_LINK_COLUMNS = [
    "link_url",
    "is_internal",
    "is_document",
    "health",
    "status_code",
    "final_url",
    "error",
    "source_page_count",
    "scanned_at",
]
logger = get_dagster_logger()


@dataclass(frozen=True)
class PageSnapshot:
    page_url: str
    page_title: str
    category: str
    topic: str | None
    last_update: str | None
    http_status: int | None
    page_health: str
    meta_description: str | None
    content_text: str
    content_hash: str
    links: list[dict[str, str | None]]


def _derive_category_topic(page_url: str) -> tuple[str, str | None]:
    path_parts = [part for part in urlparse(page_url).path.split("/") if part]
    if not path_parts:
        return "home", None
    category = path_parts[0]
    topic = path_parts[1] if len(path_parts) > 1 else None
    return category, topic


def _normalize_link(source_url: str, href: str | None) -> str | None:
    if not href:
        return None
    href = href.strip()
    if not href or href.startswith("#"):
        return None
    normalized = urljoin(source_url, href)
    parsed = urlparse(normalized)
    if parsed.scheme not in {"http", "https"}:
        return normalized
    return parsed._replace(fragment="").geturl().rstrip("/")


def _is_internal_url(url: str, base_domain: str) -> bool:
    parsed = urlparse(url)
    return parsed.scheme in {"http", "https"} and parsed.netloc == base_domain


def _is_allowed_section_url(url: str) -> bool:
    path_parts = [part for part in urlparse(url).path.split("/") if part]
    if not path_parts:
        return False
    return path_parts[0] in ALLOWED_SECTION_ROOTS


def _is_document_url(url: str) -> bool:
    path = urlparse(url).path.lower()
    return any(path.endswith(suffix) for suffix in DOCUMENT_SUFFIXES)


def _should_validate_link_url(url: str, base_domain: str) -> bool:
    parsed = urlparse(url)
    return parsed.scheme in {"http", "https"}


def _should_track_link_url(url: str, base_domain: str) -> bool:
    if not url:
        return False
    if _is_internal_url(url, base_domain):
        return _is_allowed_section_url(url) or _is_document_url(url)
    return True


def _canonicalize_crawl_url(url: str) -> str:
    parsed = urlparse(url)
    canonical = parsed._replace(fragment="", query="")
    normalized = canonical.geturl().rstrip("/")
    return normalized or BASE_URL


def _should_crawl_url(url: str, base_domain: str) -> bool:
    if not _is_internal_url(url, base_domain):
        return False
    path = urlparse(url).path.lower()
    if any(path.endswith(suffix) for suffix in SKIPPED_CRAWL_SUFFIXES):
        return False
    return _is_allowed_section_url(url)


def _candidate_last_updates(meta_values: Iterable[str | None], time_values: Iterable[str | None]) -> list[str]:
    candidates: list[str] = []
    for value in [*meta_values, *time_values]:
        if value:
            cleaned = value.strip()
            if cleaned:
                candidates.append(cleaned)
    return candidates


def _resolve_last_update(
    meta_values: Iterable[str | None],
    time_values: Iterable[str | None],
    header_value: str | None,
) -> str | None:
    for candidate in _candidate_last_updates(meta_values, time_values):
        try:
            return pd.to_datetime(candidate, utc=True).isoformat()
        except (TypeError, ValueError):
            continue
    if header_value:
        try:
            return pd.to_datetime(header_value, utc=True).isoformat()
        except (TypeError, ValueError):
            return None
    return None


def _classify_link_health(status_code: int | None, error: str | None, is_internal: bool) -> str:
    if error:
        error_message = error.lower()
        if "too many requests" in error_message or "rate limit" in error_message:
            return "rate_limited"
        if any(
            marker in error_message
            for marker in (
                "kerberos",
                "unauthorized",
                "forbidden",
                "authentication",
                "login",
                "remote end closed connection without response",
                "remotedisconnected",
            )
        ):
            return "restricted" if is_internal else "blocked"
        return "unreachable"
    if status_code is None:
        return "unknown"
    if 200 <= status_code < 300:
        return "healthy"
    if 300 <= status_code < 400:
        return "redirect"
    if status_code in {401, 403}:
        return "restricted"
    if status_code in {404, 410}:
        return "broken"
    if status_code == 429:
        return "rate_limited"
    if status_code in {500, 502, 503, 504}:
        return "server_error"
    if status_code in {400, 406, 999}:
        return "blocked"
    if status_code >= 400:
        return "unknown"
    return "unknown"


def _check_link_health(link_url: str, base_domain: str) -> tuple[str, int | None, str | None, str | None]:
    parsed = urlparse(link_url)
    if parsed.scheme not in {"http", "https"}:
        return "non_http", None, None, None
    is_internal = parsed.netloc == base_domain

    response = None
    try:
        response = requests.head(link_url, allow_redirects=True, timeout=REQUEST_TIMEOUT_SECONDS)
        if response.status_code in {400, 401, 403, 405, 406, 429, 999}:
            response = requests.get(link_url, allow_redirects=True, timeout=REQUEST_TIMEOUT_SECONDS)
        return (
            _classify_link_health(response.status_code, None, is_internal=is_internal),
            response.status_code,
            response.url,
            None,
        )
    except requests.RequestException as exc:
        health = _classify_link_health(
            getattr(response, "status_code", None),
            str(exc),
            is_internal=is_internal,
        )
        return health, getattr(response, "status_code", None), None, str(exc)


def _page_health(http_status: int | None) -> str:
    if http_status is None:
        return "unreachable"
    if 200 <= http_status < 300:
        return "healthy"
    if 300 <= http_status < 400:
        return "redirect"
    return "broken"


def _safe_text_content(page, selector: str) -> str | None:
    locator = page.locator(selector)
    if locator.count() == 0:
        return None
    return locator.first.text_content()


def _safe_attribute(page, selector: str, attribute_name: str) -> str | None:
    locator = page.locator(selector)
    if locator.count() == 0:
        return None
    return locator.first.get_attribute(attribute_name)


def _failed_page_snapshot(page_url: str, http_status: int | None = None) -> PageSnapshot:
    category, topic = _derive_category_topic(page_url)
    return PageSnapshot(
        page_url=page_url,
        page_title="",
        category=category,
        topic=topic,
        last_update=None,
        http_status=http_status,
        page_health=_page_health(http_status),
        meta_description=None,
        content_text="",
        content_hash=md5(b"").hexdigest(),
        links=[],
    )


def _scrape_page_snapshot(browser_context, page_url: str) -> PageSnapshot:
    page = browser_context.new_page()
    response = None
    try:
        try:
            response = page.goto(page_url, wait_until="networkidle", timeout=PLAYWRIGHT_TIMEOUT_MS)
        except Exception:
            try:
                response = page.goto(
                    page_url,
                    wait_until="domcontentloaded",
                    timeout=PLAYWRIGHT_TIMEOUT_MS,
                )
            except Exception:
                return _failed_page_snapshot(page_url, http_status=getattr(response, "status", None))

        title = page.title().strip()
        content_text = _safe_text_content(page, "main") or _safe_text_content(page, "body") or ""
        meta_description = _safe_attribute(page, "meta[name='description']", "content")
        meta_values = page.eval_on_selector_all(
            "meta[property='article:modified_time'], meta[property='og:updated_time'], meta[name='lastmod'], meta[name='dcterms.modified'], meta[name='date']",
            "(nodes) => nodes.map((node) => node.getAttribute('content'))",
        )
        time_values = page.eval_on_selector_all(
            "time[datetime]",
            "(nodes) => nodes.map((node) => node.getAttribute('datetime'))",
        )
        content_link_selector = "main a[href]" if page.locator("main").count() > 0 else "body a[href]"
        raw_links = page.eval_on_selector_all(
            content_link_selector,
            "(nodes) => nodes.map((node) => ({ href: node.getAttribute('href'), text: (node.textContent || '').trim() }))",
        )
        normalized_links = []
        seen_links: set[str] = set()
        for raw_link in raw_links:
            normalized = _normalize_link(page_url, raw_link.get("href"))
            if (
                normalized
                and _should_track_link_url(normalized, urlparse(BASE_URL).netloc)
                and normalized not in seen_links
            ):
                seen_links.add(normalized)
                normalized_links.append(
                    {
                        "link_url": normalized,
                        "link_text": raw_link.get("text") or None,
                    }
                )
        category, topic = _derive_category_topic(page_url)
        http_status = response.status if response else None
        last_update = _resolve_last_update(
            meta_values=meta_values,
            time_values=time_values,
            header_value=response.headers.get("last-modified") if response else None,
        )
        return PageSnapshot(
            page_url=page_url,
            page_title=title,
            category=category,
            topic=topic,
            last_update=last_update,
            http_status=http_status,
            page_health=_page_health(http_status),
            meta_description=meta_description,
            content_text=" ".join(content_text.split()),
            content_hash=md5(content_text.encode("utf-8")).hexdigest(),
            links=normalized_links,
        )
    finally:
        page.close()


def _crawl_internal_urls(browser_context, seed_urls: Iterable[str], base_url: str = BASE_URL) -> list[str]:
    base_domain = urlparse(base_url).netloc
    queue = deque(_canonicalize_crawl_url(url) for url in seed_urls if _should_crawl_url(url, base_domain))
    visited: set[str] = set()
    discovered: set[str] = set()
    logger.info("Falling back to recursive internal-link crawl from %s", base_url)

    while queue:
        page_url = queue.popleft()
        if page_url in visited:
            continue
        visited.add(page_url)
        if len(visited) == 1 or len(visited) % PROGRESS_LOG_EVERY == 0:
            logger.info(
                "Discovery crawl progress: visited %s pages, queued %s, current=%s",
                len(visited),
                len(queue),
                page_url,
            )

        snapshot = _scrape_page_snapshot(browser_context, page_url)
        discovered.add(page_url)

        for link in snapshot.links:
            link_url = link["link_url"]
            if not link_url:
                continue
            canonical_link = _canonicalize_crawl_url(link_url)
            if not _should_crawl_url(canonical_link, base_domain):
                continue
            if canonical_link not in visited:
                queue.append(canonical_link)

    return sorted(discovered)


def _discover_site_urls(session: requests.Session, browser_context) -> list[str]:
    logger.info(
        "Using recursive internal-link crawl from owned section seeds: %s",
        ", ".join(sorted(ALLOWED_SECTION_ROOTS)),
    )
    return _crawl_internal_urls(browser_context, seed_urls=SECTION_SEED_URLS, base_url=BASE_URL)


def _validate_unique_links(
    link_urls: Iterable[str], base_domain: str
) -> dict[str, tuple[str, int | None, str | None, str | None]]:
    unique_link_urls = sorted(set(link_urls))
    if not unique_link_urls:
        return {}

    logger.info(
        "Validating %s unique links with up to %s workers",
        len(unique_link_urls),
        MAX_LINK_CHECK_WORKERS,
    )
    results: dict[str, tuple[str, int | None, str | None, str | None]] = {}
    with ThreadPoolExecutor(max_workers=MAX_LINK_CHECK_WORKERS) as executor:
        future_to_url = {
            executor.submit(_check_link_health, link_url, base_domain): link_url for link_url in unique_link_urls
        }
        for index, future in enumerate(as_completed(future_to_url), start=1):
            link_url = future_to_url[future]
            results[link_url] = future.result()
            if index == 1 or index % PROGRESS_LOG_EVERY == 0 or index == len(unique_link_urls):
                logger.info(
                    "Unique link validation progress: %s/%s complete",
                    index,
                    len(unique_link_urls),
                )
    return results


def _scan_site(
    browser_resource: PlaywrightBrowserResource,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    scanned_at = datetime.now(timezone.utc).isoformat()
    logger.info("Starting website content health scan for %s", SCAN_SCOPE)
    with requests.Session() as session:
        session.headers.update({"User-Agent": "BASIN website content health scanner/1.0"})
        with browser_resource.browser_context() as browser_context:
            discovered_urls = _discover_site_urls(session, browser_context)
            logger.info("Discovered %s total URLs to scan", len(discovered_urls))
            if not discovered_urls:
                empty_pages = pd.DataFrame(columns=PAGE_COLUMNS)
                empty_link_refs = pd.DataFrame(columns=LINK_REF_COLUMNS)
                return empty_pages, empty_link_refs

            page_rows: list[dict[str, object]] = []
            raw_link_rows: list[dict[str, object]] = []
            base_domain = urlparse(BASE_URL).netloc
            for index, page_url in enumerate(discovered_urls, start=1):
                if index == 1 or index % PROGRESS_LOG_EVERY == 0 or index == len(discovered_urls):
                    logger.info(
                        "Website content health progress: scanning page %s/%s: %s",
                        index,
                        len(discovered_urls),
                        page_url,
                    )
                snapshot = _scrape_page_snapshot(browser_context, page_url)
                page_rows.append(
                    {
                        "page_url": snapshot.page_url,
                        "page_path": urlparse(snapshot.page_url).path or "/",
                        "page_title": snapshot.page_title,
                        "category": snapshot.category,
                        "topic": snapshot.topic,
                        "last_update": snapshot.last_update,
                        "http_status": snapshot.http_status,
                        "page_health": snapshot.page_health,
                        "meta_description": snapshot.meta_description,
                        "content_text": snapshot.content_text,
                        "content_hash": snapshot.content_hash,
                        "link_count": len(snapshot.links),
                        "scanned_at": scanned_at,
                    }
                )
                for link in snapshot.links:
                    is_document = _is_document_url(link["link_url"])
                    raw_link_rows.append(
                        {
                            "source_page_url": snapshot.page_url,
                            "link_url": link["link_url"],
                            "link_text": link["link_text"],
                            "is_internal": urlparse(link["link_url"]).netloc == base_domain,
                            "is_document": is_document,
                            "scanned_at": scanned_at,
                        }
                    )
            logger.info(
                "Completed website crawl: %s page rows, %s link refs",
                len(page_rows),
                len(raw_link_rows),
            )

    pages_df = pd.DataFrame(page_rows, columns=PAGE_COLUMNS)
    link_refs_df = pd.DataFrame(raw_link_rows, columns=LINK_REF_COLUMNS)
    return pages_df, link_refs_df


def _build_unique_links_df(link_refs_df: pd.DataFrame) -> pd.DataFrame:
    if link_refs_df.empty:
        return pd.DataFrame(columns=UNIQUE_LINK_COLUMNS)

    validation_timestamp = datetime.now(timezone.utc).isoformat()
    base_domain = urlparse(BASE_URL).netloc
    link_results = _validate_unique_links(link_refs_df["link_url"].tolist(), base_domain=base_domain)
    unique_rows: list[dict[str, object]] = []
    for link_url, link_group in link_refs_df.groupby("link_url", sort=True):
        first_row = link_group.iloc[0]
        health, status_code, final_url, error = link_results.get(
            link_url,
            ("unknown", None, None, None),
        )
        unique_rows.append(
            {
                "link_url": link_url,
                "is_internal": bool(first_row["is_internal"]),
                "is_document": bool(first_row["is_document"]),
                "health": health,
                "status_code": status_code,
                "final_url": final_url,
                "error": error,
                "source_page_count": int(link_group["source_page_url"].nunique()),
                "scanned_at": validation_timestamp,
            }
        )
    return pd.DataFrame(unique_rows, columns=UNIQUE_LINK_COLUMNS)


def _build_links_df(link_refs_df: pd.DataFrame, unique_links_df: pd.DataFrame) -> pd.DataFrame:
    if link_refs_df.empty:
        return pd.DataFrame(columns=LINK_COLUMNS)

    validation_timestamp = (
        unique_links_df["scanned_at"].iloc[0] if not unique_links_df.empty else datetime.now(timezone.utc).isoformat()
    )
    validation_columns = ["link_url", "health", "status_code", "final_url", "error"]
    merged_df = link_refs_df.drop(columns=["scanned_at"]).merge(
        unique_links_df[validation_columns],
        on="link_url",
        how="left",
    )
    merged_df["scanned_at"] = validation_timestamp
    return merged_df.reindex(columns=LINK_COLUMNS)


def _pages_output_metadata(pages_df: pd.DataFrame) -> dict[str, object]:
    category_counts = pages_df["category"].value_counts().head(20).to_dict() if not pages_df.empty else {}
    return {
        "scan_scope": SCAN_SCOPE,
        "page_count": len(pages_df),
        "healthy_page_count": (int((pages_df["page_health"] == "healthy").sum()) if not pages_df.empty else 0),
        "unreachable_page_count": (int((pages_df["page_health"] == "unreachable").sum()) if not pages_df.empty else 0),
        "broken_page_count": (int((pages_df["page_health"] == "broken").sum()) if not pages_df.empty else 0),
        "pages_with_zero_links": (int((pages_df["link_count"] == 0).sum()) if not pages_df.empty else 0),
        "categories_covered": MetadataValue.json(
            sorted(pages_df["category"].dropna().unique().tolist()) if not pages_df.empty else []
        ),
        "category_counts": MetadataValue.json(category_counts),
        "sample_urls": MetadataValue.json(pages_df["page_url"].head(20).tolist() if not pages_df.empty else []),
    }


def _link_refs_output_metadata(link_refs_df: pd.DataFrame) -> dict[str, object]:
    return {
        "scan_scope": SCAN_SCOPE,
        "link_ref_count": len(link_refs_df),
        "unique_link_ref_count": (int(link_refs_df["link_url"].nunique()) if not link_refs_df.empty else 0),
    }


def _unique_links_output_metadata(unique_links_df: pd.DataFrame) -> dict[str, object]:
    return {
        "scan_scope": SCAN_SCOPE,
        "unique_link_count": len(unique_links_df),
        "unique_internal_link_count": (int(unique_links_df["is_internal"].sum()) if not unique_links_df.empty else 0),
        "unique_external_link_count": (
            int((~unique_links_df["is_internal"]).sum()) if not unique_links_df.empty else 0
        ),
        "broken_unique_link_count": (
            int((unique_links_df["health"] == "broken").sum()) if not unique_links_df.empty else 0
        ),
        "restricted_unique_link_count": (
            int((unique_links_df["health"] == "restricted").sum()) if not unique_links_df.empty else 0
        ),
        "rate_limited_unique_link_count": (
            int((unique_links_df["health"] == "rate_limited").sum()) if not unique_links_df.empty else 0
        ),
        "server_error_unique_link_count": (
            int((unique_links_df["health"] == "server_error").sum()) if not unique_links_df.empty else 0
        ),
        "blocked_unique_link_count": (
            int((unique_links_df["health"] == "blocked").sum()) if not unique_links_df.empty else 0
        ),
        "unreachable_unique_link_count": (
            int((unique_links_df["health"] == "unreachable").sum()) if not unique_links_df.empty else 0
        ),
        "redirect_unique_link_count": (
            int((unique_links_df["health"] == "redirect").sum()) if not unique_links_df.empty else 0
        ),
        "sample_broken_links": MetadataValue.json(
            unique_links_df.loc[unique_links_df["health"] == "broken", "link_url"].head(20).tolist()
            if not unique_links_df.empty
            else []
        ),
    }


def _links_output_metadata(links_df: pd.DataFrame) -> dict[str, object]:
    return {
        "scan_scope": SCAN_SCOPE,
        "link_count": len(links_df),
        "broken_link_count": (int((links_df["health"] == "broken").sum()) if not links_df.empty else 0),
    }


@multi_asset(
    outs={
        "mit_sustainability_pages": AssetOut(io_manager_key="postgres_replace", group_name="raw"),
        "mit_sustainability_link_refs": AssetOut(io_manager_key="postgres_replace", group_name="raw"),
    },
    compute_kind="playwright",
)
def mit_sustainability_content_health(
    playwright_browser: ResourceParam[PlaywrightBrowserResource],
):
    pages_df, link_refs_df = _scan_site(playwright_browser)
    yield Output(
        value=pages_df,
        output_name="mit_sustainability_pages",
        metadata=_pages_output_metadata(pages_df),
    )
    yield Output(
        value=link_refs_df,
        output_name="mit_sustainability_link_refs",
        metadata=_link_refs_output_metadata(link_refs_df),
    )


@asset(io_manager_key="postgres_replace", group_name="raw")
def mit_sustainability_unique_links(
    mit_sustainability_link_refs: pd.DataFrame,
) -> Output[pd.DataFrame]:
    unique_links_df = _build_unique_links_df(mit_sustainability_link_refs)
    return Output(value=unique_links_df, metadata=_unique_links_output_metadata(unique_links_df))


@asset(io_manager_key="postgres_replace", group_name="raw")
def mit_sustainability_links(
    mit_sustainability_link_refs: pd.DataFrame,
    mit_sustainability_unique_links: pd.DataFrame,
) -> Output[pd.DataFrame]:
    links_df = _build_links_df(
        mit_sustainability_link_refs,
        mit_sustainability_unique_links,
    )
    return Output(value=links_df, metadata=_links_output_metadata(links_df))
