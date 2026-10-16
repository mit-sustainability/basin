"""Export Confluence root pages (and descendants) as knowledge Documents."""

from __future__ import annotations

import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from markdownify import markdownify

from orchestrator.knowledge.bundle import Document, slugify
from orchestrator.resources.confluence import ConfluencePage, ConfluenceResource

# Macro UI chrome in export_view (recently-updated, pagetree, hidden form state, icons).
NOISE_SELECTOR = "script, style, form, input, img, .hidden, .recently-updated, .plugin_pagetree, .aui-icon"


def html_to_markdown(html: str, page_url: str) -> str:
    """Convert Confluence export_view HTML to Markdown, resolving links against `page_url`."""
    soup = BeautifulSoup(html or "", "html.parser")
    for tag in soup.select(NOISE_SELECTOR):
        tag.decompose()
    for link in soup.select("a[href]"):
        link["href"] = urljoin(page_url, link["href"])
    markdown = markdownify(str(soup), heading_style="ATX", strip=["span", "div"])
    return re.sub(r"\n{3,}", "\n\n", markdown).strip()


def _page_path(page: ConfluencePage, root_id: str, root_title: str) -> str:
    segments = [root_title]
    if page.page_id != root_id:
        ids = [pid for pid, _ in page.ancestors]
        below = page.ancestors[ids.index(root_id) + 1 :] if root_id in ids else ()
        segments += [title for _, title in below] + [page.title]
    return "confluence/" + "/".join(slugify(s) or "page" for s in segments) + ".md"


def export_confluence(
    confluence: ConfluenceResource, root_page_ids: tuple[str, ...], exported_at: str
) -> list[Document]:
    documents: dict[str, Document] = {}
    for root_id in root_page_ids:
        root = confluence.get_page(root_id)
        for page in [root, *confluence.list_descendants(root_id)]:
            if page.page_id in documents:
                continue
            url = f"{confluence.base_url.rstrip('/')}/pages/viewpage.action?pageId={page.page_id}"
            body = html_to_markdown(page.export_html, url)
            documents[page.page_id] = Document(
                document_id=f"confluence-{page.page_id}",
                markdown_path=_page_path(page, root_id, root.title),
                source_type="confluence",
                source_key=page.page_id,
                canonical_url=url,
                source_revision=str(page.version),
                exported_at=exported_at,
                title=page.title,
                markdown=f"# {page.title}\n\n{body}\n" if body else f"# {page.title}\n",
            )
    return list(documents.values())
