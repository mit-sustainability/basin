"""Publish MkDocs and Confluence content as an immutable Beacon knowledge bundle.

Contract: specs/008-knowledge-bundle-publisher/contracts/. Dagster publishes to S3 only;
it never touches Beacon deployment state.
"""

from __future__ import annotations

import re
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from dagster import Output, ResourceParam, asset, get_dagster_logger
from dagster_aws.s3 import S3Resource
from markdownify import markdownify

from orchestrator.resources.confluence import ConfluencePage, ConfluenceResource
from orchestrator.resources.knowledge_bundle import (
    CONFLUENCE_ROOT_PAGE_IDS,
    FENCE,
    HEADING,
    MKDOCS_REF,
    MKDOCS_REPO_URL,
    MKDOCS_SITE_URL,
    S3_BUCKET,
    S3_PREFIX,
    Bundle,
    Document,
    build_bundle,
    publish_bundle,
    slugify,
)

# Syntax that would change reader-visible content if left unrendered.
BANNED = (re.compile(r"--8<--"), re.compile(r"\{\{"), re.compile(r"\{%"))
FRONT_MATTER = re.compile(r"\A---\n.*?\n---\n", re.S)


class UnsupportedSyntaxError(ValueError):
    """Raised when a page uses syntax that this exporter does not render."""


def clone(repo_url: str, ref: str, dest: Path) -> str:
    """Shallow-fetch `ref` (branch, tag or SHA) into dest and return the commit SHA."""
    for args in (
        ["init", "-q"],
        ["fetch", "-q", "--depth", "1", repo_url, ref],
        ["checkout", "-q", "FETCH_HEAD"],
    ):
        subprocess.run(
            ["git", *args], cwd=dest, check=True, capture_output=True, text=True
        )
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=dest,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _check_syntax(rel: str, text: str) -> None:
    in_fence = False
    for line in text.splitlines():
        if FENCE.match(line):
            in_fence = not in_fence
        elif not in_fence and any(p.search(line) for p in BANNED):
            raise UnsupportedSyntaxError(
                f"{rel}: unsupported syntax in line {line.strip()!r}"
            )


def _canonical_url(site_url: str, rel: Path) -> str:
    parts = list(rel.with_suffix("").parts)
    if parts and parts[-1] == "index":
        parts.pop()
    return site_url.rstrip("/") + "/" + "".join(f"{p}/" for p in parts)


def export_mkdocs(
    repo_dir: Path,
    site_url: str,
    revision: str,
    exported_at: str,
    docs_dir: str = "docs",
) -> list[Document]:
    root = repo_dir / docs_dir
    documents = []
    for path in sorted(root.rglob("*.md")):
        rel = path.relative_to(root)
        text = FRONT_MATTER.sub("", path.read_text(encoding="utf-8"))
        _check_syntax(f"{docs_dir}/{rel}", text)
        h1 = next(
            (
                m.group(2)
                for m in map(HEADING.match, text.splitlines())
                if m and len(m.group(1)) == 1
            ),
            None,
        )
        title = h1 or rel.stem.replace("_", " ").replace("-", " ").title()
        if not h1:
            text = f"# {title}\n\n{text}"
        documents.append(
            Document(
                document_id="mkdocs-"
                + slugify(rel.with_suffix("").as_posix().replace("/", "-")),
                markdown_path=f"portfolio/{rel.as_posix()}",
                source_type="mkdocs",
                source_key=f"{docs_dir}/{rel.as_posix()}",
                canonical_url=_canonical_url(site_url, rel),
                source_revision=revision,
                exported_at=exported_at,
                title=title,
                markdown=text.strip() + "\n",
            )
        )
    return documents


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


logger = get_dagster_logger()
GROUP = "knowledge"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@asset(name="knowledge_mkdocs_documents", group_name=GROUP, compute_kind="python")
def knowledge_mkdocs_documents() -> Output[list[Document]]:
    """Clone the public MkDocs repo at the configured ref and export its Markdown pages."""

    with tempfile.TemporaryDirectory() as tmp:
        revision = clone(MKDOCS_REPO_URL, MKDOCS_REF, Path(tmp))
        documents = export_mkdocs(Path(tmp), MKDOCS_SITE_URL, revision, _now())
    return Output(
        documents, metadata={"documents": len(documents), "revision": revision}
    )


@asset(name="knowledge_confluence_documents", group_name=GROUP, compute_kind="python")
def knowledge_confluence_documents(
    confluence: ResourceParam[ConfluenceResource],
) -> Output[list[Document]]:
    """Export the allowlisted Confluence root pages and their descendants as rendered Markdown."""

    documents = export_confluence(confluence, CONFLUENCE_ROOT_PAGE_IDS, _now())
    return Output(documents, metadata={"documents": len(documents)})


@asset(name="knowledge_bundle", group_name=GROUP, compute_kind="python")
def knowledge_bundle(
    knowledge_mkdocs_documents: list[Document],
    knowledge_confluence_documents: list[Document],
) -> Output[Bundle]:
    """Validate and package both sources into a deterministic archive."""

    bundle = build_bundle(
        [*knowledge_mkdocs_documents, *knowledge_confluence_documents]
    )
    return Output(
        bundle,
        metadata={
            "bundle_version": bundle.version,
            "sha256": bundle.sha256,
            "documents": len(bundle.documents),
            "archive_bytes": len(bundle.archive),
        },
    )


@asset(name="knowledge_published", group_name=GROUP, compute_kind="s3")
def knowledge_published(knowledge_bundle: Bundle, s3: S3Resource) -> Output[dict]:
    """Upload the archive and ledger to S3 (never overwriting) and report the pin values."""

    entry = publish_bundle(s3.get_client(), S3_BUCKET, S3_PREFIX, knowledge_bundle)
    logger.info(
        "BEACON_KNOWLEDGE_VERSION=%s BEACON_KNOWLEDGE_SHA256=%s (%s)",
        entry["bundle_version"],
        entry["sha256"],
        "unchanged, not uploaded" if entry["skipped"] else "published",
    )
    return Output(
        entry,
        metadata={
            "BEACON_KNOWLEDGE_VERSION": entry["bundle_version"],
            "BEACON_KNOWLEDGE_SHA256": entry["sha256"],
            "skipped": entry["skipped"],
            "diff": entry["diff"],
        },
    )
