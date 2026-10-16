"""Build a Beacon-compatible knowledge bundle (manifest + deterministic tar.gz).

Contract: specs/008-knowledge-bundle-publisher/contracts/knowledge-bundle-contract.md
"""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import re
import tarfile
from dataclasses import dataclass, field

from orchestrator.knowledge.validate import validate_manifest

FENCE = re.compile(r"^\s*(```|~~~)")
HEADING = re.compile(r"^(#{1,6})[ \t]+(.*?)[ \t]*#*[ \t]*$")
BEACON_HEADING = re.compile(r"(?m)^#{1,6}\s+.+$")
LINK = re.compile(r"\[([^\]]*)\]\([^)]*\)")


@dataclass(frozen=True)
class Document:
    document_id: str
    markdown_path: str
    source_type: str
    source_key: str
    canonical_url: str
    source_revision: str
    exported_at: str
    title: str
    markdown: str


@dataclass(frozen=True)
class Bundle:
    version: str
    content_hash: str
    manifest: dict
    archive: bytes
    sha256: str
    documents: tuple[Document, ...] = field(repr=False)


def slugify(text: str) -> str:
    """MkDocs-style slug: lowercase, drop punctuation, whitespace runs to one hyphen."""
    text = re.sub(r"[^\w\s-]", "", text.lower()).strip()
    return re.sub(r"\s+", "-", text)


def _plain(title: str) -> str:
    return re.sub(r"[*_`]", "", LINK.sub(r"\1", title)).strip()


def extract_headings(markdown: str) -> list[dict]:
    """Ordered headings, index-aligned with Beacon's section lookup.

    Beacon pairs the i-th manifest heading with the i-th match of `^#{1,6}\\s+.+$` over the
    whole Markdown (H1s and fenced lines included), so this uses the same regex and skips nothing.
    ponytail: a `# comment` line inside a code fence becomes a heading; strip fences upstream if it matters.
    """
    headings: list[dict] = []
    stack: list[tuple[int, str]] = []
    used: set[str] = set()
    for match in BEACON_HEADING.finditer(markdown):
        raw = match.group()
        level = len(raw.split()[0])
        text = re.sub(r"\s+#+$", "", raw[level:].strip())
        title = _plain(text) or text
        while stack and stack[-1][0] >= level:
            stack.pop()
        stack.append((level, title))
        base = slugify(title) or "section"
        anchor, n = base, 0
        while anchor in used:
            n += 1
            anchor = f"{base}-{n}"
        used.add(anchor)
        headings.append(
            {"title": title, "level": level, "anchor": anchor, "path": " / ".join(t for _, t in stack)}
        )
    return headings


def content_hash(documents: list[Document]) -> str:
    """Hash of bundle content, excluding the per-run `exported_at` timestamp."""
    rows = [
        [
            d.document_id,
            d.markdown_path,
            d.canonical_url,
            d.source_revision,
            d.title,
            hashlib.sha256(d.markdown.encode("utf-8")).hexdigest(),
            extract_headings(d.markdown),
        ]
        for d in sorted(documents, key=lambda d: d.document_id)
    ]
    return hashlib.sha256(json.dumps(rows).encode("utf-8")).hexdigest()


def build_manifest(version: str, documents: list[Document]) -> dict:
    return {
        "bundle_version": version,
        "documents": [
            {
                "document_id": d.document_id,
                "markdown_path": d.markdown_path,
                "source_type": d.source_type,
                "source_key": d.source_key,
                "canonical_url": d.canonical_url,
                "source_revision": d.source_revision,
                "exported_at": d.exported_at,
                "title": d.title,
                "headings": extract_headings(d.markdown),
            }
            for d in sorted(documents, key=lambda d: d.document_id)
        ],
    }


def build_archive(files: dict[str, bytes]) -> bytes:
    """Deterministic tar.gz: sorted members, fixed owner/mode/mtime, gzip mtime=0."""
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w", format=tarfile.PAX_FORMAT) as tar:
        for name in sorted(files):
            info = tarfile.TarInfo(name)
            info.size, info.mtime, info.mode = len(files[name]), 0, 0o644
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            tar.addfile(info, io.BytesIO(files[name]))
    out = io.BytesIO()
    with gzip.GzipFile(fileobj=out, mode="wb", mtime=0, filename="") as gz:
        gz.write(raw.getvalue())
    return out.getvalue()


def build_bundle(documents: list[Document]) -> Bundle:
    """Validate and package documents. Version is `YYYYMMDD-<content hash prefix>`."""
    if not documents:
        raise ValueError("Cannot build a knowledge bundle with no documents")
    digest = content_hash(documents)
    date = min(d.exported_at for d in documents)[:10].replace("-", "")
    version = f"{date}-{digest[:8]}"
    manifest = build_manifest(version, documents)
    files = {d.markdown_path: d.markdown.encode("utf-8") for d in documents}
    validate_manifest(manifest, files)
    files["manifest.json"] = json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8")
    archive = build_archive(files)
    return Bundle(
        version=version,
        content_hash=digest,
        manifest=manifest,
        archive=archive,
        sha256=hashlib.sha256(archive).hexdigest(),
        documents=tuple(documents),
    )
