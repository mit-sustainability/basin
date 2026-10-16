"""Knowledge bundle build and immutable S3 publish (Beacon contract).

Contract: specs/008-knowledge-bundle-publisher/contracts/. Credentials stay in the environment.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
import re
import tarfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Mapping

from botocore.exceptions import ClientError

S3_BUCKET = os.getenv("KNOWLEDGE_S3_BUCKET", "mitos-resources")
S3_PREFIX = os.getenv("KNOWLEDGE_S3_PREFIX", "knowledgebase").strip("/")

MKDOCS_REPO_URL = os.getenv(
    "KNOWLEDGE_MKDOCS_REPO_URL", "https://github.com/mit-sustainability/portfolio"
)
MKDOCS_REF = os.getenv("KNOWLEDGE_MKDOCS_REF", "main")
MKDOCS_SITE_URL = os.getenv(
    "KNOWLEDGE_MKDOCS_SITE_URL", "https://mit-sustainability.github.io/portfolio/"
)

# Contribution Guide, Gen AI Tools, Tableau. Each root is exported with its descendants.
CONFLUENCE_ROOT_PAGE_IDS = ("319732366", "319723988", "319724724")


SOURCE_TYPES = {"mkdocs", "confluence"}
DOC_FIELDS = (
    "document_id",
    "markdown_path",
    "source_type",
    "source_key",
    "canonical_url",
    "source_revision",
    "exported_at",
    "title",
)


class ManifestError(ValueError):
    """Raised when a manifest or its Markdown files violate the contract."""


def _check_path(path: str) -> None:
    if not path.endswith(".md"):
        raise ManifestError(f"markdown_path must end in .md: {path!r}")
    if path.startswith("/") or "\\" in path or ".." in path.split("/") or ".." in path:
        raise ManifestError(f"markdown_path must be relative with no '..': {path!r}")


def validate_manifest(
    manifest: dict, files: Mapping[str, bytes | str] | None = None
) -> None:
    """Raise ManifestError on the first contract violation.

    If `files` is given, every markdown_path must exist there and decode as UTF-8.
    """
    if not str(manifest.get("bundle_version") or "").strip():
        raise ManifestError("bundle_version is required")
    documents = manifest.get("documents")
    if not isinstance(documents, list) or not documents:
        raise ManifestError("documents must be a non-empty list")

    ids: set[str] = set()
    paths: set[str] = set()
    for doc in documents:
        for name in DOC_FIELDS:
            if not isinstance(doc.get(name), str) or not doc[name].strip():
                raise ManifestError(
                    f"document field {name!r} is required: {doc.get('document_id')!r}"
                )
        if doc["source_type"] not in SOURCE_TYPES:
            raise ManifestError(f"unknown source_type {doc['source_type']!r}")
        try:
            datetime.fromisoformat(doc["exported_at"].replace("Z", "+00:00"))
        except ValueError as exc:
            raise ManifestError(
                f"exported_at is not ISO-8601: {doc['exported_at']!r}"
            ) from exc
        if doc["document_id"] in ids:
            raise ManifestError(f"duplicate document_id {doc['document_id']!r}")
        ids.add(doc["document_id"])
        _check_path(doc["markdown_path"])
        if doc["markdown_path"] in paths:
            raise ManifestError(f"duplicate markdown_path {doc['markdown_path']!r}")
        paths.add(doc["markdown_path"])

        # ponytail: anchor uniqueness is per document; tighten if Beacon enforces it bundle-wide.
        anchors: set[str] = set()
        for heading in doc.get("headings", []):
            if (
                not heading.get("title")
                or not heading.get("anchor")
                or not heading.get("path")
            ):
                raise ManifestError(
                    f"heading needs title, anchor and path: {doc['document_id']!r}"
                )
            if (
                not isinstance(heading.get("level"), int)
                or not 1 <= heading["level"] <= 6
            ):
                raise ManifestError(
                    f"heading level must be 1-6: {doc['document_id']!r}"
                )
            if heading["anchor"] in anchors:
                raise ManifestError(
                    f"duplicate anchor {heading['anchor']!r} in {doc['document_id']!r}"
                )
            anchors.add(heading["anchor"])

        if files is not None:
            if doc["markdown_path"] not in files:
                raise ManifestError(f"missing Markdown file {doc['markdown_path']!r}")
            body = files[doc["markdown_path"]]
            if isinstance(body, bytes):
                try:
                    body.decode("utf-8")
                except UnicodeDecodeError as exc:
                    raise ManifestError(
                        f"{doc['markdown_path']!r} is not UTF-8"
                    ) from exc


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
            {
                "title": title,
                "level": level,
                "anchor": anchor,
                "path": " / ".join(t for _, t in stack),
            }
        )
    return headings


def _row(d: Document) -> list:
    """Manifest-affecting fields of a document, excluding the per-run `exported_at`."""
    return [
        d.document_id,
        d.markdown_path,
        d.canonical_url,
        d.source_revision,
        d.title,
        hashlib.sha256(d.markdown.encode("utf-8")).hexdigest(),
        extract_headings(d.markdown),
    ]


def doc_fingerprint(d: Document) -> str:
    """Per-document hash used by the ledger diff to spot metadata-only changes."""
    return hashlib.sha256(json.dumps(_row(d)).encode("utf-8")).hexdigest()


def content_hash(documents: list[Document]) -> str:
    """Hash of bundle content, excluding the per-run `exported_at` timestamp."""
    rows = [_row(d) for d in sorted(documents, key=lambda d: d.document_id)]
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
    files["manifest.json"] = json.dumps(manifest, indent=2, ensure_ascii=False).encode(
        "utf-8"
    )
    archive = build_archive(files)
    return Bundle(
        version=version,
        content_hash=digest,
        manifest=manifest,
        archive=archive,
        sha256=hashlib.sha256(archive).hexdigest(),
        documents=tuple(documents),
    )


CONFLICT_CODES = {"PreconditionFailed", "ConditionalRequestConflict"}
MISSING_CODES = {"NoSuchKey", "404"}


class AlreadyPublishedError(RuntimeError):
    """Raised when an object for this bundle version already exists."""


class RevokedVersionError(RuntimeError):
    """Raised when the bundle version was revoked after a content takedown."""


def _get_json(client, bucket: str, key: str):
    try:
        return json.loads(client.get_object(Bucket=bucket, Key=key)["Body"].read())
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in MISSING_CODES:
            return None
        raise


def _put_new(client, bucket: str, key: str, body: bytes, content_type: str) -> None:
    try:
        client.put_object(
            Bucket=bucket, Key=key, Body=body, ContentType=content_type, IfNoneMatch="*"
        )
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in CONFLICT_CODES:
            raise AlreadyPublishedError(
                f"s3://{bucket}/{key} already exists; published versions are immutable"
            ) from exc
        raise


def _ledger_entries(client, bucket: str, prefix: str) -> list[dict]:
    entries, token = [], None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": f"{prefix}/ledger/"}
        if token:
            kwargs["ContinuationToken"] = token
        page = client.list_objects_v2(**kwargs)
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                entries.append(_get_json(client, bucket, obj["Key"]))
        if not page.get("IsTruncated"):
            return entries
        token = page["NextContinuationToken"]


def manifest_diff(previous: list[dict], current: list[dict]) -> dict[str, list[str]]:
    # Ledger entries written before `fingerprint` existed only support revision + Markdown hash.
    if all("fingerprint" in d for d in previous):
        key = lambda d: d["fingerprint"]  # noqa: E731
    else:
        key = lambda d: (d["source_revision"], d["markdown_sha256"])  # noqa: E731
    old, new = (
        {d["document_id"]: key(d) for d in previous},
        {d["document_id"]: key(d) for d in current},
    )
    return {
        "added": sorted(new.keys() - old.keys()),
        "removed": sorted(old.keys() - new.keys()),
        "changed": sorted(k for k in new.keys() & old.keys() if new[k] != old[k]),
    }


def _verified_stored_sha(client, bucket: str, key: str, bundle: Bundle) -> str:
    """Return the sha256 of the stored archive after checking it holds this bundle's content."""
    raw = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    try:
        with tarfile.open(fileobj=io.BytesIO(raw), mode="r:gz") as tar:
            files = {
                m.name: tar.extractfile(m).read()
                for m in tar.getmembers()
                if m.isfile()
            }
        manifest = json.loads(files.get("manifest.json", b"{}"))
    except (tarfile.TarError, OSError, ValueError):
        manifest, files = {}, {}
    expected = {d.markdown_path: d.markdown.encode("utf-8") for d in bundle.documents}
    stored = {k: v for k, v in files.items() if k != "manifest.json"}
    if manifest.get("bundle_version") != bundle.version or stored != expected:
        raise AlreadyPublishedError(
            f"s3://{bucket}/{key} exists but does not match this bundle's content"
        )
    return hashlib.sha256(raw).hexdigest()


def publish_bundle(client, bucket: str, prefix: str, bundle: Bundle) -> dict:
    """Upload the archive and ledger without overwriting; skip if content is unchanged.

    Returns the ledger entry, with `skipped=True` when nothing was uploaded.
    """
    revoked = (_get_json(client, bucket, f"{prefix}/revoked.json") or {}).get(
        "revoked", []
    )
    if bundle.version in revoked:
        raise RevokedVersionError(f"Bundle version {bundle.version} is revoked")

    entries = _ledger_entries(client, bucket, prefix)
    previous = max(entries, key=lambda e: e["published_at"]) if entries else None
    if (
        previous
        and previous["content_hash"] == bundle.content_hash
        and previous["bundle_version"] not in revoked
    ):
        return {**previous, "skipped": True}

    documents = [
        {
            "document_id": d.document_id,
            "source_type": d.source_type,
            "source_key": d.source_key,
            "source_revision": d.source_revision,
            "markdown_sha256": hashlib.sha256(d.markdown.encode("utf-8")).hexdigest(),
            "fingerprint": doc_fingerprint(d),
        }
        for d in sorted(bundle.documents, key=lambda d: d.document_id)
    ]
    entry = {
        "bundle_version": bundle.version,
        "sha256": bundle.sha256,
        "content_hash": bundle.content_hash,
        "published_at": datetime.now(timezone.utc).isoformat(),
        "archive_key": f"{prefix}/{bundle.version}.tar.gz",
        "documents": documents,
        "diff": manifest_diff(previous["documents"] if previous else [], documents),
        "previous_version": previous["bundle_version"] if previous else None,
    }
    try:
        _put_new(
            client, bucket, entry["archive_key"], bundle.archive, "application/gzip"
        )
    except AlreadyPublishedError:
        # A prior run uploaded the archive but failed before writing the ledger: adopt it.
        entry["sha256"] = _verified_stored_sha(
            client, bucket, entry["archive_key"], bundle
        )
    _put_new(
        client,
        bucket,
        f"{prefix}/ledger/{bundle.version}.json",
        json.dumps(entry, indent=2).encode("utf-8"),
        "application/json",
    )
    return {**entry, "skipped": False}
