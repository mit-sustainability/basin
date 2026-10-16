"""Validate a manifest against the Beacon knowledge bundle contract."""

from __future__ import annotations

from datetime import datetime
from typing import Mapping

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


def validate_manifest(manifest: dict, files: Mapping[str, bytes | str] | None = None) -> None:
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
                raise ManifestError(f"document field {name!r} is required: {doc.get('document_id')!r}")
        if doc["source_type"] not in SOURCE_TYPES:
            raise ManifestError(f"unknown source_type {doc['source_type']!r}")
        try:
            datetime.fromisoformat(doc["exported_at"].replace("Z", "+00:00"))
        except ValueError as exc:
            raise ManifestError(f"exported_at is not ISO-8601: {doc['exported_at']!r}") from exc
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
            if not heading.get("title") or not heading.get("anchor") or not heading.get("path"):
                raise ManifestError(f"heading needs title, anchor and path: {doc['document_id']!r}")
            if not isinstance(heading.get("level"), int) or not 1 <= heading["level"] <= 6:
                raise ManifestError(f"heading level must be 1-6: {doc['document_id']!r}")
            if heading["anchor"] in anchors:
                raise ManifestError(f"duplicate anchor {heading['anchor']!r} in {doc['document_id']!r}")
            anchors.add(heading["anchor"])

        if files is not None:
            if doc["markdown_path"] not in files:
                raise ManifestError(f"missing Markdown file {doc['markdown_path']!r}")
            body = files[doc["markdown_path"]]
            if isinstance(body, bytes):
                try:
                    body.decode("utf-8")
                except UnicodeDecodeError as exc:
                    raise ManifestError(f"{doc['markdown_path']!r} is not UTF-8") from exc
