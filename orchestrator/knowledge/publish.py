"""Immutable S3 publish plus JSON ledger for knowledge bundles."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

from botocore.exceptions import ClientError

from orchestrator.knowledge.bundle import Bundle

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
        client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type, IfNoneMatch="*")
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in CONFLICT_CODES:
            raise AlreadyPublishedError(f"s3://{bucket}/{key} already exists; published versions are immutable") from exc
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


def _doc_index(documents: list[dict]) -> dict[str, tuple]:
    return {d["document_id"]: (d["source_revision"], d["markdown_sha256"]) for d in documents}


def manifest_diff(previous: list[dict], current: list[dict]) -> dict[str, list[str]]:
    old, new = _doc_index(previous), _doc_index(current)
    return {
        "added": sorted(new.keys() - old.keys()),
        "removed": sorted(old.keys() - new.keys()),
        "changed": sorted(k for k in new.keys() & old.keys() if new[k] != old[k]),
    }


def publish_bundle(client, bucket: str, prefix: str, bundle: Bundle) -> dict:
    """Upload the archive and ledger without overwriting; skip if content is unchanged.

    Returns the ledger entry, with `skipped=True` when nothing was uploaded.
    """
    revoked = (_get_json(client, bucket, f"{prefix}/revoked.json") or {}).get("revoked", [])
    if bundle.version in revoked:
        raise RevokedVersionError(f"Bundle version {bundle.version} is revoked")

    entries = _ledger_entries(client, bucket, prefix)
    previous = max(entries, key=lambda e: e["published_at"]) if entries else None
    if previous and previous["content_hash"] == bundle.content_hash:
        return {**previous, "skipped": True}

    documents = [
        {
            "document_id": d.document_id,
            "source_type": d.source_type,
            "source_key": d.source_key,
            "source_revision": d.source_revision,
            "markdown_sha256": hashlib.sha256(d.markdown.encode("utf-8")).hexdigest(),
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
    _put_new(client, bucket, entry["archive_key"], bundle.archive, "application/gzip")
    _put_new(
        client,
        bucket,
        f"{prefix}/ledger/{bundle.version}.json",
        json.dumps(entry, indent=2).encode("utf-8"),
        "application/json",
    )
    return {**entry, "skipped": False}
