"""Immutable S3 publish plus JSON ledger for knowledge bundles."""

from __future__ import annotations

import hashlib
import io
import json
import tarfile
from datetime import datetime, timezone

from botocore.exceptions import ClientError

from orchestrator.knowledge.bundle import Bundle, doc_fingerprint

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


def manifest_diff(previous: list[dict], current: list[dict]) -> dict[str, list[str]]:
    # Ledger entries written before `fingerprint` existed only support revision + Markdown hash.
    if all("fingerprint" in d for d in previous):
        key = lambda d: d["fingerprint"]  # noqa: E731
    else:
        key = lambda d: (d["source_revision"], d["markdown_sha256"])  # noqa: E731
    old, new = {d["document_id"]: key(d) for d in previous}, {d["document_id"]: key(d) for d in current}
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
            files = {m.name: tar.extractfile(m).read() for m in tar.getmembers() if m.isfile()}
        manifest = json.loads(files.get("manifest.json", b"{}"))
    except (tarfile.TarError, OSError, ValueError):
        manifest, files = {}, {}
    expected = {d.markdown_path: d.markdown.encode("utf-8") for d in bundle.documents}
    stored = {k: v for k, v in files.items() if k != "manifest.json"}
    if manifest.get("bundle_version") != bundle.version or stored != expected:
        raise AlreadyPublishedError(f"s3://{bucket}/{key} exists but does not match this bundle's content")
    return hashlib.sha256(raw).hexdigest()


def publish_bundle(client, bucket: str, prefix: str, bundle: Bundle) -> dict:
    """Upload the archive and ledger without overwriting; skip if content is unchanged.

    Returns the ledger entry, with `skipped=True` when nothing was uploaded.
    """
    revoked = (_get_json(client, bucket, f"{prefix}/revoked.json") or {}).get("revoked", [])
    if bundle.version in revoked:
        raise RevokedVersionError(f"Bundle version {bundle.version} is revoked")

    entries = _ledger_entries(client, bucket, prefix)
    previous = max(entries, key=lambda e: e["published_at"]) if entries else None
    if previous and previous["content_hash"] == bundle.content_hash and previous["bundle_version"] not in revoked:
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
        _put_new(client, bucket, entry["archive_key"], bundle.archive, "application/gzip")
    except AlreadyPublishedError:
        # A prior run uploaded the archive but failed before writing the ledger: adopt it.
        entry["sha256"] = _verified_stored_sha(client, bucket, entry["archive_key"], bundle)
    _put_new(
        client,
        bucket,
        f"{prefix}/ledger/{bundle.version}.json",
        json.dumps(entry, indent=2).encode("utf-8"),
        "application/json",
    )
    return {**entry, "skipped": False}
