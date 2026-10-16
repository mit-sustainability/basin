import io
import json

import pytest
from botocore.exceptions import ClientError

from orchestrator.knowledge.bundle import build_bundle
from orchestrator.knowledge.publish import (
    AlreadyPublishedError,
    RevokedVersionError,
    manifest_diff,
    publish_bundle,
)

from .conftest import make_doc


class FakeS3:
    """In-memory stand-in for the boto3 calls publish_bundle uses."""

    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def put_object(self, Bucket, Key, Body, IfNoneMatch=None, **_):
        if IfNoneMatch == "*" and Key in self.objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        self.objects[Key] = Body

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[Key])}

    def list_objects_v2(self, Bucket, Prefix, **_):
        return {"Contents": [{"Key": k} for k in sorted(self.objects) if k.startswith(Prefix)]}


def test_first_publish_uploads_archive_and_ledger():
    s3, bundle = FakeS3(), build_bundle([make_doc("a")])
    entry = publish_bundle(s3, "bkt", "kb", bundle)

    assert entry["skipped"] is False
    assert s3.objects[f"kb/{bundle.version}.tar.gz"] == bundle.archive
    ledger = json.loads(s3.objects[f"kb/ledger/{bundle.version}.json"])
    assert ledger["sha256"] == bundle.sha256
    assert ledger["diff"] == {"added": ["a"], "removed": [], "changed": []}
    assert ledger["previous_version"] is None


def test_unchanged_content_is_not_republished():
    s3 = FakeS3()
    publish_bundle(s3, "bkt", "kb", build_bundle([make_doc("a")]))
    keys = set(s3.objects)
    entry = publish_bundle(s3, "bkt", "kb", build_bundle([make_doc("a", exported_at="2026-10-17T00:00:00Z")]))

    assert entry["skipped"] is True
    assert set(s3.objects) == keys


def test_changed_content_publishes_new_version_with_diff():
    s3 = FakeS3()
    first = publish_bundle(s3, "bkt", "kb", build_bundle([make_doc("a"), make_doc("b")]))
    second = publish_bundle(
        s3, "bkt", "kb", build_bundle([make_doc("a", source_revision="new"), make_doc("c")])
    )

    assert second["skipped"] is False
    assert second["previous_version"] == first["bundle_version"]
    assert second["diff"] == {"added": ["c"], "removed": ["b"], "changed": ["a"]}


def test_existing_version_is_never_overwritten():
    s3, bundle = FakeS3(), build_bundle([make_doc("a")])
    s3.objects[f"kb/{bundle.version}.tar.gz"] = b"existing"
    with pytest.raises(AlreadyPublishedError):
        publish_bundle(s3, "bkt", "kb", bundle)
    assert s3.objects[f"kb/{bundle.version}.tar.gz"] == b"existing"


def test_revoked_version_is_refused():
    s3, bundle = FakeS3(), build_bundle([make_doc("a")])
    s3.objects["kb/revoked.json"] = json.dumps({"revoked": [bundle.version]}).encode()
    with pytest.raises(RevokedVersionError):
        publish_bundle(s3, "bkt", "kb", bundle)
    assert not any(k.endswith(".tar.gz") for k in s3.objects)


def test_manifest_diff_ignores_unchanged_documents():
    docs = [{"document_id": "a", "source_revision": "1", "markdown_sha256": "x"}]
    assert manifest_diff(docs, docs) == {"added": [], "removed": [], "changed": []}


def test_unchanged_content_after_revocation_publishes_a_new_version():
    s3 = FakeS3()
    first = publish_bundle(s3, "bkt", "kb", build_bundle([make_doc("a")]))
    s3.objects["kb/revoked.json"] = json.dumps({"revoked": [first["bundle_version"]]}).encode()
    later = build_bundle([make_doc("a", exported_at="2026-10-17T00:00:00Z")])
    entry = publish_bundle(s3, "bkt", "kb", later)

    assert entry["skipped"] is False and entry["bundle_version"] != first["bundle_version"]


def test_metadata_only_change_is_reported_as_changed():
    s3 = FakeS3()
    publish_bundle(s3, "bkt", "kb", build_bundle([make_doc("a")]))
    entry = publish_bundle(s3, "bkt", "kb", build_bundle([make_doc("a", canonical_url="https://new.example/a")]))

    assert entry["diff"]["changed"] == ["a"]


def test_retry_after_failed_ledger_write_adopts_stored_archive():
    class FlakyS3(FakeS3):
        fail = True

        def put_object(self, Bucket, Key, Body, **kw):
            if "/ledger/" in Key and self.fail:
                self.fail = False
                raise RuntimeError("ledger write failed")
            super().put_object(Bucket, Key, Body, **kw)

    s3 = FlakyS3()
    first = build_bundle([make_doc("a")])
    with pytest.raises(RuntimeError):
        publish_bundle(s3, "bkt", "kb", first)
    retry = build_bundle([make_doc("a", exported_at="2026-10-16T09:00:00Z")])
    entry = publish_bundle(s3, "bkt", "kb", retry)

    assert entry["sha256"] == first.sha256 and f"kb/ledger/{first.version}.json" in s3.objects


def test_manifest_diff_falls_back_for_legacy_ledger_entries():
    old = [{"document_id": "a", "source_revision": "1", "markdown_sha256": "x"}]
    assert manifest_diff(old, [{**old[0], "fingerprint": "f"}])["changed"] == []
