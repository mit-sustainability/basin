import copy
import gzip
import hashlib
import io
import json
import tarfile
from dataclasses import replace
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

from orchestrator.resources.knowledge_bundle import (
    AlreadyPublishedError,
    Document,
    ManifestError,
    RevokedVersionError,
    build_bundle,
    extract_headings,
    manifest_diff,
    publish_bundle,
    slugify,
    validate_manifest,
)

FIXTURES = (
    Path(__file__).resolve().parents[3]
    / "specs/008-knowledge-bundle-publisher/contracts/fixtures"
)


def make_doc(
    document_id="doc-a", markdown="# A\n\n## Overview\n\ntext\n", **overrides
) -> Document:
    fields = dict(
        document_id=document_id,
        markdown_path=f"portfolio/{document_id}.md",
        source_type="mkdocs",
        source_key=f"docs/{document_id}.md",
        canonical_url=f"https://example.edu/{document_id}/",
        source_revision="abc123",
        exported_at="2026-10-16T00:00:00Z",
        title="A",
        markdown=markdown,
    )
    fields.update(overrides)
    return Document(**fields)


def test_headings_include_title_and_follow_beacon_heading_regex():
    md = "# Title\n\n## Overview\n\n### Sub *a*\n\n## Overview ##\n\n```\n## in fence\n```\n"
    headings = extract_headings(md)
    assert [(h["level"], h["title"], h["anchor"]) for h in headings] == [
        (1, "Title", "title"),
        (2, "Overview", "overview"),
        (3, "Sub a", "sub-a"),
        (2, "Overview", "overview-1"),
        (
            2,
            "in fence",
            "in-fence",
        ),  # Beacon's regex counts fenced lines, so index alignment needs them
    ]
    assert headings[2]["path"] == "Title / Overview / Sub a"


def test_headings_are_index_aligned_with_beacon_regex():
    import re

    md = "# T\n\ntext\n\n## A\n\n### B\n\n```\n# c\n```\n\n## A\n"
    assert len(extract_headings(md)) == len(re.findall(r"(?m)^#{1,6}\s+.+$", md))


def test_heading_anchor_does_not_collide_with_literal_suffix():
    anchors = [h["anchor"] for h in extract_headings("## A\n## A\n## A 1\n## A\n")]
    assert len(set(anchors)) == 4


def test_slugify_matches_mkdocs_style():
    assert slugify("Hello, World & Co.") == "hello-world-co"


def test_bundle_is_deterministic_and_sorted():
    docs = [make_doc("b"), make_doc("a")]
    first, second = build_bundle(docs), build_bundle(list(reversed(docs)))
    assert first.sha256 == second.sha256 == hashlib.sha256(first.archive).hexdigest()

    raw = gzip.decompress(first.archive)
    assert first.archive[4:8] == b"\x00\x00\x00\x00"  # gzip mtime=0
    with tarfile.open(fileobj=io.BytesIO(raw)) as tar:
        members = tar.getmembers()
    assert [m.name for m in members] == sorted(m.name for m in members)
    assert {(m.mtime, m.uid, m.gid, m.mode, m.uname, m.gname) for m in members} == {
        (0, 0, 0, 0o644, "", "")
    }
    assert all(m.isfile() for m in members)


def test_version_depends_on_content_not_export_time():
    base = build_bundle([make_doc()])
    reexported = build_bundle([make_doc(exported_at="2026-10-16T09:30:00Z")])
    changed = build_bundle([make_doc(source_revision="def456")])
    assert reexported.content_hash == base.content_hash
    assert changed.content_hash != base.content_hash
    assert base.version == f"20261016-{base.content_hash[:8]}"


def test_empty_bundle_is_rejected():
    with pytest.raises(ValueError):
        build_bundle([])


def test_build_rejects_unsafe_document():
    with pytest.raises(ValueError):
        build_bundle([make_doc(markdown_path="../outside.md")])


def test_bundle_conforms_to_valid_fixture_shape():
    fixture = json.loads((FIXTURES / "valid/manifest.json").read_text())
    docs = [
        make_doc(
            d["document_id"],
            (FIXTURES / "valid" / d["markdown_path"]).read_text(),
            **{k: v for k, v in d.items() if k not in ("headings", "document_id")},
        )
        for d in fixture["documents"]
    ]
    bundle = build_bundle(docs)

    assert set(bundle.manifest) == set(fixture)
    assert set(bundle.manifest["documents"][0]) == set(fixture["documents"][0])
    assert set(bundle.manifest["documents"][0]["headings"][0]) == set(
        fixture["documents"][0]["headings"][0]
    )
    # The expanded archive passes the same validation as the fixture.
    with tarfile.open(fileobj=io.BytesIO(gzip.decompress(bundle.archive))) as tar:
        files = {m.name: tar.extractfile(m).read() for m in tar.getmembers()}
    validate_manifest(json.loads(files.pop("manifest.json")), files)
    assert (
        replace(docs[0]).markdown
        == (FIXTURES / "valid" / docs[0].markdown_path).read_text()
    )


VALID = json.loads((FIXTURES / "valid/manifest.json").read_text())


def _files(manifest):
    return {
        d["markdown_path"]: (FIXTURES / "valid" / d["markdown_path"]).read_bytes()
        for d in manifest["documents"]
    }


def test_valid_fixture_is_accepted_with_its_files():
    validate_manifest(VALID, _files(VALID))


@pytest.mark.parametrize(
    "fixture",
    sorted((FIXTURES / "invalid").glob("*/manifest.json")),
    ids=lambda p: p.parent.name,
)
def test_every_invalid_fixture_is_rejected(fixture):
    with pytest.raises(ManifestError):
        validate_manifest(json.loads(fixture.read_text()))


def _mutate(fn):
    manifest = copy.deepcopy(VALID)
    fn(manifest)
    return manifest


@pytest.mark.parametrize(
    "mutation",
    [
        lambda m: m.pop("bundle_version"),
        lambda m: m.update(documents=[]),
        lambda m: m["documents"][1].update(
            document_id=m["documents"][0]["document_id"]
        ),
        lambda m: m["documents"][1].update(
            markdown_path=m["documents"][0]["markdown_path"]
        ),
        lambda m: m["documents"][0].update(markdown_path="/abs/path.md"),
        lambda m: m["documents"][0].update(markdown_path="a/../b.md"),
        lambda m: m["documents"][0].update(markdown_path="a/b.txt"),
        lambda m: m["documents"][0].update(source_type="notion"),
        lambda m: m["documents"][0].update(exported_at="yesterday"),
        lambda m: m["documents"][0].update(title=""),
        lambda m: m["documents"][0]["headings"][1].update(anchor="overview"),
        lambda m: m["documents"][0]["headings"][0].update(level=7),
    ],
)
def test_contract_violations_are_rejected(mutation):
    with pytest.raises(ManifestError):
        validate_manifest(_mutate(mutation))


def test_missing_and_non_utf8_markdown_are_rejected():
    files = _files(VALID)
    path = VALID["documents"][0]["markdown_path"]
    with pytest.raises(ManifestError):
        validate_manifest(VALID, {k: v for k, v in files.items() if k != path})
    with pytest.raises(ManifestError):
        validate_manifest(VALID, {**files, path: b"\xff\xfe"})


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
        return {
            "Contents": [
                {"Key": k} for k in sorted(self.objects) if k.startswith(Prefix)
            ]
        }


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
    entry = publish_bundle(
        s3,
        "bkt",
        "kb",
        build_bundle([make_doc("a", exported_at="2026-10-17T00:00:00Z")]),
    )

    assert entry["skipped"] is True
    assert set(s3.objects) == keys


def test_changed_content_publishes_new_version_with_diff():
    s3 = FakeS3()
    first = publish_bundle(
        s3, "bkt", "kb", build_bundle([make_doc("a"), make_doc("b")])
    )
    second = publish_bundle(
        s3,
        "bkt",
        "kb",
        build_bundle([make_doc("a", source_revision="new"), make_doc("c")]),
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
    s3.objects["kb/revoked.json"] = json.dumps(
        {"revoked": [first["bundle_version"]]}
    ).encode()
    later = build_bundle([make_doc("a", exported_at="2026-10-17T00:00:00Z")])
    entry = publish_bundle(s3, "bkt", "kb", later)

    assert (
        entry["skipped"] is False and entry["bundle_version"] != first["bundle_version"]
    )


def test_metadata_only_change_is_reported_as_changed():
    s3 = FakeS3()
    publish_bundle(s3, "bkt", "kb", build_bundle([make_doc("a")]))
    entry = publish_bundle(
        s3,
        "bkt",
        "kb",
        build_bundle([make_doc("a", canonical_url="https://new.example/a")]),
    )

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

    assert (
        entry["sha256"] == first.sha256
        and f"kb/ledger/{first.version}.json" in s3.objects
    )


def test_manifest_diff_falls_back_for_legacy_ledger_entries():
    old = [{"document_id": "a", "source_revision": "1", "markdown_sha256": "x"}]
    assert manifest_diff(old, [{**old[0], "fingerprint": "f"}])["changed"] == []
