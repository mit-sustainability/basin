import gzip
import hashlib
import io
import json
import tarfile
from dataclasses import replace

import pytest

from orchestrator.knowledge.bundle import build_bundle, extract_headings, slugify
from orchestrator.knowledge.validate import validate_manifest

from .conftest import FIXTURES, make_doc


def test_headings_include_title_and_follow_beacon_heading_regex():
    md = "# Title\n\n## Overview\n\n### Sub *a*\n\n## Overview ##\n\n```\n## in fence\n```\n"
    headings = extract_headings(md)
    assert [(h["level"], h["title"], h["anchor"]) for h in headings] == [
        (1, "Title", "title"),
        (2, "Overview", "overview"),
        (3, "Sub a", "sub-a"),
        (2, "Overview", "overview-1"),
        (2, "in fence", "in-fence"),  # Beacon's regex counts fenced lines, so index alignment needs them
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
    assert {(m.mtime, m.uid, m.gid, m.mode, m.uname, m.gname) for m in members} == {(0, 0, 0, 0o644, "", "")}
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
    assert set(bundle.manifest["documents"][0]["headings"][0]) == set(fixture["documents"][0]["headings"][0])
    # The expanded archive passes the same validation as the fixture.
    with tarfile.open(fileobj=io.BytesIO(gzip.decompress(bundle.archive))) as tar:
        files = {m.name: tar.extractfile(m).read() for m in tar.getmembers()}
    validate_manifest(json.loads(files.pop("manifest.json")), files)
    assert replace(docs[0]).markdown == (FIXTURES / "valid" / docs[0].markdown_path).read_text()
