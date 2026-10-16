import copy
import json

import pytest

from orchestrator.knowledge.validate import ManifestError, validate_manifest

from .conftest import FIXTURES

VALID = json.loads((FIXTURES / "valid/manifest.json").read_text())


def _files(manifest):
    return {d["markdown_path"]: (FIXTURES / "valid" / d["markdown_path"]).read_bytes() for d in manifest["documents"]}


def test_valid_fixture_is_accepted_with_its_files():
    validate_manifest(VALID, _files(VALID))


@pytest.mark.parametrize("fixture", sorted((FIXTURES / "invalid").glob("*/manifest.json")), ids=lambda p: p.parent.name)
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
        lambda m: m["documents"][1].update(document_id=m["documents"][0]["document_id"]),
        lambda m: m["documents"][1].update(markdown_path=m["documents"][0]["markdown_path"]),
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
