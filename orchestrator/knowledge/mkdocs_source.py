"""Export a MkDocs repo's Markdown source as knowledge Documents."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

from orchestrator.knowledge.bundle import FENCE, HEADING, Document, slugify

# Syntax that would change reader-visible content if left unrendered.
BANNED = (re.compile(r"--8<--"), re.compile(r"\{\{"), re.compile(r"\{%"))
FRONT_MATTER = re.compile(r"\A---\n.*?\n---\n", re.S)


class UnsupportedSyntaxError(ValueError):
    """Raised when a page uses syntax that this exporter does not render."""


def clone(repo_url: str, ref: str, dest: Path) -> str:
    """Shallow-fetch `ref` (branch, tag or SHA) into dest and return the commit SHA."""
    for args in (["init", "-q"], ["fetch", "-q", "--depth", "1", repo_url, ref], ["checkout", "-q", "FETCH_HEAD"]):
        subprocess.run(["git", *args], cwd=dest, check=True, capture_output=True, text=True)
    return subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=dest, check=True, capture_output=True, text=True
    ).stdout.strip()


def _check_syntax(rel: str, text: str) -> None:
    in_fence = False
    for line in text.splitlines():
        if FENCE.match(line):
            in_fence = not in_fence
        elif not in_fence and any(p.search(line) for p in BANNED):
            raise UnsupportedSyntaxError(f"{rel}: unsupported syntax in line {line.strip()!r}")


def _canonical_url(site_url: str, rel: Path) -> str:
    parts = list(rel.with_suffix("").parts)
    if parts and parts[-1] == "index":
        parts.pop()
    return site_url.rstrip("/") + "/" + "".join(f"{p}/" for p in parts)


def export_mkdocs(
    repo_dir: Path, site_url: str, revision: str, exported_at: str, docs_dir: str = "docs"
) -> list[Document]:
    root = repo_dir / docs_dir
    documents = []
    for path in sorted(root.rglob("*.md")):
        rel = path.relative_to(root)
        text = FRONT_MATTER.sub("", path.read_text(encoding="utf-8"))
        _check_syntax(f"{docs_dir}/{rel}", text)
        h1 = next((m.group(2) for m in map(HEADING.match, text.splitlines()) if m and len(m.group(1)) == 1), None)
        title = h1 or rel.stem.replace("_", " ").replace("-", " ").title()
        if not h1:
            text = f"# {title}\n\n{text}"
        documents.append(
            Document(
                document_id="mkdocs-" + slugify(rel.with_suffix("").as_posix().replace("/", "-")),
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
