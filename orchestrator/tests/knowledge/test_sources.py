from unittest.mock import MagicMock

import pytest

from orchestrator.knowledge.confluence_source import export_confluence, html_to_markdown
from orchestrator.knowledge.mkdocs_source import UnsupportedSyntaxError, export_mkdocs
from orchestrator.resources.confluence import ConfluencePage

NOW = "2026-10-16T00:00:00Z"


def _write(root, rel, text):
    path = root / "docs" / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_mkdocs_export_maps_paths_urls_and_titles(tmp_path):
    _write(tmp_path, "index.md", "# Home\n\nhi\n")
    _write(tmp_path, "GHG/scope_3.md", "---\ntags: x\n---\nno heading here\n")
    _write(tmp_path, "GHG/index.md", "# GHG\n")
    docs = {d.source_key: d for d in export_mkdocs(tmp_path, "https://x.io/p/", "sha1", NOW)}

    assert docs["docs/index.md"].canonical_url == "https://x.io/p/"
    assert docs["docs/GHG/index.md"].canonical_url == "https://x.io/p/GHG/"
    scope = docs["docs/GHG/scope_3.md"]
    assert scope.canonical_url == "https://x.io/p/GHG/scope_3/"
    assert scope.markdown_path == "portfolio/GHG/scope_3.md"
    assert scope.title == "Scope 3" and scope.markdown.startswith("# Scope 3\n\nno heading here")
    assert "tags: x" not in scope.markdown
    assert scope.source_revision == "sha1" and scope.source_type == "mkdocs"


@pytest.mark.parametrize("line", ["--8<-- \"a.md\"", "{{ macro() }}", "{% include 'x' %}"])
def test_mkdocs_rejects_content_altering_syntax(tmp_path, line):
    _write(tmp_path, "bad.md", f"# Bad\n\n{line}\n")
    with pytest.raises(UnsupportedSyntaxError):
        export_mkdocs(tmp_path, "https://x.io/p/", "sha1", NOW)


def test_mkdocs_allows_banned_syntax_inside_code_fences_and_admonitions(tmp_path):
    _write(tmp_path, "ok.md", "# Ok\n\n!!! note\n    careful\n\n```\n{{ literal }}\n```\n")
    assert len(export_mkdocs(tmp_path, "https://x.io/p/", "sha1", NOW)) == 1


def test_confluence_html_keeps_tables_and_drops_macro_chrome():
    html = (
        '<p>Intro <a href="/confluence/display/X/Y">link</a></p>'
        '<table><thead><tr><th>Tool</th><th>Use</th></tr></thead>'
        "<tbody><tr><td>A</td><td>B</td></tr></tbody></table>"
        '<div class="recently-updated"><h2>Recently Updated</h2></div>'
        '<input type="hidden" value="x"/><img src="/a.png"/>'
    )
    md = html_to_markdown(html, "https://wikis.mit.edu/confluence/pages/viewpage.action?pageId=1")

    assert "[link](https://wikis.mit.edu/confluence/display/X/Y)" in md
    assert "| Tool | Use |" in md and "| A | B |" in md
    assert "Recently Updated" not in md and "a.png" not in md


def _page(page_id, title, ancestors=(), html="<p>body</p>", version=2):
    return ConfluencePage(page_id, title, "", None, None, [], "", version, tuple(ancestors), html)


def test_confluence_export_builds_documents_with_hierarchy_and_dedupes():
    home = ("1", "Home")
    root = _page("10", "Guide", [home])
    child = _page("11", "Setup Steps", [home, ("10", "Guide")])
    confluence = MagicMock(base_url="https://wikis.mit.edu/confluence/")
    confluence.get_page.return_value = root
    confluence.list_descendants.return_value = [child, root]  # root repeated -> deduped

    docs = export_confluence(confluence, ("10",), NOW)

    assert [d.markdown_path for d in docs] == ["confluence/guide.md", "confluence/guide/setup-steps.md"]
    assert docs[1].document_id == "confluence-11" and docs[1].source_revision == "2"
    assert docs[1].canonical_url == "https://wikis.mit.edu/confluence/pages/viewpage.action?pageId=11"
    assert docs[1].markdown == "# Setup Steps\n\nbody\n"


def test_confluence_links_resolve_against_the_current_page():
    page = "https://wikis.mit.edu/confluence/pages/viewpage.action?pageId=1"
    md = html_to_markdown('<a href="#setup">s</a> <a href="https://x.org/a">x</a>', page)

    assert f"[s]({page}#setup)" in md and "[x](https://x.org/a)" in md
