from pathlib import Path

from orchestrator.knowledge.bundle import Document

FIXTURES = Path(__file__).resolve().parents[3] / "specs/008-knowledge-bundle-publisher/contracts/fixtures"


def make_doc(document_id="doc-a", markdown="# A\n\n## Overview\n\ntext\n", **overrides) -> Document:
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
