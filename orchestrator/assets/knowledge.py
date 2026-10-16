"""Publish MkDocs and Confluence content as an immutable Beacon knowledge bundle.

Contract: specs/008-knowledge-bundle-publisher/contracts/. Dagster publishes to S3 only;
it never touches Beacon deployment state.
"""

from __future__ import annotations

import tempfile
from datetime import datetime, timezone
from pathlib import Path

from dagster import Output, ResourceParam, asset, get_dagster_logger
from dagster_aws.s3 import S3Resource

from orchestrator.knowledge import config
from orchestrator.knowledge.bundle import Bundle, Document, build_bundle
from orchestrator.knowledge.confluence_source import export_confluence
from orchestrator.knowledge.mkdocs_source import clone, export_mkdocs
from orchestrator.knowledge.publish import publish_bundle
from orchestrator.resources.confluence import ConfluenceResource

logger = get_dagster_logger()
GROUP = "knowledge"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@asset(name="knowledge_mkdocs_documents", group_name=GROUP, compute_kind="python")
def knowledge_mkdocs_documents() -> Output[list[Document]]:
    """Clone the public MkDocs repo at the configured ref and export its Markdown pages."""

    with tempfile.TemporaryDirectory() as tmp:
        revision = clone(config.MKDOCS_REPO_URL, config.MKDOCS_REF, Path(tmp))
        documents = export_mkdocs(Path(tmp), config.MKDOCS_SITE_URL, revision, _now())
    return Output(documents, metadata={"documents": len(documents), "revision": revision})


@asset(name="knowledge_confluence_documents", group_name=GROUP, compute_kind="python")
def knowledge_confluence_documents(confluence: ResourceParam[ConfluenceResource]) -> Output[list[Document]]:
    """Export the allowlisted Confluence root pages and their descendants as rendered Markdown."""

    documents = export_confluence(confluence, config.CONFLUENCE_ROOT_PAGE_IDS, _now())
    return Output(documents, metadata={"documents": len(documents)})


@asset(name="knowledge_bundle", group_name=GROUP, compute_kind="python")
def knowledge_bundle(
    knowledge_mkdocs_documents: list[Document], knowledge_confluence_documents: list[Document]
) -> Output[Bundle]:
    """Validate and package both sources into a deterministic archive."""

    bundle = build_bundle([*knowledge_mkdocs_documents, *knowledge_confluence_documents])
    return Output(
        bundle,
        metadata={
            "bundle_version": bundle.version,
            "sha256": bundle.sha256,
            "documents": len(bundle.documents),
            "archive_bytes": len(bundle.archive),
        },
    )


@asset(name="knowledge_published", group_name=GROUP, compute_kind="s3")
def knowledge_published(knowledge_bundle: Bundle, s3: S3Resource) -> Output[dict]:
    """Upload the archive and ledger to S3 (never overwriting) and report the pin values."""

    entry = publish_bundle(s3.get_client(), config.S3_BUCKET, config.S3_PREFIX, knowledge_bundle)
    logger.info(
        "BEACON_KNOWLEDGE_VERSION=%s BEACON_KNOWLEDGE_SHA256=%s (%s)",
        entry["bundle_version"],
        entry["sha256"],
        "unchanged, not uploaded" if entry["skipped"] else "published",
    )
    return Output(
        entry,
        metadata={
            "BEACON_KNOWLEDGE_VERSION": entry["bundle_version"],
            "BEACON_KNOWLEDGE_SHA256": entry["sha256"],
            "skipped": entry["skipped"],
            "diff": entry["diff"],
        },
    )
