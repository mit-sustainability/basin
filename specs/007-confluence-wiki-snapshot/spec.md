# Feature Specification: MITOS Confluence Wiki Snapshot Pipeline

**Feature Branch**: `[007-confluence-wiki-snapshot]`
**Created**: 2026-04-02
**Status**: Draft
**Input**: User description: "Build the first step of a local RAG system by scraping MIT Office wiki pages from Confluence through the REST API."

## User Scenarios & Testing

### User Story 1 - Materialize raw wiki snapshots (Priority: P1)

A maintainer needs a Dagster asset that fetches all Confluence pages in the `MITOS` space and stores the latest normalized snapshot in the warehouse so downstream RAG work has a stable source.

**Independent Test**: Run the manual job and verify the `raw.confluence_mitos_pages` snapshot contains page identity, content, labels, version, and sync metadata.

**Acceptance Scenarios**:

1. **Given** valid Confluence credentials, **When** the asset runs, **Then** it paginates through the `MITOS` space and stores one latest row per page in the raw warehouse.
2. **Given** a page already exists in the raw snapshot, **When** the upstream Confluence `version` or `updated_at` is unchanged, **Then** the asset reuses the existing stored snapshot instead of reprocessing the page body.

### User Story 2 - Normalize Confluence storage content (Priority: P2)

A downstream RAG workflow needs wiki content in markdown-like plain text with stable URLs and metadata.

**Independent Test**: Unit tests can show Confluence storage XHTML is cleaned, markdown is produced, and relative links become absolute wiki URLs.

**Acceptance Scenarios**:

1. **Given** page body XHTML with Confluence-only macro/layout tags, **When** the converter runs, **Then** those tags are removed from the markdown output.
2. **Given** page body links such as `/display/MITOS/Runbook`, **When** the converter runs, **Then** the resulting markdown contains absolute URLs rooted at the configured Confluence base URL.

## Requirements

### Functional Requirements

- **FR-001**: The implementation MUST add a repo-local Confluence resource that authenticates with a PAT and calls the Confluence REST API directly via `requests`.
- **FR-002**: The resource MUST page through `/rest/api/content` for the configured space until no more page results remain.
- **FR-003**: The extraction asset MUST persist a latest-page snapshot to `raw.confluence_mitos_pages`.
- **FR-004**: The raw snapshot MUST include `page_id`, `title`, `page_url`, `author`, `updated_at`, `labels`, `storage_xhtml`, `markdown_content`, `content_hash`, `version`, and `synced_at`.
- **FR-005**: The asset MUST use existing persisted snapshot state, not Dagster run metadata, to decide whether a page needs refresh.
- **FR-006**: The first slice MUST not call the downstream RAG/vector ingestion API.
- **FR-007**: The implementation MUST add a manual Dagster job for this asset and MUST NOT add a schedule or sensor in this slice.
- **FR-008**: Baseline docs and backlog entries MUST be updated for the new Confluence integration and manual execution path.

## Success Criteria

- **SC-001**: Running the manual job produces a warehouse-backed Confluence snapshot table with one latest row per current `MITOS` page.
- **SC-002**: Unit tests cover pagination, PAT auth, XHTML normalization, link rewriting, and incremental refresh decisions.
- **SC-003**: Repository docs identify the new env vars `CONFLUENCE_BASE_URL`, `CONFLUENCE_PAT`, and `CONFLUENCE_SPACE_KEY`.
