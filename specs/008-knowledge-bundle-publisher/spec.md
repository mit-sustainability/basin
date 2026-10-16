# Feature Specification: Knowledge Bundle Publisher

**Status**: Draft
**Input**: Publish Confluence pages and a public MkDocs GitHub repo as a Beacon-compatible knowledge bundle for MCP ingestion. See `contracts/`.

## User Story

A maintainer triggers one Dagster job. It builds an immutable, checksummed bundle from the Confluence root pages and the MkDocs repo, uploads it to S3, and reports `bundle_version` + SHA-256 to paste into a reviewed Beacon deployment promotion.

## Decisions

- **Trigger**: manual job only; no schedule or sensor.
- **MkDocs**: `https://github.com/mit-sustainability/portfolio` (public, `docs/`, 11 pages at `ccd5309`), cloned at a pinned ref. The source Markdown is used as-is; a page using syntax that would alter reader-visible content (`--8<--` snippets, `{{ }}`/`{% %}` templating; admonitions like `!!! note` are passed through as-is, since their text is still readable) fails the run. No rendering.
- **Confluence**: allowlist of root page IDs (reviewed config in repo); each root plus its descendants is exported. The space home page is excluded (navigation only). Reuses `ConfluenceResource` from BL-010; fetches rendered `body.export_view` and converts with `markdownify` (storage converter has a table-flattening gap, see `plan.md`).
- **S3**: bucket `mitos-resources`, prefix `knowledgebase/`.
- **Ledger**: JSON objects in S3 beside the archive, so Beacon/MCP deploy tooling can read them.
- **Beacon deploy state is never touched.**

## Requirements

- **FR-001** Bundle matches `contracts/knowledge-bundle-contract.md` and the valid fixture.
- **FR-002** The validator rejects every `contracts/fixtures/invalid/*` case.
- **FR-003** Archive is deterministic: same inputs give the same SHA-256.
- **FR-004** Upload never overwrites an existing version (S3 conditional write).
- **FR-005** Each publish writes `knowledgebase/ledger/<bundle_version>.json` with version, SHA-256, per-document source revisions, and manifest diff vs. the previous version; a `revoked` list is read at publish time and the publisher refuses to republish a revoked version.
- **FR-006** Credentials and endpoints come from environment variables.

## Success Criteria

- **SC-001** Triggering the job produces `s3://mitos-resources/knowledgebase/<bundle_version>.tar.gz` and a ledger JSON.
- **SC-002** Unit tests cover FR-001 to FR-005 using stubbed sources.

## Open Questions

None. Confirmed config: Confluence roots `319732366`, `319723988`, `319724724`; MkDocs `site_url` `https://mit-sustainability.github.io/portfolio/`.
