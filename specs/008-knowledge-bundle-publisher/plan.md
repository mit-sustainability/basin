# Plan: Knowledge Bundle Publisher

## Assets (group `knowledge`)

`knowledge_mkdocs_documents` and `knowledge_confluence_documents` -> `knowledge_bundle` -> `knowledge_published`

Source assets emit `Document` records (manifest fields plus Markdown body). `knowledge_bundle` validates and builds manifest + archive in memory. `knowledge_published` uploads and writes the ledger. Job: `knowledge_publish_job`, manual.

## Touched paths

- `orchestrator/knowledge/` (new, pure Python, no Dagster imports): `models.py` (Document, manifest writer), `headings.py`, `validate.py`, `archive.py`
- `orchestrator/assets/knowledge.py`, `orchestrator/jobs/knowledge_publish.py`
- `orchestrator/resources/confluence.py` (`ancestors` and `export_html` on `ConfluencePage`, plus `get_page`, `list_children`, `list_descendants`; descendants walk `/child/page` because `/descendant/page` returns HTTP 500 on wikis.mit.edu)
- `orchestrator/__init__.py` (register assets/job; reuse `S3Resource`)
- `orchestrator/tests/`
- `specs/001-platform-baseline/` (runtime contracts: new env vars, S3 prefix, job)

## Confluence reuse (from BL-010)

- Reuse `ConfluenceResource` (auth, paging, `_normalize_page`); extend with `ancestors` and `list_descendants(root_id)`, and request `body.export_view`.
- **Convert rendered HTML with `markdownify` + `beautifulsoup4`, not `ConfluenceStorageToMarkdown`.** Do not depend on `raw.confluence_mitos_pages` (space-wide, no ancestors).
- Before conversion, decompose `script, style, .hidden, form, input, .recently-updated, .plugin_pagetree, .aui-icon` (macro UI chrome).
- `canonical_url` uses the `?pageId=` form, not `_links.webui`, so it survives renames.

### Evidence (2026-10-16, 5 MITOS pages, `wikis.mit.edu/confluence`)

- Storage converter flattens tables: `ToolCore CapabilityWho Can Get It & How`, rows run together. `markdownify` on `export_view` yields a correct Markdown table.
- `export_view` of the space home page is 11 KB vs. 1 KB storage, almost all macro UI (recently-updated, pagetree). After the strip list above it is 314 chars of real text.
- Simple pages (headings, lists, links) convert equivalently either way.
- Not verified: include/excerpt-include expansion. None of the 5 sampled pages use them. Re-check when a root page that does is added.

## Field mapping

| Field | MkDocs | Confluence |
|---|---|---|
| `source_key` | repo path (`docs/x.md`) | page ID |
| `source_revision` | commit SHA | `version.number` as string |
| `canonical_url` | `site_url` + built URL | `<base>/pages/viewpage.action?pageId=<id>` |
| `markdown_path` | `<section>/<slug>.md` | `confluence/<root slug>/<ancestor slugs>/<slug>.md` |
| `headings.anchor` | MkDocs-style slug, duplicates `-1`, `-2` | same |
| `headings.path` | ancestors joined with ` / ` | same |

`bundle_version` = `YYYYMMDD-<first 8 of manifest sha256>`; unchanged content short-circuits with no publish.

## Env vars (new)

`KNOWLEDGE_S3_BUCKET` (`mitos-resources`), `KNOWLEDGE_S3_PREFIX` (`knowledgebase`), `KNOWLEDGE_MKDOCS_REPO_URL` (`https://github.com/mit-sustainability/portfolio`), `KNOWLEDGE_MKDOCS_REF`, `KNOWLEDGE_MKDOCS_SITE_URL` (`https://mit-sustainability.github.io/portfolio/`); Confluence vars as in BL-010. Root page IDs (`319732366`, `319723988`, `319724724`) live in a reviewed config file.

## Verification

Conformance against `contracts/fixtures`, per-rule validator tests, determinism (build twice, equal SHA-256), heading extractor duplicates, S3 immutability (second put fails), revoked-version refusal.
