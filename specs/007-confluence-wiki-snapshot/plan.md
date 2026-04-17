# Implementation Plan: MITOS Confluence Wiki Snapshot Pipeline

**Branch**: `[007-confluence-wiki-snapshot]` | **Date**: 2026-04-02 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/007-confluence-wiki-snapshot/spec.md`

## Summary

Add a new raw Dagster asset for the `MITOS` Confluence space. The asset uses a repo-local `requests` client with PAT auth to paginate `/rest/api/content`, converts Confluence storage XHTML into markdown-like text, and persists the latest page snapshot to Postgres for downstream RAG chunking work. Incremental behavior is based on the existing raw snapshot table, not Dagster run metadata.

## Technical Context

**Language/Version**: Python 3.11
**Primary Dependencies**: Dagster, pandas, requests, SQLAlchemy
**Storage**: PostgreSQL `raw` schema via `postgres_replace`
**Testing**: pytest
**Target Platform**: Local Dagster development and the existing AWS-hosted Dagster runtime
**Project Type**: Brownfield data-platform pipeline
**Performance Goals**: Reliable full-space pagination and idempotent reruns matter more than low latency
**Constraints**: Env-var configuration, PAT-based external API access, no downstream vector ingestion in this slice
**Scale/Scope**: One new Confluence resource, one new raw asset, one manual job, focused unit tests, and baseline/backlog docs updates

## Constitution Check

- Pass: the change is derived from existing Dagster raw-asset and warehouse-write patterns.
- Pass: the new external dependency and env vars are named directly.
- Pass: the change surface is narrow and testable.
- Pass: no warehouse-layer redesign or schedule changes are introduced.

## Project Structure

### Documentation (this feature)

```text
specs/007-confluence-wiki-snapshot/
├── spec.md
└── plan.md
```

### Source Code (repository root)

```text
orchestrator/
├── assets/confluence_wiki.py
├── jobs/confluence_wiki_snapshot.py
├── resources/confluence.py
└── tests/
    ├── assets/test_confluence_wiki.py
    ├── jobs/test_confluence_wiki.py
    └── resources/test_confluence.py
```

**Structure Decision**: Keep the implementation in the existing Dagster repository layout. The Confluence API client lives in `resources/`, the raw snapshot asset lives in `assets/`, and the manual execution entry point lives in `jobs/`.
