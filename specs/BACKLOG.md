# Known Backlog

Use this file as the durable, human-readable backlog for work that is not yet complete.

## Status Rules

- `proposed`: noted, not yet shaped enough to start
- `ready`: sufficiently shaped to branch and execute
- `in_progress`: active branch or active implementation exists
- `blocked`: cannot proceed without an external dependency or decision
- `done`: completed and merged
- `dropped`: intentionally not pursuing

## Type Rules

- `pipeline`: new or changed Dagster asset flow
- `dbt`: new or changed dbt models, sources, or warehouse outputs
- `resource`: external connector, IO manager, resource, or client behavior
- `utility`: shared helper or refactor with narrow scope
- `ops`: schedules, sensors, deployment, CI, infra glue, environment handling
- `docs`: baseline docs, workflow docs, catalog docs, or backlog maintenance

## Backlog

| ID | Title | Status | Type | Scope | Branch | Spec | Outcome |
|----|-------|--------|------|-------|--------|------|---------|
| BL-001 | Split platform baseline into domain specs | proposed | docs | `specs/001-platform-baseline/` |  | optional | Break the brownfield baseline into smaller domain maps for food, travel, utilities, waste, and inventory work. |
| BL-002 | Improve orchestrator test coverage | proposed | pipeline | `orchestrator/tests`, `orchestrator/jobs`, `orchestrator/assets` |  | no | Add tests for more asset modules, job selections, schedules, and sensors so runtime behavior is less dependent on manual verification. |
| BL-003 | Document runtime environment contracts | proposed | docs | `specs/001-platform-baseline/`, `README.md`, workflow docs |  | optional | Make environment variables, secrets expectations, and service dependencies easier to audit across local, CI, and deployment flows. |
| BL-004 | Simplify spec workflow for data-platform work | in_progress | docs | `AGENTS.md`, `specs/README.md`, `specs/BACKLOG.md`, `.specify/memory/constitution.md` | `website-content-health` | no | Shift the repo to a backlog-first workflow where feature spec folders are optional for routine pipeline, dbt, resource, and utility work. |
| BL-005 | Add repo-local execution, review, and test personas | in_progress | docs | `.agents/skills`, `AGENTS.md` | `website-content-health` | no | Define Codex skills for Dagster asset implementation, code review, and unit-test coverage so parallel agents can take clearer roles. |
| BL-006 | Add MIT Sustainability website content health scan | done | pipeline | `orchestrator/assets/website_content_health.py`, `orchestrator/jobs/website_content_health.py`, `orchestrator/schedules/mitos_warehouse.py`, `orchestrator/tests/`, `specs/002-website-content-health/` | `website-content-health` | `specs/002-website-content-health/` | Scrape MIT Sustainability pages into partitioned Dagster assets and record page content plus hyperlink health for scheduled monitoring. |
| BL-008 | Refresh local Intel workspace bootstrap and dependency pins | done | ops | `.vscode`, `Makefile`, `README.md`, `orchestrator/requirements*`, `orchestrator/setup.py`, `warehouse/requirements.txt`, `specs/001-platform-baseline/` | `codex/automate-billing-downloads` | no | Opening BASIN in VS Code starts an Intel terminal on `oracle_client`, and local setup refreshes the pinned Dagster, dbt, and Playwright runtime consistently. |
| BL-009 | Add selective ECS Fargate Spot offload for heavy assets | in_progress | ops | `orchestrator/assets/website_content_health.py`, `orchestrator/resources/`, `docker-compose.yaml`, `specs/001-platform-baseline/`, `orchestrator/tests/` |  | optional | Launch selected heavy assets as ad hoc ECS Fargate Spot tasks while keeping the main Dagster instance on EC2 and preserving Postgres-backed outputs. |
| BL-010 | Add MITOS Confluence wiki snapshot pipeline | in_progress | pipeline | `orchestrator/assets`, `orchestrator/resources`, `orchestrator/jobs`, `orchestrator/tests`, `specs/BACKLOG.md`, `specs/001-platform-baseline/` | `007-confluence-wiki-snapshot` | `specs/007-confluence-wiki-snapshot/` | Fetch paginated Confluence pages from the `MITOS` space with PAT auth, convert storage XHTML to Markdown, normalize page URLs and labels, and persist raw page snapshots in Postgres for downstream RAG processing. |
| BL-011 | Add wiki chunk handoff to RAG ingestion API | proposed | resource | `orchestrator/assets`, `orchestrator/resources`, `orchestrator/tests`, `specs/001-platform-baseline/` |  | optional | Chunk normalized wiki Markdown, attach citation metadata, and upsert changed chunks to the downstream vector-ingestion API without creating duplicate embeddings. |
| BL-012 | Knowledge bundle core: models, validators, deterministic archive | done | utility | `orchestrator/knowledge/`, `orchestrator/tests/` | `knowledge-bundle-publisher` | `specs/008-knowledge-bundle-publisher/` | Pure-Python Document model, manifest writer, heading extractor, validator (rejects all invalid fixtures), and deterministic tar.gz + SHA-256 that conforms to the Beacon contract. |
| BL-013 | Knowledge publish to S3 with ledger | done | resource | `orchestrator/assets/knowledge.py`, `orchestrator/tests/` | `knowledge-bundle-publisher` | `specs/008-knowledge-bundle-publisher/` | Conditional-write upload of the archive, ledger JSON with manifest diff, and revoked-version refusal. Depends on BL-012. |
| BL-014 | Knowledge source: Confluence root-page export | done | pipeline | `orchestrator/resources/confluence.py`, `orchestrator/assets/knowledge.py` | `knowledge-bundle-publisher` | `specs/008-knowledge-bundle-publisher/` | Reuse `ConfluenceResource` (add `ancestors`, `list_descendants`, `export_view`) with a `markdownify` converter to export allowlisted root pages and descendants as Documents. |
| BL-015 | Knowledge source: public MkDocs repo export | done | pipeline | `orchestrator/assets/knowledge.py`, `orchestrator/tests/` | `knowledge-bundle-publisher` | `specs/008-knowledge-bundle-publisher/` | Clone the public repo at a pinned ref, fail on content-altering syntax, emit Documents. Depends on BL-012. |
| BL-016 | Knowledge publish job wiring and baseline docs | done | ops | `orchestrator/__init__.py`, `orchestrator/jobs/knowledge_publish.py`, `specs/001-platform-baseline/` | `knowledge-bundle-publisher` | `specs/008-knowledge-bundle-publisher/` | Manual `knowledge_publish_job`, env var docs, and runtime-contract updates. Depends on BL-013 to BL-015. |

## Maintenance Rules

- Add a backlog entry before opening a new feature branch unless the work is an emergency fix already in progress.
- Use backlog rows as the default unit of parallel work for agents.
- Keep each row narrow enough that one agent can own it without stepping on unrelated work.
- When a branch is created, add the branch name to the relevant backlog row.
- Mark `Spec` as `no` by default. Use `optional` or a concrete spec path only when the work crosses the threshold described in `specs/README.md`.
- When scope changes materially, update both the backlog row and the feature spec if one exists.
- Remove as little history as possible; prefer changing status over deleting rows.
