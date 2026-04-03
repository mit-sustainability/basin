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
| BL-007 | Add MITOS Confluence wiki snapshot pipeline | in_progress | pipeline | `orchestrator/assets`, `orchestrator/resources`, `orchestrator/jobs`, `orchestrator/tests`, `specs/BACKLOG.md`, `specs/001-platform-baseline/` | `007-confluence-wiki-snapshot` | `specs/007-confluence-wiki-snapshot/` | Fetch paginated Confluence pages from the `MITOS` space with PAT auth, convert storage XHTML to Markdown, normalize page URLs and labels, and persist raw page snapshots in Postgres for downstream RAG processing. |
| BL-008 | Add wiki chunk handoff to RAG ingestion API | proposed | resource | `orchestrator/assets`, `orchestrator/resources`, `orchestrator/tests`, `specs/001-platform-baseline/` |  | optional | Chunk normalized wiki Markdown, attach citation metadata, and upsert changed chunks to the downstream vector-ingestion API without creating duplicate embeddings. |

## Maintenance Rules

- Add a backlog entry before opening a new feature branch unless the work is an emergency fix already in progress.
- Use backlog rows as the default unit of parallel work for agents.
- Keep each row narrow enough that one agent can own it without stepping on unrelated work.
- When a branch is created, add the branch name to the relevant backlog row.
- Mark `Spec` as `no` by default. Use `optional` or a concrete spec path only when the work crosses the threshold described in `specs/README.md`.
- When scope changes materially, update both the backlog row and the feature spec if one exists.
- Remove as little history as possible; prefer changing status over deleting rows.
