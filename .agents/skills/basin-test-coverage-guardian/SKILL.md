---
name: "basin-test-coverage-guardian"
description: "Use when BASIN code changes introduce or alter behavior. Focus on adding or improving unit tests for new pipelines, assets, resources, helpers, schedules, and other repository logic."
compatibility: "BASIN repository with pytest-based orchestrator tests"
metadata:
  author: "repo-local"
  source: "AGENTS.md and repository workflow"
---

# BASIN Test Coverage Guardian

Use this skill whenever a change adds or alters behavior. The default assumption is that new behavior should come with targeted tests.

## Required Context

Check:

- `AGENTS.md`
- `specs/BACKLOG.md`
- existing tests under `orchestrator/tests/`
- the changed implementation files

## Coverage Policy

- New behavior should normally add new tests.
- Changed behavior should normally update existing tests or add focused regression tests.
- Do not rely only on manual verification when a deterministic unit test is feasible.

## Test Design Rules

- Prefer narrow unit tests over expensive end-to-end coverage.
- Test the behavior that changed, not the entire platform.
- For asset code, isolate pure transforms, failure conditions, metadata shaping, or resource interactions where possible.
- For resource code, test auth, request shaping, retries, and failure handling using mocks or fixtures.
- For utilities, test edge cases and reused paths directly.
- For schedules and sensors, test the decision logic or selection behavior when practical.

## When Coverage Is Hard

If a deterministic test is not feasible:

1. explain why
2. add the smallest useful seam or helper you can test
3. document the residual verification gap in the review or final summary

## Deliverable

A patch that improves confidence in the changed behavior, ideally by adding or updating pytest coverage in the closest relevant test module.
