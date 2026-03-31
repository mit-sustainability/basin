# Implementation Plan: Website Content Health Scan

**Branch**: `[website-content-health]` | **Date**: 2026-03-23 | **Spec**: [spec.md](./spec.md)

## Summary

Add a new website-content-health domain to the orchestrator. The slice uses owned MIT Sustainability section roots as the crawl frontier, recursively discovers in-scope pages, scrapes each page with Playwright, writes page and link outputs to PostgreSQL through the existing pandas IO manager, and schedules the full job to run on a recurring cadence.

## Touched Paths

- `orchestrator/assets/website_content_health.py`
- `orchestrator/jobs/website_content_health.py`
- `orchestrator/schedules/mitos_warehouse.py`
- `orchestrator/__init__.py`
- `orchestrator/tests/assets/test_website_content_health.py`
- `orchestrator/tests/jobs/test_website_content_health.py`
- `orchestrator/requirements.in`
- `orchestrator/requirements.txt`
- `orchestrator/setup.py`
- `orchestrator/Dockerfile`
- `specs/BACKLOG.md`
- `specs/001-platform-baseline/`

## Technical Decisions

- Use recursive discovery from owned section seed URLs to avoid hard-coding individual pages while keeping the crawl bounded.
- Use a single multi-asset to avoid rendering each page twice when producing the two output tables.
- Replace raw outputs on each materialization and include `scanned_at` in the outputs for run-level freshness.
- Validate unique internal and external HTTP links directly and attach those validation results back to source-page link rows.

## Verification

- Add focused unit tests for recursive discovery, metadata extraction helpers, link validation, and schedule registration behavior.
- Run targeted pytest modules for the new asset and schedule logic.
