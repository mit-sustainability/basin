# Feature Specification: Website Content Health Scan

**Feature Branch**: `[website-content-health]`
**Created**: 2026-03-23
**Status**: Draft
**Input**: User description: "Create a Dagster asset that uses Playwright to scan and scrape https://sustainability.mit.edu, support manual and scheduled execution, and output page and hyperlink health tables."

## User Scenarios & Testing

### User Story 1 - Materialize website scans

An operator needs to run a full scan of `https://sustainability.mit.edu` through Dagster without manually enumerating page URLs.

**Independent Test**: Dagster exposes a website content health job that discovers and scans the configured owned site sections without manual URL enumeration.

### User Story 2 - Inspect page content freshness

A maintainer needs a table of scanned pages that captures page URL, content, category, and the most recent update signal detected on the page.

**Independent Test**: A successful scan writes a page table row per page with category/topic, update metadata, and content payload fields.

### User Story 3 - Inspect hyperlink health

A maintainer needs a table of hyperlinks discovered on the scanned pages so downstream tools can review broken or stale URLs.

**Independent Test**: A successful scan writes one row per discovered hyperlink with source page, normalized URL, and a health classification.

## Functional Requirements

- The implementation MUST discover crawl targets from the configured MIT Sustainability section roots rather than a hard-coded page list of individual pages.
- The implementation MUST use Playwright to render and scrape page content for each discovered page in scope.
- The implementation MUST output one table for scanned pages and one table for discovered hyperlinks.
- The implementation MUST support manual execution from Dagster and a recurring scheduled execution path for the full scan job.
- The implementation MUST keep external configuration and runtime dependencies explicit, including the Playwright browser requirement.

## Success Criteria

- Dagster loads a dedicated job for website content health scans.
- A recurring scheduled execution runs the full website content health job.
- The page table contains enough content and freshness metadata to support downstream summarization or change detection.
- The hyperlink table contains enough URL health metadata to support downstream remediation workflows.
