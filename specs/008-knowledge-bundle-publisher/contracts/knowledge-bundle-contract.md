# Knowledge Bundle Contract

Dagster produces this contract; Beacon consumes it. Neither process contacts the other at request time.

## Expanded volume

The init container extracts one archive into `/knowledge` and writes:

```text
/knowledge/
  manifest.json
  .verified-sha256
  <manifest Markdown paths>
```

`.verified-sha256` is a single UTF-8 line containing the SHA-256 pinned by
deployment configuration. The init container verifies the downloaded archive
before extraction, rejects links and special files, and writes this marker only
after successful verification.

## Manifest

`manifest.json` is UTF-8 JSON with `bundle_version` and `documents`. Each
document has `document_id`, `markdown_path`, `source_type` (`mkdocs` or
`confluence`), `source_key`, `canonical_url`, `source_revision`, `exported_at`,
`title`, and ordered headings. Each heading has `title`, `level`, `anchor`, and
`path`.

Document IDs, Markdown paths, and heading anchors are unique. Markdown paths
are relative `.md` paths and may not contain `..`. Markdown is UTF-8. Dagster
uses rendered Confluence content and either renders MkDocs or rejects source
syntax that would alter reader-visible content.

## Archive and publication

Dagster owns normalization, deterministic archive creation, SHA-256
calculation, and S3 upload. It sorts archive members, fixes tar owner, group,
mode, and timestamp metadata, uses gzip `mtime=0`, and never overwrites a
published version. A reviewed deployment promotion supplies both the bundle
version and checksum to Beacon.

## Conformance fixtures

`tests/fixtures/knowledge/valid` is the minimum accepted expanded bundle.
`tests/fixtures/knowledge/invalid` contains one deliberately invalid condition
per fixture. Dagster producer tests must generate a bundle compatible with the
valid fixture. Beacon consumer tests load the expanded valid fixture and reject
every invalid fixture.
