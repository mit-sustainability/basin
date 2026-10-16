# Knowledge Connector Operations

Dagster is the publisher. It selects the reviewed MkDocs root and Confluence
allowlist, supplies rendered/validated Markdown to its archive builder, uploads
one immutable S3 object, and records the version, SHA-256, source revisions,
and manifest diff. It does not update Beacon deployment state.

A reviewed deployment promotion pins `BEACON_KNOWLEDGE_VERSION` and
`BEACON_KNOWLEDGE_SHA256`. The init container downloads that exact object,
verifies its SHA-256 before extraction, writes `/knowledge/.verified-sha256`,
and mounts `/knowledge` read-only into Beacon. The MCP container has no S3,
Git, or Confluence credentials.

The S3 bucket must prevent overwrite of a published version. Rollback uses a
previous reviewed version but must refuse versions listed as revoked after a
content takedown. Rollouts may briefly serve two versions; citations expose the
version and deployment policy must bound that window.

The contract fixtures in `tests/fixtures/knowledge/` are producer/consumer
conformance fixtures. Dagster tests its generated bundle against them; Beacon
tests loading them locally. Before changing retrieval, evaluate representative
real office questions against the first production bundle and record recall at
five.
