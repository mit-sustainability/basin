# Contracts

Verbatim copies of the Beacon-owned contract (source: `beacon` repo, commit `71c14a2`, `docs/` and `tests/fixtures/knowledge/`). Beacon is the consumer and owns the contract; re-copy when it changes, do not edit here.

- `knowledge-bundle-contract.md` - bundle layout, manifest schema, archive rules
- `knowledge-connector-operations.md` - publisher/deployer responsibilities
- `fixtures/valid/` - minimum accepted bundle; producer tests must generate a bundle compatible with it
- `fixtures/invalid/` - one invalid condition per fixture; the producer's validator must reject each
