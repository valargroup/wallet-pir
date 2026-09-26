# Receiver directory changelog

## Unreleased

- Add authenticated zero-OVK receiver extraction and shared, versioned payment
  records with pagination, coverage validation, and deterministic PIR row layouts.
- Add a standalone, resumable mainnet indexer with coinbase exclusion, atomic
  block storage, bounded concurrent downloads, batch anchor validation, reorg
  rollback, and immutable local publications.
- Add reusable encrypted receiver lookups bound to one accepted publication,
  with bounded HTTP downloads and complete-history pagination checks.
