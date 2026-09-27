# Receiver directory changelog

## Unreleased

- Add common inclusion-proof snapshots, bounded HTTP retrieval and a local refresh
  helper. Commitment indexing preserves coinbase positions and rewind semantics.

- Add authenticated zero-OVK receiver extraction and shared, versioned payment
  records with pagination, coverage validation, and deterministic PIR row layouts.
- Add a standalone, resumable mainnet indexer with coinbase exclusion, atomic
  block storage, bounded concurrent downloads, batch anchor validation, reorg
  rollback, and immutable local publications. Continuous serving follows the
  canonical tip, rotates publications without restarting HTTP, and revokes
  orphaned sessions using the Enhance publication lifecycle.
- Add reusable encrypted receiver lookups bound to one accepted publication,
  with bounded HTTP downloads, revision-bound setup and witnesses, and
  complete-history pagination checks.
- Add a bounded loopback PIR service and a public mainnet refund lookup test.
- Add shared compact/Enhance reconstruction and verify a real refund through
  both encrypted lookups with authenticated output recovery.
