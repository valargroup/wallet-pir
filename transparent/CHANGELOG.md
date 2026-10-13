# Transparent history recovery changelog

Covers the crates a wallet uses to recover transparent history over PIR:
`transparent-events`, `transparent-filter`, `transparent-native`,
`transparent-shard`, `transparent-wallet`, `transparent-wallet-store`, and the
`transparent-shard-server`, `transparent-filter-server` and
`transparent-block-server` services. Txid display lookups have their own
[changelog](CHANGELOG-txid.md). Both are released under one tag.

## Unreleased

Versions are `0.1.0-rc.4`; not yet published.

- **A bad reply fails one entry, not the sync.** A private reply that fails
  to decode or verify, a page link outside the shard's tables, or a record
  that contradicts the store now fails one directory entry (one script in
  one shard), reported as `SyncError::Content` where it cannot be. The walk
  finishes every other planned query in the same order: each script's
  directory queries are all sent before any is decoded, the shard's commit
  still lands for its other scripts, and a failed page walk sends only the
  dummy queries valid replies had committed it to. A malformed map or
  manifest, a broken filter and a lost answer still stop the sync.
- **Three failures, then the entry is held.** The store counts content
  failures per (shard, script) and never resets them: `WalletStore` gains
  `entry_failures`, and `ShardCommit` gains `failed_entries`. An entry at
  `MAX_CONTENT_FAILURES` (3) is not queried again. `SyncReport` and
  `SyncStatus` list `failed_entries`, and an incomplete sync they explain
  reports `IncompleteReason::ContentFailed` (`content-failed`). Request
  failures (refusals, overload, stale revisions) do not count.
- **Store schema 7, no migration.** `transparent-wallet-store` adds the
  failure counts and refuses a schema 6 store, which must be recreated.

## 0.1.0-rc.3 — 2026-10-13

No history recovery changes; released with the txid display's `txid-4k8`
tables ([txid changelog](CHANGELOG-txid.md)).

## 0.1.0-rc.2 — 2026-10-12

First release published to crates.io, so wallets can depend on registry
versions (`=0.1.0-rc.2`) instead of a git tag. No behaviour changes from
0.1.0-rc.1.

- The ipir-sp family now comes from crates.io at the same versions:
  `ipir-sp 0.1.0-rc.7`, `reinspiring 0.1.3-rc.1`. A workspace can hold only one
  `ipir-sp 0.1.0-rc.*` from crates.io, so anything else in it must use rc.7 too.
- The shared helpers these crates need, `pir-native`, `pir-control` and
  `pir-observability`, are published at 0.1.0.

## 0.1.0-rc.1 — 2026-10-12

First tagged release. Earlier wallet-libraries builds pinned a bare revision;
this candidate's code matches the last such pin (`3e705bb3`), so moving to the
tag changes no behaviour, only the version requirement (`=0.1.0-rc.1`).

- **Wallet API v2.** Wallet-facing paths name their protocol version, and
  these crates speak `/v2` only: `/v2/shards` and `/v2/filters/shards`. The
  public origin keeps serving `/v1` (schema v11 history) beside it for wallets
  already released. Operator probes stay at `/v1/health`, `/v1/ready` and
  `/v1/status`.
- **Schema v12 history: directory shards with separate page shards.** One
  directory query per matched script finds where its latest run of events
  sits; each run points back to the one before it, so the wallet walks the
  chain one round per run, batched across scripts. The walk refuses truncated
  chains, forward or self pointers, a run count that does not drop by one per
  hop, and page manifests whose ranges overflow their directory.
- **8-bit native profile.** Directory and page tables use ipir-sp
  `v0.1.0-rc.7`'s 8-bit profile with a one-digit packing key: one byte per
  coefficient, 22-bit published masks, 28-bit dithered queries and 10-bit
  responses. Servers accept only the 28-bit query lengths; there is no 16-bit
  fallback on `/v2`.
- **Run cache.** Runs in sealed page shards are cached by page shard and
  script, each holding only its own events, so a tip catch-up stops at the
  first cached run. A script's cached runs are dropped once a commit covers it
  over the whole sealed directory shard.
- **Resumable recovery.** Checked run fragments are saved between recovery
  passes, query batches are capped, and each response is checked against a
  finite byte budget, so an interrupted sync resumes from saved progress.
- **Declared re-cuts.** The shard map lists every re-cut it has made (epoch,
  first changed height, the entries it superseded). Wallets keep the history
  they hold across a declared re-cut, redo only unfinished work, and refuse a
  map that rewrites sealed content at or below an accepted block without a
  declaration (`MapDiverged`).
- **Store schema 6, no migration.** `transparent-wallet-store` refuses a
  SQLite store written by an earlier schema; a wallet upgrading from a
  revision pin must start a fresh store and recover again.
