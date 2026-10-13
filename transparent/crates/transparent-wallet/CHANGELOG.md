# Changelog

## Unreleased

- A private reply that fails on content fails one directory entry, not the
  sync: the walk finishes every other planned query and commit, and a failed
  page walk sends only the dummies valid replies committed it to. The store
  counts failures per (shard, script); an entry at three is not queried
  again. New: `SyncError::Content`, `IncompleteReason::ContentFailed`,
  `SyncReport::failed_entries`, `WalletStore::entry_failures`,
  `ShardCommit::failed_entries`. The SQLite store moves to schema 7.
- Recovery respects query and byte limits between page requests and saves checked
  fragments so one-query passes finish long histories after restarts.
- The reference SQLite store uses fresh format 6 for saved fragment progress.
  Earlier prototype companion stores must be recreated; there is no migration.
- Page requests with a finite byte budget run sequentially so at most one atomic
  response can cross the limit.
