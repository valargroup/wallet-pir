# Receiver directory changelog

## Unreleased

- Add the receiver directory: authenticated zero-OVK receiver extraction,
  paginated fixed-width records, deterministic publications from 8192 to 65536
  rows that retry up to 16 salts before doubling, `IWFLT1` files of labeled BIP 158
  receiver sets (paid receivers, plus each swap provider's recent and seen sets as
  the manifest declares them, with when its feed started and last completed a read;
  directory profile `ironwood-zero-ovk-receiver-v1`, whose revision hashes every
  field at fixed width and refuses unknown ones), and common `IWPROOF1` witness
  files, built from histories of at most 2^22 commitments.
- Add encrypted receiver lookups (`ironwood-receiver-pir-v1-two-mask-m29`, the
  shared `pir-native` profile) and a row-file mode chosen by remaining work, both
  bound to one accepted publication and run over a host-supplied transport. A PIR
  lookup reads a history longer than 16 pages from the row file, so many payments
  to one receiver cannot fail a lookup. An HTTP service serves each publication's
  sessions, rows, witnesses and filters, with process identity and the indexer's
  latest report at `/v1/receiver/health` and the shared HTTP metrics, and refuses
  a publication that fails `Snapshot::validate` and a witness file without a path
  for every served record. A replaced
  publication keeps serving its sessions for 60 seconds, and the next replacement
  waits for that to end; a revocation stops session files and query answers
  still being sent at their next frame.
  Queries are admitted with Enhance's shared primitives:
  each client may have two in flight, and a query waits up to 2 seconds for one of
  two evaluation slots after its upload completes; refusals are 429 with
  `Retry-After: 1`. A query longer than its session's is 413 and a shorter one
  400, refused before it waits for a slot. Responses, refusals included, are
  `no-store`. Session IDs hash the session manifest at fixed width, and a
  session manifest is at most 16 KiB.
- Add the `receiver-indexer` service, whose `receiver-directory` binary backfills
  mainnet with batch anchor checks and reorg rollback, keeps publishing and
  serving each canonical tip from memory while revoking orphaned sessions,
  republishes a paused tip when its provider sets change, and
  feeds the `near-intents` filter sets from the NEAR Intents explorer when given a
  partner key. It checks each block against its header's merkle root, fails over
  between nodes, bounds each node response, publishes a few blocks
  below the tip, revokes only on a proven fork, reports completed NEAR payouts
  missing from the index and logs with `tracing`. `receiver-directory probe`, also
  built as `receiver-probe`, is a `pir-monitor` service probe with an embedded
  mainnet fixture that runs one live encrypted lookup of a pinned
  payment, checked against the fixture's pinned fields, for the fixture's
  independently decoded receiver, which recovery must reproduce, checking chain facts
  on one node at a time, highest tip first, and with `--witnesses` checks the
  witness file against the node's Ironwood root.
