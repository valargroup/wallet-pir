# Receiver directory changelog

## Unreleased

- Add the receiver directory: authenticated zero-OVK receiver extraction,
  paginated fixed-width records, deterministic publications from 8192 to 65536
  rows that retry up to 16 salts before doubling, `IWFLT1` files of labeled BIP 158
  receiver sets (paid receivers, plus each swap provider's recent and seen sets as
  the manifest declares them, with when its feed started and last completed a read;
  directory profile `ironwood-zero-ovk-receiver-v1`, whose revision hashes every
  field at fixed width and refuses unknown ones), and common `IWPROOF1` witness
  files, built from histories within a caller-set commitment limit.
- Add encrypted receiver lookups (`ironwood-receiver-pir-v1-two-mask-m29`, the
  shared `pir-native` profile) and a row-file mode chosen by remaining work, both
  bound to one accepted publication over a host-supplied transport; a history
  longer than 16 pages is read from the row file. An HTTP service serves each
  validated publication's sessions, rows, witnesses and filters, plus health and
  metrics for operators. A replaced publication serves its sessions for 60
  seconds. Queries are admitted with Enhance's shared primitives, and wallet-route
  responses, refusals included, are `no-store`. A session manifest is at most
  16 KiB.
- Add the `receiver-indexer` service, whose `receiver-directory` binary backfills
  mainnet with batch anchor checks and reorg rollback, serves each canonical tip
  from memory while revoking orphaned sessions, feeds the `near-intents` filter
  sets from the NEAR Intents explorer and reports completed NEAR payouts missing
  from the index or recently reported without a usable transaction, and when the
  running process last read each NEAR feed. `receiver-directory probe`, also built
  as `receiver-probe`, is a `pir-monitor` service probe that looks up a pinned
  payment over live encrypted PIR; a deploy runs it with `--await-feed-reads` to
  wait for the restarted process to read both NEAR feeds.
