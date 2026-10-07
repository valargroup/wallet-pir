# Receiver directory changelog

## Unreleased

- Add the receiver directory: authenticated zero-OVK receiver extraction,
  paginated fixed-width records, deterministic publications from 8192 to 65536
  rows that retry up to 16 salts before doubling, and common `IWPROOF1` witness
  files.
- Add encrypted receiver lookups (`ironwood-receiver-pir-v1-q48`) and a row-file
  mode chosen by remaining work, both bound to one accepted publication and run
  over a host-supplied transport. A PIR lookup reads a history longer than 16 pages
  from the row file, so many payments to one receiver cannot fail a lookup.
- Add the `receiver-directory` indexer and HTTP service: resumable mainnet
  backfill with batch anchor checks and reorg rollback, and continuous canonical
  serving that rotates publications and revokes orphaned sessions.
- Add `IWFLT1` publication filters (directory profile
  `ironwood-zero-ovk-receiver-v2`): paid receivers, and the receivers a swap
  provider was given recently or ever as a payout address, served at
  `/v1/receiver/filters/:session` and fetched with `fetch_filters`. The indexer
  reads the NEAR Intents explorer feed when given a partner key.
