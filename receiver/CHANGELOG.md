# Receiver directory changelog

## Unreleased

- Add the receiver directory: authenticated zero-OVK receiver extraction,
  paginated fixed-width records, deterministic publications from 8192 to 65536
  rows and common `IWPROOF1` witness files.
- Add encrypted receiver lookups (`ironwood-receiver-pir-v1-q48`) and a row-file
  mode chosen by remaining work, both bound to one accepted publication and run
  over a host-supplied transport.
- Add the `receiver-directory` indexer and HTTP service: resumable mainnet
  backfill with batch anchor checks and reorg rollback, and continuous canonical
  serving that rotates publications and revokes orphaned sessions.
