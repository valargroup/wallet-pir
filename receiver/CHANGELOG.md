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
