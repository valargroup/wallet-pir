# Receiver directory changelog

## Unreleased

- Publish recovery objects with every publication, committed by the manifest's
  `catalog_sha256`: closed 48-block paid buckets, a cumulative paid set, the
  monotone seen set, a bucket index and the catalog head, in wallet-libraries'
  formats, served from immutable, CDN-cacheable `/v1/receiver/{catalog,
  bucket-index, bucket/<start>, paid, seen}/<digest>` routes, staged under
  `recovery/` by one-shot runs and read by `serve-publication`. `recovery.sqlite`
  keeps the payment-history epoch, which a detected correction of published
  history increments, and the seen union across restarts, rebuilds and reorgs;
  `receiver-directory recovery` bumps the epoch or adopts the live state on a new
  host. `--retained-buckets` sets the catalog's retention (720 by default).

- Validate records and witness proofs before writing one-shot publication files,
  including when reading an existing database; refuse invalid output before any
  files are created.

- Consume the canonical record codec from wallet-libraries with cryptography
  disabled for directory framing; remove the local codec package. The indexer
  explicitly enables validated receiver, record, recovery and witness APIs.

- Read the node through wallet-libraries' server-only chain adapters
  (`zakura-server-chain`) instead of the indexer's own node client and block
  decoding. Batch limits, response bounds and cookie handling are unchanged.
- Move receiver cryptography to wallet-libraries' `zakura-receiver-crypto`: address
  and record field checks, zero-OVK recovery and witness files. The receiver PIR
  packages now parse structure only and expose explicitly unchecked record and
  receiver bytes, and witness files as bytes; the indexer checks every payment
  before storing it and every publication before serving it.
- Add the receiver directory: authenticated zero-OVK receiver extraction,
  paginated fixed-width records, deterministic publications from 8192 to 65536
  rows that retry up to 16 salts before doubling, `IWFLT1` files of labeled BIP 158
  receiver sets (paid receivers, plus each swap provider's recent and seen sets as
  the manifest declares them, with when its feed started and last completed a read;
  directory profile `ironwood-zero-ovk-receiver-v1`, whose revision hashes every
  field at fixed width and refuses unknown ones), and common `IWPROOF1` witness
  files, built from histories within a caller-set commitment limit.
- Add encrypted receiver lookups (`ironwood-receiver-pir-v1-two-mask-m29`, the
  shared `pir-native` profile), bound to one accepted publication over a
  host-supplied transport, one query per history page. An HTTP service serves each
  validated publication's sessions, witnesses and filters, plus health and
  metrics for operators. A replaced publication serves its sessions for 60
  seconds; any session not served is 410. Queries are admitted with Enhance's
  shared primitives, and wallet-route responses, refusals included, are
  `no-store`. A session manifest is at most 16 KiB.
- Add the `receiver-indexer` service, whose `receiver-directory` binary backfills
  mainnet with batch anchor checks and reorg rollback, serves each canonical tip
  from memory while revoking orphaned sessions, feeds the `near-intents` filter
  sets from the NEAR Intents explorer and reports completed NEAR payouts to Orchard
  receivers missing from the index or recently reported without a parsable
  transaction, and when the running process last read each NEAR feed.
  `receiver-directory probe`, also built as `receiver-probe`, is a `pir-monitor`
  service probe that looks up a pinned payment over live encrypted PIR; a deploy runs
  it with `--await-feed-reads` to wait for the restarted process to read both NEAR
  feeds and serve a publication of those reads.
