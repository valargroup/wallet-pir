# Receiver directory changelog

## Unreleased

- Replace the common `IWPROOF1` witness file with a companion witness PIR table per
  payment table. Records grow to 289 bytes with a witness ordinal, `init` serves
  the payment and witness sessions together, and witness payloads are read over
  PIR. Witnesses are built per Ironwood shard from stored commitments, so
  `--witnesses`, `--max-witness-commitments` and the 2^24 commitment limit are
  gone. The probe checks the fixture's payload against the node's subtree root.
- Add the receiver directory: authenticated zero-OVK receiver extraction,
  paginated fixed-width records, deterministic publications from 8192 to 65536
  rows that retry up to 16 salts before doubling (directory profile
  `ironwood-zero-ovk-receiver-v1`, whose revision hashes every field at fixed
  width, the history correction revision included, and refuses unknown ones),
  and common `IWPROOF1` witness files, built from histories within a caller-set
  commitment limit.
- Add encrypted receiver lookups (`ironwood-receiver-pir-v1-two-mask-m29`, the
  shared `pir-native` profile), bound to one accepted publication over a
  host-supplied transport, one query per history page. An HTTP service serves each
  validated publication's sessions and witnesses, plus health and metrics for
  operators. A replaced publication serves its sessions for 60 seconds; any
  session not served is 410. Queries are admitted with Enhance's shared
  primitives, and wallet-route responses, refusals included, are `no-store`. A
  session manifest is at most 16 KiB.
- Serve bounded rolling block filters, one per block and derived from the chain
  index, independently of PIR preparation, with pruning, canonical revocation and
  history correction revisions shared with PIR publications. Wallets use them only
  to decide which receivers to look up over PIR.
- Add the `receiver-indexer` service, whose `receiver-directory` binary backfills
  mainnet with batch anchor checks and reorg rollback and serves each canonical tip
  from memory while revoking orphaned sessions. With an optional NEAR Intents
  explorer key it audits completed NEAR payouts to Orchard receivers, reporting
  those missing from the index or recently reported without a parsable
  transaction, and when the running process last read the payout feed. The audit
  never gates indexing, publication, serving or deployment.
  `receiver-directory probe`, also built as `receiver-probe`, is a `pir-monitor`
  service probe that looks up a pinned payment over live encrypted PIR and checks
  the current block hints against an independent node.
- Read the node through wallet-libraries' server-only chain adapters
  (`zakura-server-chain`) instead of the indexer's own node client and block
  decoding. Batch limits, response bounds and cookie handling are unchanged.
- Consume the canonical record codec from wallet-libraries with cryptography
  disabled for directory framing; remove the local codec package. The indexer
  explicitly enables validated receiver, record, recovery and witness APIs.
- Move receiver cryptography to wallet-libraries' `zakura-receiver-crypto`: address
  and record field checks, zero-OVK recovery and witness files. The receiver PIR
  packages now parse structure only and expose explicitly unchecked record and
  receiver bytes, and witness files as bytes; the indexer checks every payment
  before storing it and every publication before serving it.
- Validate records and witness proofs before writing one-shot publication files,
  including when reading an existing database; refuse invalid output before any
  files are created.
