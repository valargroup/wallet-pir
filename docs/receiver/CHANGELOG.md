# Receiver directory changelog

## Unreleased

- Publish permanent per-block receiver filters and durable seen objects independently of PIR preparation. Bind immutable ranges and session coverage to a durable archive identity and correction epoch. Detect record corrections even in empty blocks below a simultaneous fork. Preserve canonical history while pruning retired versions after their serving grace. Add an independently configurable filter depth without changing production defaults.

- Backfill the NEAR feeds' history from `--near-since`, now required to serve with a
  key, in sweeps that record each page and resume after a failure or restart. A
  recent pass rereads the last week hourly and a full pass the whole history daily,
  so swaps the explorer lists late, corrects or a read skips are recorded. Live
  reads, payout completions included, reach back at most a day. The seen set is
  published only once both feeds' history reaches the live reads, and declares where
  it begins. The provider store's receivers are an append-only union. Health's report
  gives each feed's history apart from its live reads, and the probe fails
  `stale_history` when it ends more than six hours ago or its last full pass began
  more than twice that pass's duration plus a day ago, and at least three days ago.
  `--near-sweep-pages` sets the pages each poll's sweeps read, shared between the
  passes so neither starves.
- Report each publication's use of the limits past which no publication is created
  (records against the 240,000 the largest table places reliably, filter bytes,
  witness bytes and commitments, and the NEAR full pass duration against two days) as `indexer.capacity`, with a `warning` level from
  70% and `critical` from 90%, and the limit behind a refused publication, an
  oversized filter or witness file included, as `capacity_refusal`. The probe passes
  them on, and `pir-monitor` alerts on them apart from availability. Running sweep
  duration uses wall time even when the chain is paused. Optional capacity reads
  run concurrently and never delay the primary probe verdict.

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
