# Receiver directory changelog

## Unreleased

- A restarted publisher serves its previous Receiver publication again as soon as
  it has read the chain, under its new recovery epoch, while it rebuilds the tables.
  Only a publication of the same history revision whose files are all still kept
  and whose anchors are still on the chain is restored, so Receiver no longer goes
  unready for the ~27 s rebuild after every publisher deploy.

- A publisher stop no longer waits for Receiver to build every shard of a pass: the
  build ends at the next shard and replaces nothing a later pass reuses, so a deploy's
  publisher restart takes about a second instead of up to 20.

- A restarted publisher republishes the same Receiver tables under a new
  publication. Inventories and descriptors carry the publisher's recovery epoch,
  so query servers no longer refuse the restarted publication as the one they saw
  withdrawn during the restart, and Receiver is ready again without waiting for the
  next block. Query servers built before this change refuse the new field, so deploy
  it with the publisher (`pir-deploy all` or `--breaking`).

- Use wallet-libraries' functional-area PIR packages for Receiver: formats and
  validation from `zakura-pir-protocol`, publication building from
  `zakura-pir-publisher`, the evaluator from `zakura-pir-query-server` and the client
  from `zakura-pir-client`, replacing `zakura-receiver-crypto`,
  `zakura-receiver-directory` and `zakura-receiver-pir`. Serving no longer
  depends on the PIR client.
- Publish history as an inventory of sealed Receiver shards and a recent tier.
  Shards seal automatically on exact payment placement and companion witness
  capacity in the pinned `--shard-rows` geometry, or after `--shard-target`
  payments, on block boundaries; an oversized block becomes its own shard of
  several segments. Each sealed shard keeps a GCS receiver filter while it is
  served. `/v1/receiver/inventory`, its chunks and `/v1/receiver/shard-filters/{digest}`
  replace `init`, and a displaced inventory serves for `--retention-seconds`
  (default 600). Staged serving loads one-shot inventory layouts. Prepared tables
  are written to the data directory and kept resident only within a
  `--table-memory-mib` budget (default 4096, at least 1536), loading on demand.
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
  history correction revisions shared with PIR publications. The window spans
  seven days of target time, more blocks after NU7, and `--filter-blocks` can
  only shorten it. Wallets use them only to decide which receivers to look up
  over PIR.
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
