# Receiver directory changelog

## Unreleased

- Breaking: Receiver serves each segment as a directory and a companion. A lookup
  reads one 2,048-row directory row, chosen by the segment's choice object, and on a
  hit one companion row per record, which carries the record and its witness payload.
  Both are prepared P8 tables. The payment and witness tables, the separate witness
  query, `--min-rows`, `--shard-rows` and `--shard-target` are removed; a publisher
  configuration still naming `min_rows` is refused. `GET /receiver/v1/choice/:digest`
  serves choice objects. A shard seals after 317,440 payments, or before a block
  that would not fit, and an oversized block still splits into up to eight segments.
  The store reseals from its chain index on first start, dropping earlier seals and
  table files. The minimum table budget falls from 1,536 MiB to 920 MiB, charged at
  each preparation's measured peak until it finishes. `native_certificate receiver`
  writes a table's certificate report for the operator's deploy gate. Wallets need
  the matching wallet-libraries client.

- Breaking: block hints are sparse and keyed by height. A wallet fetches a
  catch-up gap, usually in one request, from
  `/receiver/v1/block-filters/{revision}/{from}/{limit}` as one `IWRANGE2` range
  that carries a set only for blocks that pay a receiver, so a full empty window
  is a 92-byte header. Ranges cover only the rolling
  window the head advertises; history before it comes from PIR. A request
  still building an answer from hints its view no longer authorizes is refused.
  The `IWBATCH1` batch format and the `/block-filters/range/...`
  route are removed. No released wallet uses block hints.

- A query server prepares the tables a new Receiver publication adds, usually the
  shard the latest block extended, before it serves that publication. The first
  lookup after each new block took about 7 s instead of about 3 s while the server
  prepared the table inside that request; the old publication now serves for those
  seconds instead. The first publication after a start still prepares its tables
  on first use, so readiness does not wait for them.

- The `receiver-probe` canary runs again, a second later, while the publisher's
  health names a newer publication than the one the origin served it, for up to
  15 s. After each new publication, health reports it 6 to 10 s before query nodes
  serve it. The probe used to fail as `report_unavailable` in that window, and
  bursts of blocks raised the canary's availability alert. It reports the wait as
  `waited_seconds`, which `pir-monitor` leaves out of the canary's latency, so a
  burst of blocks no longer raises its latency warning either.

- The NEAR audit also checks refunds: failed swaps out of ZEC and excess refunds on
  successful ones, about 50 a day. A refund counts as found when any transaction
  NEAR reports for the swap pays its refund address, and an unmatched excess refund
  is confirmed with 1Click before it counts as missing. Health reports refund counts
  and per-feed progress for pir-apm. Payouts and refunds found in the index retire
  after two hours, so each publication checks only recent items instead of every
  payout ever recorded. The audit tables are additive, so a rollback keeps working.

- The wallet inventory and filter head wait up to 15 s instead of answering 503
  while a query server has no Receiver publication loaded: about 2 s after it
  starts, or after a publisher restart hands it new files. A query node's Caddy
  also retries the connection through the server's own restart, so wallets see a
  slower answer instead of 502 or 503 during deploys.

- A publisher stop also ends the open tip shard's build, which checks every
  witness against its shard root and took about 18 s each pass: the build asks for
  a stop between its steps and every 256 checked records, so a stop during that
  check no longer holds the publisher for up to 18 s.

- A restarted publisher no longer replaces the Receiver publication it restored
  with one that has block filters but no tables while it rebuilds them, and query
  servers reuse the tables they already loaded when a publication names the same
  files. A publisher restart now interrupts Receiver for about one query-server
  poll instead of until the rebuild finishes.

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
