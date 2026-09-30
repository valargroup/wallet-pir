# Transparent txid display PIR: server contract and qualification

This stage adds an opt-in server capability and a reference query helper in
wallet-pir. Wallet-libraries and Vizor integration are subsequent stages. Source
and local qualification do not establish deployment or production capacity.
Executable milestone tracking belongs in [remaining work](remaining-work.md).

## Scope and dependency

The implementation reuses the metadata work's `TransactionMetadata`, `FeeState`,
canonical fee calculation, version-3 event journal, and version-11 history shards.
The display codec has its own identity, `transparent-txid-display-v1`.
The exact metadata dependency and native dependency are recorded in each demo
report. Integrate against the metadata agent's committed head, then repin to main
when it lands. Do not publish that agent's unmerged changes as part of delivery.

This stage builds display extraction, durable publication, private tables, routing,
a native HTTP demo, and qualification tests. It does not build wallet APIs or
migrations, a production wallet coordinator, Vizor screens, parent retrieval,
canonical transaction retrieval, new hosts, deployment workflows, or cutover.

## Published facts

| Field | Meaning and validation |
|---|---|
| Txid | Full 32-byte identity in protocol internal byte order; carried by the directory and every fragment |
| Coinbase flag | Fee is non-applicable and transparent input count excludes the coinbase input |
| Shared metadata | Exact, unknown, or non-applicable whole-transaction fee; complete transparent input count; one shielded-components bit |
| Transparent outputs | Complete canonical order, exact zatoshi values, raw locking scripts; index is the position in this list |

The payload starts with codec version 1 and a flags byte. Bit 0 marks coinbase;
bits 3, 4, and 5 reuse shared metadata's exact-fee, shielded-components, and
metadata-present meanings. Unknown bits and missing metadata are refused.
Counts, values, script lengths, and metadata integers use canonical unsigned
LEB128. Unknown fee has no exact-fee flag; an exact zero fee has that flag and an
encoded zero. No timestamp, sender address, contact label, or payment attribution
is invented. Timestamps remain in block data.

The monetary total cannot exceed `MAX_MONEY`. Output count, script lengths, and a
lower bound on the canonical transaction's size are bounded by the supported
2 MB transaction ceiling. The encoded display payload has a conservative 4 MB
ceiling for integer expansion; this is a decoder bound, not a row width or a
promise of transaction authentication.

Records are extracted from complete canonical transactions, including external
transparent unshielding, coinbase, mixed pools, empty/OP_RETURN/unusual scripts,
and transparent-input transactions with no transparent outputs. Script-history
entries alone cannot supply this coverage. A shielded-only transaction with no
transparent input or output is outside this capability.

## Durability and publication identity

`event-ingest --txid-display` writes block-hash-addressed display sidecars under
`display-v1/`. The sidecar is synced and linked durably before its block is
appended to the existing event journal. The existing checkpoint determines which
block hashes are covered. A crash or reorg can leave an orphan sidecar, but it
cannot make that sidecar evidence of committed coverage. Sidecars are immutable;
a different payload under the same hash is a contradiction. Scripts above the existing journal's 10,000-byte reader ceiling use an additional
sidecar envelope carrying the unchanged shared event bytes and raw script.
Publisher projections merge those events so the public filter and excluded-script
counts retain them; no event codec is reinterpreted. Snapshot publication
uses its captured hashes, and retained sidecars remain readable through reorgs.

`shard-publish --txid-display` requires a sidecar for every covered block and
publishes the additional tables before atomically naming their manifests in the
map. Missing canonical output data is a failure, never a fabricated empty list.
Older journals without these sidecars cannot supply the capability. Use a fresh
journal/publication directory; this stage supplies no production data migration.
Continuing a publication cannot toggle display capability or rewrite a sealed
revision. An absent optional capability preserves existing serialized manifests;
a present capability creates a distinct manifest digest and publication identity.
Old consumers must be qualified for the new manifest before any activation.

This sidecar design adds a file per ingested block and retains unreachable files.
That cost and production retention are not qualified by the small local demo.

## Packed private lookup

`TxDirectory` (`txdirectory`) and `TxPages` (`txpages`) use 4,096-byte native rows.
Their row counts follow the shard's named directory/page geometry. Multiple small
entries share a row. Encoded payloads of at most 128 bytes fit inline; larger
payloads use private page locators. The directory envelope carries txid, total
payload length, first page, page count, and inline length. Its locator fields are
fixed little-endian integers; record integers use LEB128. Rows have an entry count,
length-delimited entries, and zero padding.

Page fragments carry full txid, byte offset, total length, and a payload chunk of
up to 4,050 bytes. Scripts can cross fragment boundaries. Assembly checks identity,
canonical offsets/chunk lengths, duplicates, gaps, total length, and the complete
record codec before reporting a complete output list. Server load also checks
placement, duplicate directory identities, orphan fragments, geometry, and digests.
Validation streams files instead of retaining both plaintext tables in memory.

Two domain-separated directory choices hash the full txid. Hashing, native setup,
table names, response epochs, and revision/table query bindings are separate from
script-history tables. A native query retrieves a selected row across **every**
segment. The reference helper retrieves both distinct choices and each needed
page row across all page segments.

## One coordinator endpoint

The existing endpoint, assignment planner, worker verifier, bounded runtime
cache, revision retention, and readiness controls serve all four tables:

```text
Transparent PIR endpoint
  + directory / pages       : script-history discovery
  + txdirectory / txpages   : complete transparent display outputs
```

The public map advertises optional segment counts; manifests advertise the codec,
record count, geometry, and digests. Public setup discovery declares each display
table's native scheme and dimensions. Display tables count toward assignment,
warm readiness, runtime-cache and disk-cache admission. Existing stale-revision,
overload, deadline, and unavailable responses remain authoritative.

The caller keeps the txid locally and supplies placement accepted by its own
chain. The reference helper selects the containing public shard. Unknown placement
and missing capability produce explicit results without dispatch. A directory miss
is distinct from a transport failure. Public routing names shard, revision, table,
and public setup segment identity; queries never name selected rows, txids, raw
scripts, or overflow locators in paths or headers. Overflow locators are retrieved
and used privately. Failure leaves retrieval incomplete, with no public txid or
parent fallback.

Accepted leakage remains range, table kind, page count, and timing. This does not
hide size or authenticate metadata against consensus. It is a trusted-publisher
service qualified against independent facts.

## Reproduce the native demo

Run from the repository root:

```bash
make transparent-txid-demo
```

The target uses the `release-fast` development profile and a leased Cargo target.
It verifies frozen checksums and the separate Python oracle, runs native encrypted
HTTP retrieval, then verifies that `--corrupt-oracle` exits nonzero. Its temporary
journal, publication, and loopback server require no external service or secrets.
The example can also run directly:

```bash
cargo run --locked --profile release-fast -p transparent-shard-server \
  --example transparent-txid-demo -- --report /tmp/transparent-txid-demo.json
```

Frozen inputs are mainnet blocks 347499–347501 and genesis, plus fifteen complete
parent transactions. The fixture records pinned upstream vector URLs, checksums,
and the public raw-transaction source. Genesis is a separate publication so the
demo does not imply coverage over the missing intervening heights. It preserves
the 67-byte P2PK output even though that script is excluded from the 40-byte
private history index. The ordinary send fixture returns change to the script of
its consumed output; the demo assigns that public script to Alice without keys.

| Case/control | Required evidence |
|---|---|
| Ordinary transparent send with change | Exact fee, recipient/change outputs, matching summary |
| Several outputs | Complete order, indices by position, values, raw scripts |
| External transparent unshielding | Canonical extraction works without an owned supported input script |
| Coinbase | Non-applicable fee and zero ordinary transparent inputs |
| Unusual output script | Raw bytes retained; no invented address |
| Valid large output list | Confirmed 300-output records require private overflow and fully assemble |
| Missing txid | Absence result differs from transport failure |
| Wrong revision/table binding | Server refuses the request |
| Altered fee or output | Independent comparison rejects the candidate |
| Interrupted overflow | No complete record escapes; a fresh retrieval can finish |
| Unsupported/unknown placement | No query or public fallback dispatched |
| Multiple segments | Synthetic native HTTP test decodes every directory/page segment; codec tests exercise crossing boundaries |
| Restart/reorg/contradictions | Checkpoint recovery, immutable sidecars, and malformed inputs are checked separately |

The report records source/dependency SHAs, fixture and publication checksums, case
counts, native queries, payload bytes, occupied rows and record packing, elapsed
time, safe HTTP paths/header shapes, cache reservations before/after display, and
history queries while display is loaded. Byte and memory accounting limits are
stated explicitly. Never retain client keys or encrypted query bodies as evidence.
Retain the measured report and results note under `transparent/evidence/`.

A pass establishes server protocol/retrieval evidence. It does not establish
production capacity, cryptographic release readiness, wallet recovery, or Vizor
correctness. Main CI, release artifacts, and production qualification remain their
own gates. Production deployment requires separate approval.

## Next wallet stage

Add one wallet coordinator with independent history and display work lanes. Store
validated output facts, source/revision provenance, and resumable obligations in
the existing wallet database. Opening a transaction prioritizes missing details;
cached facts render immediately. Deduplicate by transaction identity and revision.

Reconstruct owned inputs from spend events and matching receives; do not infer
other parties' inputs. Preserve richer local construction records. Outputs are
transaction facts, and payment attribution additionally requires ownership and
scope. Shared funding and mixed pools can remain incomplete even with a complete
transparent output list. Keep confirmation, detail completeness, fee availability,
and financial recovery coverage independent. Optional display work must not block
payload work required for financial recovery.

Under `PrivateRequired`, failures remain incomplete and never authorize public
lookup. This display service does not retrieve parent data or canonical transaction
bytes. If a later distinct private transaction-details capability supplies those,
retrieve missing parents privately only when independent fee calculation requires
it, cache validated results, and retain incomplete state on failure. Transaction
bytes cannot recover contact names, original intent, collaborative attribution, or
application grouping.

Integrate desktop/mobile Vizor screens and qualify the existing
[13-case activity acceptance table](../../docs/transparent-pir-activity-metadata-plan.md)
at pinned server and wallet revisions. The later two-layer qualification must test
wallet-library reconstruction and Vizor title/amount/pool/date/status presentation,
including shared funding, mixed pools, self-transfers, partial coverage, unknown
fees, contradictions, restart, and reorgs. Preserve every known transaction as a
tappable row and render incomplete state explicitly.
