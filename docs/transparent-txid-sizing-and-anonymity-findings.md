# Transparent txid display sizing and independent routing findings

## Decision and qualification

The 2026-10-01 full-chain continuation is **blocked at the explicit 72-hour
throughput gate**, not completed by a sample. The sanctioned read-only archive
gateway verified anchor height **3,502,662**, hash
`0000000000228603173bfeb3650b51ceb92b1ac6b8b6392fa1fa9e9bac3acf7f`,
before and after acquisition. Sequential `getblockhash` then raw `getblock(0)`
fetched 612 blocks (heights 0–611) in 180.171 seconds: **3.397 blocks/s**,
projecting **286.4 hours remaining**, excluding parsing, UTXO and aggregation.
The [throughput receipt](../transparent/evidence/txid-sizing/full-chain-throughput.json)
pins the measurement, code and retained raw manifest checksums. No canonical
blocks have been scanned in this continuation; eligibility, continuity, node
source revision and population counts remain unverified. The gateway session
closed and its temporary key was removed. Raw inputs remain outside Git at
`/home/ai-dev/.cache/wallet-pir-census/`. Roman must authorize a longer duration
or an approved faster acquisition method before resumption. This sequential
measurement does not establish the maximum throughput of the gateway or of
pipelined requests within one session. PR #124 remains open; a superseding
findings PR requires the completed full-chain census.

**The requested boundary means MORE THAN 80% of eligible confirmed display
records fit inline; the remainder require overflow pages.** Eligible means any
transparent input or output, including coinbase. Shielded-only transactions with
neither are excluded. Coverage counts distinct transactions, not outputs,
fragments, history events or allocated rows.

**UNQUALIFIED: no measured full-chain threshold or minimum anonymity population
can be recommended from the available dataset.** The accessible convenience
sample supports a byte/packing comparison and demonstrates why 95% should not be
assumed. It cannot qualify either 128 bytes or a higher production cutoff. The
roughly 17 million history event-bearing txids are not a census of complete
display records. This PR changes analysis and documentation only.

The sample has 60 metadata-resolvable records from 41 sparse upstream mainnet
blocks: 41 coinbase, 19 other transactions, and 1,905 transparent outputs. It
excludes **74 of 134 eligible transactions** because complete external prevouts
are missing. Only 23 blocks have complete eligible-display extraction. Another
24 transactions are shielded-only. The fee extractor refuses missing parents;
this analysis never fabricates empty outputs or exact fees for exclusions.

Within these 60 records, the smallest display-v1 cutoff strictly exceeding 80%
is 118 bytes, which reaches 90%. Cutoffs 128–1024 all produce the same 54 inline
and six overflow records. The sample-specific byte frontier is 118, with no
benefit from increasing to 192–1024; it is **not the recommended chain threshold**.
A 95% target jumps to 8,744 bytes, exceeding the present directory row capacity.
The next implementation gate is a complete anchored canonical display export,
then a joint threshold/routing/workload measurement. Executable gates are in
[remaining work](../transparent/docs/remaining-work.md#txid-display-sizing-and-independent-routing).

## Data discovery, exact pins and coverage

[Discovery evidence](../transparent/evidence/txid-sizing/discovery.json) records
actual source-selection decisions. Operations documentation identifies the
coordinator archive at `/root/.cache/zakura`, read through Zakura RocksDB secondary
`StateReader::open`, with full indexed parent transactions. Near-tip blocks come
from coordinator loopback RPC on port 8232 with a node-local cookie. Read-only
access must use the sanctioned wrapper/selector; credentials remain on the node.

Existing retention receipts name a 2,108,498,060-byte, 100-block raw cache bundle
on the coordinator (SHA256
`7819878f98b6ca37749f289f20ec5bd02d728d1fb56ce73678b8c74b945f2964`)
and a 226,423,138-byte independent raw bundle (SHA256
`668d2c6543614a8b9cf874e38fef85a227784c4e927272b551705ee09dfe4a99`).
Their bytes were not accessible with the configured hub selectors. The hub
registry contains no registered chain export; backup locations hold registry
SQLite files. The archive mount returns permission denied. No production SSH
agent identity or RPC/cookie environment selector is configured. Available
repository variables identify build runners only. Unexpired GitHub artifacts
advertise small history inventory/census aggregates, not canonical raw exports.
No secret retrieval, workflow dispatch, production changes or access bypass was
attempted. This does not prove an export does not exist elsewhere.

The [public init endpoint](https://transparent-pir.valargroup.dev/v1/shards/init)
was read successfully: schema `transparent-shard-v10`, advertised coverage
0–3,502,499, 86 shards, map digest
`139642fafdcf72471aa3e7b03ecc94c56faea673853f8732fa28edf8ff9d97fd`.
Its response SHA256 is
`812d6d9a36a6672ac24d7ad2091c80c23c5a128733c86c3e94868985a18352e5`.
It supplies discovery/geometry facts, not canonical output records or proof of
complete display eligibility. No live display-v1 sidecar coverage was established.

The accessible source is the existing immutable Cargo cache for
[Zakura revision af944f5194ef2e9921bc96af017629450375013c](https://github.com/zakura-core/zakura/tree/af944f5194ef2e9921bc96af017629450375013c/crates/zakura-test/src/vectors).
Selection includes **every** `block-main-*.txt` without `bad` in the name at that
revision: 41 blocks, 354,315 decoded raw bytes, 158 total transactions. Selection
was fixed by source availability, not size or wallet ownership. Upstream vectors
are consensus test material, not a probability sample. The exporter resolves
parents from complete outputs in selected blocks only; external-parent absence
is an additional, severe selection bias. Existing display-demo parents and
conclusions are not used as population evidence.

All individual heights, block hashes, text/raw checksums, inclusion counts and
exclusion txids are pinned in [sources.json](../transparent/evidence/txid-sizing/sources.json)
and [canonical-sample.json](../transparent/evidence/txid-sizing/canonical-sample.json).
The highest selected block is height **1,687,121**, hash
`0000000000cf398eb1fbf9dd05b6ca4aead556b46d884428d3b7557ecd8739dd`;
its raw SHA256 is
`214c0a0c2a9fefbb9b9b6e07281fdc4c0674aeb6d9eeefd3b8ccbac074e2df42`.
There is **no continuous chain anchor** across gaps, no current-chain qualification,
and no coverage of later eras. The source revision and individual upstream block
identities are the sample provenance; this tool does not independently validate
chain consensus or connect omitted intermediate blocks.

Wallet-pir source pin: `ccbfd35fc21935f8a6878f93d288cc3107ea90e2`.
Metadata dependency: `3cfbc4848b92b1385b9215174251f8e51fbfa804`, landed before
that source pin. The offline Rust exporter directly compiles the existing
canonical `extract.rs`, which calls `Transaction::value_balance` and
`remaining_transaction_value`, and uses shared `TransactionMetadata`/`FeeState`.
It uses the implemented display-v1 encoder and packer. Relevant source-file and
Cargo lock checksums are retained; no fee calculation is copied into Python.

Equal weights apply only to the 60 extracted records. Inclusion probabilities
and valid population/era weights are unknown, so binomial confidence intervals
would be misleading and are withheld. Deterministic identification bounds within
the **134 selected-block eligible transactions** are provided instead: at 128,
inline coverage is between 54/134 = **40.30%** and (54+74)/134 = **95.52%**.
Those are missingness bounds, not confidence bounds for the chain. The sample
cannot even prove greater-than-80% coverage for its selected-block population.

The extracted data include 16 records with shielded components and 13
non-coinbase records with shielded components, zero transparent inputs and
transparent outputs (external unshielding shape). Exact standard output scripts
are preserved, including genesis P2PK. The extracted sample has **no**
transparent-input/no-transparent-output record and **no** raw-escape script.
Those shapes are tested synthetically for codec accounting but their chain
frequency remains unmeasured. Exclusions retain txid/reason, not partial records.

## Implemented bytes and precisely specified proposals

The [display design](../transparent/docs/txid-display.md) is authoritative for
implemented behavior. Display-v1 payload bytes are:
`version:u8 + flags:u8 + input_count:ULEB + exact_fee:ULEB(if present) +
output_count:ULEB + each(value:ULEB + script_len:ULEB + exact_script)`.
Coinbase fee is non-applicable; unknown fee differs from exact zero.

All proposals below are analysis only and require a distinct manifest codec
identity, independent conformance/oracle tests and later implementation approval.
They preserve all output values/order and exact raw scripts.

| Variant | Payload changes | Directory / fragment changes |
|---|---|---|
| Implemented display-v1 | Above format | 46-byte directory envelope: txid32, total:u32, first_page:u32, page_count:u32, inline_len:u16. Fragment: txid32, offset:u32, total:u32. |
| Manifest-version | Remove per-record version byte; version/feature rules in bound manifest | Proposed tagged compact envelope and fragments below |
| Script-compression | Manifest-version plus exact script templates | Same compact envelope |
| Common-counts | Script-compression plus two count nibbles in one byte | Same compact envelope |

Proposed compact directory envelope: `txid32 + tag:u8`; inline tag 0 then
`payload_len:ULEB + payload`; overflow tag 1 then
`total_len:ULEB + first_row:ULEB + fragment_count:ULEB`. First row remains one-based.
Locator sizes are calculated from actual proposed packing, not assumed constant.
Rows retain count:u32 and entry length:u16. Proposed fragments carry
`txid32 + offset:ULEB + total_len:ULEB + chunk`, with variable chunk capacity
`4096 - 4 - 2 - header_bytes`. Their private contiguous placement and decoder
rules must be specified before implementation; the proposal is not wire-compatible.

Script tags: 0 = raw escape with ULEB length and original bytes; 1 = exactly
`76 a9 14 <20> 88 ac`; 2 = exactly `a9 14 <20> 87`; 3 = exactly
`21 <33-byte key beginning 02/03> ac`; 4 = exactly
`41 <65-byte key beginning 04> ac`. Tags 1/2 carry the 20-byte payload;
3/4 carry the complete original key. No uncompressed-key curve reconstruction,
address substitution, script normalization or lossy OP_RETURN handling occurs.
Near matches use raw escape, costing one additional tag byte. Sample outputs:
1,812 P2PKH, 79 P2SH, 13 compressed P2PK, one uncompressed P2PK.

Common-count header: flags byte, then input-count nibble in the high half and
output-count nibble in the low half. Values 0–14 are direct; nibble 15 is followed
by that count as canonical ULEB (input escape first, output escape second), then
exact-fee ULEB if present. Counts 15 and larger do not gain the small-count byte
saving. Shared fee semantics/flags remain unchanged.

## Sample percentiles and threshold packing

Nearest-rank percentiles, bytes per complete record:

| Codec | p50 | p85 | p90 | p95 | p99/max | Total payload bytes |
|---|---:|---:|---:|---:|---:|---:|
| Implemented display-v1 | 67 | 118 | 118 | 8,744 | 15,030 | 56,394 |
| Proposed manifest-version | 66 | 117 | 117 | 8,743 | 15,029 | 56,334 |
| Proposed script-compression | 56 | 103 | 103 | 7,247 | 12,536 | 47,009 |
| Proposed common-counts | 55 | 102 | 102 | 7,247 | 12,536 | 46,955 |

Both 85% and 90% target cutoffs reach 90% because of tied coinbase sizes. 95%
requires the p95 values above; 99% and 99.9% both require the maximum in this small
sample. Even compact proposals do not make a 95% single-row cutoff feasible here.
Coinbase display-v1 p50/p95 are 70/118 bytes; non-coinbase p50/p95 are 64/15,030.
The 118-byte tied group illustrates the importance of coinbase inclusion and era
mix rather than assuming ordinary two-output sends dominate.

| Era (activation-height boundaries) | Extracted txs | display-v1 p50 | p95 | max |
|---|---:|---:|---:|---:|
| Sprout | 19 | 70 | 8,748 | 8,748 |
| Overwinter | 8 | 38 | 63 | 63 |
| Sapling | 9 | 63 | 15,030 | 15,030 |
| Blossom | 4 | 63 | 63 | 63 |
| Heartwood | 8 | 37 | 64 | 64 |
| Canopy | 2 | 118 | 118 | 118 |
| NU5+ (only selected early NU5 blocks) | 10 | 118 | 8,433 | 8,433 |

All requested cutoffs are retained separately in
[analysis.json](../transparent/evidence/txid-sizing/analysis.json). Their identical
results are grouped here; there is no interpolation over missing transactions.

| Codec / cutoff | Inline / overflow txs | Outputs | Fragments | Directory entry bytes | Overflow entry bytes | Occupied bytes |
|---|---:|---:|---:|---:|---:|---:|
| display-v1 / 128 | 54 / 6 | 1,905 | 17 | 6,540 | 53,448 | 60,292 |
| display-v1 / each 192,256,384,512,768,1024 | 54 / 6 | 1,905 | 17 | 6,540 | 53,448 | 60,292 |
| proposed manifest-version / each requested cutoff | 54 / 6 | 1,905 | 17 | 5,784 | 53,368 | 59,456 |
| proposed script-compression / each requested cutoff | 54 / 6 | 1,905 | 13 | 5,378 | 44,297 | 49,967 |
| proposed common-counts / each requested cutoff | 54 / 6 | 1,905 | 13 | 5,324 | 44,297 | 49,913 |

Only display-v1/128 is implemented; changed thresholds use its packer as
counterfactuals. Additional cutoffs 64,96,160,2048,3072,4044 and attainable
sample percentiles are retained. At 3072/4044 display-v1 reaches 55/60 = 91.67%,
still below 95%. Occupied bytes include entry lengths and headers of occupied
rows. The implemented packer places these records in 60 directory rows and 16
page rows; two fragments share a row, so 17 fragments are not 17 distinct page
rows or transactions. Retrieval totals 17 overflow row requests across six
transactions. The compact proposal has 13 fragments in 13 occupied page rows.

The sample is artificially packed into one bucket-0 `recent-4k` table pair for
reproducibility; it is not a replay of live placement. Each table allocates
4096×4096 = 16 MiB even though useful data are small. Implemented occupied-row
slack is 251,004 bytes; full allocation slack is 33,494,140 bytes. Compact
common-counts saves 10,379 occupied bytes, but does not reduce this minimum
allocation. Prepared reservations remain **167,867,504 bytes** for the pair.

The runtime formula for a 4096-byte-row segment is
`rows*4096 + 14,848 + 16 + (32 + 2*2048*8 + 8 + 2048*2048*2*8)`.
This is the existing two-mask native preprocessing upper-bound reservation,
not measured RSS. It excludes allocator/process memory, setup/hints, transient
plaintext and request allocations. Splitting tables incurs this fixed term per
segment; changing row count affects database size, request upload and scans.

A row query uploads once and returns a response for every segment. Thus for
lookup segments L and overflow segments O, expected encrypted row requests are
`distinct_lookup_choices + E(distinct_overflow_rows_mod_geometry)`; native
segment evaluations are approximately `2L + E(overflow_rows)*O`. For this sample
at 128 they are 2.2833 requests/evaluations per uniform txid opening (L=O=1);
compact script/count proposals give 2.2167. Setup fetches, cache hits, retries,
HTTP framing and workload skew are excluded. These are request-count calculations,
not timed PIR cost measurements.

## Independent geometry: cost and privacy must be chosen together

Recommend one logical public coordinator with **independently sized display
lookup and overflow tables**. Do not inherit script-history range, directory or
page geometry. As an experimental starting grid, compare 1/4/16/64 stable lookup
hash buckets, 1/4/16 overflow buckets and row geometries 4096/4096,
8192/32768 and 32768/65536. Lookup hashing should use full txids and a separate
public domain; overflow placement must not encode script-history placement.
Four lookup hash buckets with a global overflow table is a useful measurement
starting point, **not a qualified fleet or minimum-population selection**.

A single overflow database maximizes route candidate breadth but may require
many segments and expensive scans/responses. Broad stable hash buckets can
reduce per-query scan cost if their **intersections** remain sufficiently large;
broad temporal overflow buckets reveal an additional time partition. Hash
bucket counts must be limited by rare observable classes, not average bucket
population. Keep a stable broad revision identity and qualify append/refresh
behavior; a separately routed thin tail defeats otherwise broad lookup routing.

[Geometry projections](../transparent/evidence/txid-sizing/geometry-projections.json)
illustrate the tradeoff. They assume 17 million eligible records solely as a
scale example, mean inline payload 80 bytes, mean overflow payload 8500 bytes,
three overflow requests per overflow transaction, 75% target occupancy, uniform
uncached openings and balanced buckets. Those means are **not fitted population
estimates**; holding them fixed at different coverage levels is a sensitivity
experiment. No row in this report gives a measured latency or qualified minimum.

For four lookup buckets, global overflow, 8192-row lookup segments and 32768-row
overflow segments:

| Assumed coverage | Lookup / overflow segments per bucket | Reservation GiB | Requests/open without cover | Scan GiB proxy without cover | Scan GiB proxy with three-row cover for every open |
|---|---:|---:|---:|---:|---:|
| 85% | 20 / 219 | 48.576 | 2.45 | 13.569 | 83.375 |
| 90% | 21 / 146 | 35.260 | 2.30 | 6.787 | 56.062 |
| 95% | 21 / 73 | 21.569 | 2.15 | 2.681 | 28.688 |
| 99% | 22 / 15 | 11.067 | 2.03 | 1.431 | 7.000 |

Three-row cover makes five encrypted requests/open in every case, with a much
larger work cost. Independent broad overflow buckets reduce scan work but also
partition candidates. For example, the assumed 99% tail averages 42,500 candidates
per lookup/global-overflow intersection; adding 16 independent overflow buckets
reduces that average to 2,656 before counts, revision, timing or prior history
conditioning. Neither average proves even a 1000 floor. Reservation savings
and scan bytes must be checked against native preprocessing, actual occupancy,
load skew, response bytes and measured CPU latency on existing resources.

Public discovery gives native request bodies of 52,736 bytes at 4096 rows,
77,824 at 8192, 228,352 at 32768, and 429,056 at 65536, before the display query
binding. Responses are 5632 bytes per segment plus 16-byte binding/epoch framing.
More segments increase responses and server evaluations without multiplying
one row-query upload. The scan proxy above is encoded database bytes touched,
not a throughput benchmark.

## Observable intersections and the five-candidate failure

Threat model: a coordinator/server links initial lookup and every overflow
request, knows the chain and layouts, and observes table/bucket/revision routes,
segment vector, encrypted request counts, and modeled timing. PIR hides the
selected row, not those routes. Candidate sets are the intersection of **all**
observables, deduplicated by real transaction identity. Empty rows, repeated
fragments, cover queries, dummy entries and padding supply no additional real
transaction candidates. A globally placed overflow request cannot erase a
narrow time range already exposed by lookup.

The synthetic negative control contains 20,000 ordinary transactions and **five**
large tail transactions in a different lookup range. All overflow is global.
The five tail transactions have three overflow requests; their intersection
population is exactly five. Both 1000 and 10000 policy floors reject that class.
Repeating each tail fragment twenty times or adding 10,000 non-real dummy/empty entries leaves five candidates. Broadening the
lookup AND covering all openings to the same query schedule yields 20,005
candidates under a frozen common revision/no-timing model. A new tail revision
or modeled tail timing/segment fan-out splits it back to five. A separate independent-route control gives 10,000 real candidates in every lookup and overflow marginal, but only five in two joint intersections; checking either marginal alone incorrectly accepts a 10,000 policy. These are reproducible controls,
**not a claim that a live public-chain class has been measured to contain five**.

The sample routing experiment uses a 50,000-height stand-in for inherited
narrow temporal lookup, million-height coarse buckets, or four domain-separated
hash buckets. Actual live shard geometry is not replayed. Minimum and nearest-rank
class-weighted candidate percentiles at display-v1/128, frozen revision, no timing,
no cover are:

| Lookup / overflow | Classes | min | p05 | p50 | p95 |
|---|---:|---:|---:|---:|---:|
| Narrow temporal / global | 13 | 1 | 1 | 3 | 14 |
| Coarse temporal / global | 6 | 1 | 1 | 1 | 42 |
| Hash / global | 8 | 1 | 1 | 2 | 18 |
| Global / global | 4 | 1 | 1 | 1 | 54 |
| Hash / broad temporal | 9 | 1 | 1 | 2 | 18 |
| Hash / independent hash overflow | 9 | 1 | 1 | 2 | 18 |

These are candidates **inside the biased sample only**, not minima for the chain
or actual service. Missing eligible records may belong to the same classes.
Machine evidence also reports class p01/p99/max, transaction-weighted percentiles,
and classes/transactions below 1000 and 10000 at every requested threshold.
Every sample class is below both policy floors; this small sample cannot qualify
those floors. Compact proposals reduce counts/costs but need their own routing
qualification; the retained routing matrix deliberately uses implemented bytes.

Three-row cover at global/global produces a 59-record class and a one-record
class: the largest record still needs four rows. Hash/global cover has five
classes, minimum one, p50 14, p95 18; it does not hide that excess. Even padding to
four cannot qualify a 1000 floor with only 60 records. Frozen revision/no timing
is optimistic: a 100-block refresh cohort or ten-block timing cohort further
partitions candidates. Segment fan-out differences must enter the transcript
class too, including empty-bucket/absent-table dispatch behavior. Coincident hashed lookup choices produce one initial request in the current
helper; the routing model includes this additional count class. Count cover must
also send two lookup requests for those cases, not just pad overflow. A synthetic
coincident-choice count tail again isolates five candidates. Fragment count
is not directly visible inside encrypted rows; where public sizes and this
contiguous layout imply it from row requests, it is included through that
observable request count. Across segments the current helper deduplicates row
indices modulo page geometry, so large-record fragment count need not equal
requests. Real replay must use the exact helper/layout rather than assume equality.

Policy candidates 1000 and 10000 mean refusing/pooling an observable class below
that many real eligible transactions, measured at each accepted revision. They
are engineering policy options, **not formal anonymity guarantees** or uniform
posterior probabilities. Counts should be evaluated class-weighted and
transaction/workload-weighted, including the minimum, rare size tails, coinbase,
era strata, new transactions, bucket growth and refresh transitions. Padding
may merge transcript classes only when other observables match; it never creates
members. Cover must include inline opens if inline/overflow classification is to
be hidden. Exceptional large records require either larger cover schedules,
a distinct explicitly unqualified mode, or delayed/coalesced retrieval; do not
silently claim a fixed three-query policy covers every valid transaction.

Prior history retrieval remains a residual correlation channel. A coordinator
that saw a wallet query a narrow script-history range can condition a later
broad txid lookup on that range, session/time association, and public transaction
shapes. Independent hash tables do not erase that transcript. Qualification
must replay linked history plus display retrieval and test scheduling/cache/
cover behavior. No timing mitigation or independence from previous queries has
been measured in this PR.

## Recommendation and next implementation gate

Withhold a numeric production inline threshold. Measure the 85%, 90%, 95%, 99%
and higher frontiers on the complete eligible population, compare occupied bytes
and attainable row capacity rather than nominal cutoff width, and select the
lowest-cost frontier strictly above 80% that satisfies an explicitly chosen
observable-class policy under the intended workload. The sample favors its
118-byte/90% frontier over an infeasible 95% jump; this cannot be promoted to a
chain recommendation. A higher cutoff reduces overflow query probability but
can make the residual tail easier to identify. Joint optimization must include
cover cost and population intersections, not storage alone.

Recommend stable independent coarse/hash lookup routing, with global overflow
as the privacy comparison baseline; choose broad overflow buckets only after
full intersection counts and native cost measurements justify partitioning.
Keep one logical public coordinator regardless of physical table placement.
Avoid both temporal lookup inherited from history and treating global overflow
as a repair for it. No codec, sharding, wallet integration, migration or cutover
is authorized by these findings.

To finish measurement, provide a sanctioned immutable export selector or
read-only archive wrapper containing **all canonical confirmed transactions with
any transparent input/output, all raw output scripts, and complete prevouts**
through one accepted height/hash. An alternative is complete anchored display-v1
sidecars built by the shared canonical extractor, accompanied by the committed
checkpoint block hashes, eligibility/all-transaction inventories and retained
raw source provenance for independent spot checks. Include coinbase, mixed pools,
external unshielding, raw/empty/OP_RETURN scripts and input-only transparent shapes.
Pin node/data format, metadata/codec source, dependency locks, checksums and
chain continuity. Supply revision membership and refresh/tail inputs to reproduce
observable classes; do not send credential values. The current bounded tools
are not a streaming full-chain census pipeline.

## Validation and limits

Seven focused Python tests pass, including Rust/Python payload agreement,
implemented packing agreement, exact script round trips/raw escape, shared fee
states, count escapes, fragment boundaries, duplicate identities, tampering and
five-candidate/refresh/timing controls. The offline canonical exporter built and
ran with `release-fast`; its implemented table verifier passed. Existing
independent confirmed-vector facts cross-check overlapping records solely as an
oracle, not as population evidence. Source and evidence checksums plus deterministic
report regeneration are verified by the retained helper. No native encrypted
request benchmark or new privacy/production qualification was run.

The current affected-check planner treats the new analysis-tool directory as an
unknown path and broadens to the workspace. Its selection is reported honestly;
full local CI is not substituted for focused analysis checks. PR CI owns the
repository gates at the exact head; pending/skipped or inherited failures must
be reported in the PR rather than called passing. Comprehensive CI, exact-SHA
release artifacts and hardware qualification remain separate release gates.
