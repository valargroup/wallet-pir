# Transparent txid display sizing and independent routing findings

These findings size the variable-length display-v1 record, which display v2
(fixed 113-byte entries, one table per bucket, no pages) replaced on `main`.
Inline cutoffs, overflow pages and page-count classes no longer apply. The
eligible-transaction census still sizes shard counts. The tools and codec they
describe build at revision `a368f19b52330186dbf56f49066b2b03b4da368a`.

## Decision

**Recommend 128 encoded payload bytes for the current display-v1 codec.** The
whole-range probability sample estimates **92.27–92.71%** inline coverage for
the hypothetical exact-fee eligible population. Under worst-case fee widths,
the seven-cutoff adjusted normal lower bound is **91.72%** and the adjusted
block-bootstrap lower bound is **91.70%**, both strictly above 80%. This is the
smallest of the seven prespecified cutoffs that passes both screens; it is not
the minimum over every possible byte cutoff or a full-census guarantee.

Keep one logical coordinator, with display/history geometry independent. For
the next prototype, compare **four stable hash lookup buckets with global
overflow** against fully global lookup/overflow. Treat four buckets as a
performance/privacy comparison baseline, not an accepted anonymity policy.
Independent hashes do not prevent their public routes from intersecting.
**No modeled geometry qualifies a full-population anonymity minimum.** Even
global/global routing has thin excess-page classes; count cover of three is
insufficient to certify K=1000 or K=10000. Broad overflow needs a joint policy
and a concrete cost justification before adoption.

This one-day study supersedes the exhaustive UTXO/fee/native-capacity task
`t-01dd8f7354154bb8`. No old scan was resumed. Prior branches, checkpoints and
[PR #124](https://github.com/valargroup/wallet-pir/pull/124) remain preserved and
open. Production codecs, consumers, deployment and hardware qualification are
unchanged. [Remaining milestones](../transparent/docs/remaining-work.md#txid-display-sizing-and-independent-routing)
separate the bounded findings from subsequent implementation and release work.

## Coordinator census handoff (2026-10-07)

The sampled 128-byte recommendation above remains unchanged. Roman reports an
independent genesis display ingest running to height 3,508,673 on the coordinator;
this checkout has neither run that ingest nor received its result. The new
[read-only journal census command](../transparent/tools/txid-sizing/JOURNAL-CENSUS.md)
uses the existing committed-index and checksum-validating sidecar readers, shared
fee states and current display codec. It reports the requested size frontiers,
era/coinbase breakdowns, 40,000-record archive page demand, shared k-archive pages,
256/512/1024-row alternatives and distinct-candidate route intersections.

Stored bytes are measured separately from hypothetical exact-fee size bounds.
Packing endpoints are scenarios, with separate conservative page bounds covering
interior fee widths. Table allocations and preprocessing reservations are source
budgets, not native RSS or latency. Exact page/segment counts, timing and changing
revisions still require later joint-route qualification. The sidecar format cannot
count shielded-only exclusions: canonical total/eligible/exclusion counts and
ingest source/executable pins must accompany the coordinator JSON. Until that
receipt arrives, complete canonical-population qualification remains open.

## Source gate, anchor and probability design

The source/throughput choice completed at **00:57 UTC on 2026-10-04**, within
five minutes of agent start (00:52:13 UTC; eight-hour deadline 08:52:13 UTC).
The active model was verified as **gpt-6.1-sol, medium**. No configured sanctioned
bulk-export selector was found. The previous denied archive mount was not retried
or bypassed; no services, allowlists, permissions or infrastructure changed.
This is an access-discovery result, not proof that no export exists elsewhere.

One existing authorized read-only JSON-RPC SSH gateway was used after checking
process identity for overlapping scanners. Requests were sequential with a 40/s
ceiling, below the allowed 50/s. Only `getblockhash`, `getblock` and
`getblockchaininfo` were used: no `getrawtransaction`, per-input lookup or UTXO
reconstruction. The temporary mode-0600 key stayed outside Git and was deleted
when the session closed. The first 45 blocks took 36.08 seconds; their 123-minute
projection covered acquisition, not final rechecking. Even raw-only whole-chain
RPC has a 21.62h rate-ceiling lower bound; the two-read hash-pin/raw protocol in
the source-gate artifact has a 43.24h bound, excluding latency/extraction. Only a
bulk/local scan could meet the complete-population aspiration within this budget.
[Source gate](../transparent/evidence/txid-sizing/day-source-gate.json) records the
early decision to use a finite whole-range sample.

Fixed anchor: height **3,502,662**, hash
`0000000000228603173bfeb3650b51ceb92b1ac6b8b6392fa1fa9e9bac3acf7f`.
All sampled height/hash pins and the anchor were rechecked on the same session.
This verifies membership in the source node’s canonical view, not independent
full-chain consensus validation or the live node executable SHA.

Seed: `wallet-pir/one-day/shape-v1/` followed by the anchor hash. Python 3.12.3
selects 1,024 blocks uniformly without replacement in each disjoint stratum.
Exact selected heights and inclusion probabilities are retained in
[day-plan.json](../transparent/evidence/txid-sizing/day-plan.json). The strata
cover every height 0 through the anchor; all selected transactions are included.
Era subdomains inside the broad NU6 stratum retain their original selection
weights. The recent stratum is deliberately oversampled, never given equal
transaction weights with the archive.

| Selection stratum | Heights, inclusive | N blocks | n | Block inclusion probability |
|---|---:|---:|---:|---:|
| Sprout | 0–347,499 | 347,500 | 1,024 | 0.00294676 |
| Overwinter | 347,500–419,199 | 71,700 | 1,024 | 0.01428173 |
| Sapling | 419,200–653,599 | 234,400 | 1,024 | 0.00436860 |
| Blossom | 653,600–902,999 | 249,400 | 1,024 | 0.00410585 |
| Heartwood | 903,000–1,046,399 | 143,400 | 1,024 | 0.00714086 |
| Canopy | 1,046,400–1,687,103 | 640,704 | 1,024 | 0.00159824 |
| NU5 | 1,687,104–2,726,399 | 1,039,296 | 1,024 | 0.00098528 |
| NU6+ archive | 2,726,400–3,492,662 | 766,263 | 1,024 | 0.00133636 |
| recent 10000 blocks | 3,492,663–3,502,662 | 10,000 | 1,024 | 0.10240000 |

Population totals use Horvitz–Thompson weights N/n. Coverage is a ratio of
weighted inline and eligible totals, with block-cluster linearization and
stratum finite-population corrections. All transactions within a selected block
are a cluster; variable transaction counts are retained. Ordinary intervals
are pointwise normal 95%. Threshold selection uses one-sided Bonferroni bounds
over seven cutoffs and a separate 2,000-repetition stratified rescaled block
bootstrap, seed `wallet-pir/day/cluster-bootstrap/v1`. Both adjusted lower
bounds must exceed 80%. These are asymptotic sizing estimates, not formal
coverage guarantees; sparse/heavy tails may have poor interval coverage. An
unobserved tail gets no zero-variance absence claim.

## Canonical facts, eligibility and fee bounds

The canonical Rust parser and supported display-v1 encoder include **every
transaction with any transparent input or output**, including coinbase, mixed
pools, unusual scripts and input-only records. Shielded-only transactions with
neither are counted and excluded. No parent lookup is needed for input count,
shielded-component presence, outputs, identity or block membership. Shared flags
and `FeeState` remain authoritative; RPC independently checks identities,
coinbase/input counts, shielded presence, exact output amounts/raw scripts and
block inventories. Rust/Python current-codec bytes agree for every record.
All sampled transaction identities, including excluded ones, are deduplicated.

| Inventory | Observed sample | Weighted total estimate | Pointwise 95% interval |
|---|---:|---:|---:|
| All transactions | 58,079 | 18,571,501 | 17,901,287–19,241,715 |
| Eligible records | 54,053 | 16,918,745 | 16,332,719–17,504,771 |
| Shielded-only exclusions | 4,026 | 1,652,756 | 1,372,169–1,933,343 |
| Coinbase records | 9,216 | 3,502,663 | 3,502,663–3,502,663 |
| Noncoinbase records | 44,837 | 13,416,082 | 12,830,056–14,002,108 |
| Transparent outputs | 611,719 | 191,626,798 | 179,191,361–204,062,236 |
| Coinbase outputs | 22,404 | 10,304,245 | 10,289,260–10,319,230 |
| Noncoinbase outputs | 589,315 | 181,322,553 | 168,887,251–193,757,855 |
| Transparent inputs | 566,066 | 169,800,422 | 140,596,287–199,004,557 |
| Input-only records | 3,214 | 967,538 | 901,055–1,034,021 |
| Eligible records with shielded components | 8,701 | 2,630,285 | 2,507,213–2,753,356 |
| Raw-escape outputs / OP_RETURN in this sample | 886 | 46,625 | 35,271–57,978 |

Fee states in the sample: **9,216 non-applicable**, **7 exact zero**, **4,568
exact nonzero**, and **40,262 unknown**. No-transparent-input noncoinbase records
reuse the existing extractor for exact whole-transaction fees; records with
transparent inputs remain unknown in this parent-free path. These are extraction
states, not a financial claim that the underlying chain has unknown fees. Empty
scripts were not observed; no population-zero conclusion follows. The 886
raw-escape outputs were OP_RETURN scripts. No eligible unusual/input-only/mixed
transaction was excluded for missing parents.

For each unknown fee, current encoded bytes are measured as **unknown**, then
hypothetical exact-fee size is bounded by adding **1–8 ULEB128 bytes**. Canonical
nonnegative monetary constraints bound an exact fee by MAX_MONEY =
2,100,000,000,000,000 zatoshi; `uleb(0)` occupies one byte and `uleb(MAX_MONEY)`
eight. Flag width stays unchanged. Coinbase/non-applicable and known exact-zero
remain distinct. The following population coverage/packing bounds target an
exact-fee production population; they do not label unknown records actual exact
measurements. Worst-case membership already supports 128 bytes, so resolving
parents cannot change this decision and was unnecessary. Ambiguous routing
classes keep guaranteed/possible membership bounds.

## Cutoffs, frontiers and era differences

Coverage counts distinct eligible transactions, never outputs, fragments or
padding. Bound pairs vary the unknown-fee width; interval envelopes also include
pointwise sampling uncertainty. Counts below are weighted **millions**, not a
completed census. Fragment bounds account for a record becoming inline and for
fragment-boundary crossings.

| Current payload cutoff | Inline coverage bound | Pointwise 95% envelope | Inline records M | Overflow records M | Page fragments M |
|---:|---:|---:|---:|---:|---:|
| 128 | 92.27%–92.71% | 91.83%–93.15% | 15.610–15.686 | 1.233–1.308 | 2.016–2.092 |
| 192 | 93.96%–94.05% | 93.65%–94.37% | 15.898–15.912 | 1.007–1.021 | 1.790–1.805 |
| 256 | 94.32%–94.32% | 94.01%–94.63% | 15.957–15.958 | 0.960–0.961 | 1.744–1.745 |
| 384 | 94.65%–94.65% | 94.35%–94.95% | 16.013–16.014 | 0.905–0.905 | 1.689–1.689 |
| 512 | 94.82%–94.83% | 94.53%–95.12% | 16.043–16.043 | 0.875–0.876 | 1.659–1.659 |
| 768 | 95.05%–95.05% | 94.76%–95.34% | 16.081–16.082 | 0.837–0.838 | 1.621–1.622 |
| 1024 | 95.26%–95.27% | 94.99%–95.54% | 16.117–16.118 | 0.801–0.802 | 1.584–1.585 |

| Codec | 85% bytes | 90% bytes | 95% bytes | 99% bytes | 128-byte coverage bound |
|---|---:|---:|---:|---:|---:|
| display-v1 | 118–118 | 118–118 | 706–713 | 8,731–8,731 | 92.27%–92.71% |
| manifest-version | 117–117 | 117–117 | 705–712 | 8,730–8,730 | 92.31%–92.78% |
| script-compression | 103–103 | 103–103 | 585–592 | 7,233–7,233 | 92.92%–93.63% |
| common-counts | 102–102 | 102–102 | 585–592 | 7,233–7,233 | 92.99%–93.68% |

Current-codec pointwise inverted CDF intervals for the 95% frontier span
**418–1,034 bytes** after fee uncertainty; for 99%, **7,850–8,754 bytes**. These
are approximate pointwise inversions, not simultaneous percentile guarantees.
The estimated 99% frontier cannot fit the existing 4,096-byte directory row:
current framing limits one inline record to **4,044 bytes** (row count plus
entry envelope). Raising the cutoff to 192 yields about 94%, not 95%; raising
it to 1,024 estimates 95.26% worst-fee coverage, without an adjusted lower
bound above 95%. The largest observed bounded payload is 59,240 bytes; the
population maximum remains unknown.

`manifest-version` removes only the per-record version byte, requiring a new
manifest-bound codec identity. `script-compression` additionally encodes exact
P2PKH/P2SH/P2PK templates while retaining full key/hash bytes and a length-prefixed
raw escape. `common-counts` further packs two common counts into nibbles with
ULEB escapes. These are cumulative analysis proposals, not supported production
codecs. Their compact variable-length directory/page envelopes are a separate
proposal; directory locator length is bounded at 1–5 bytes. Existing fixed
envelopes and all proposal bytes are kept separate. Packing bounds inspect
every possible fee width because directory bytes can **decrease** when an inline
record crosses into overflow; simply subtracting endpoint averages is unsafe.

| Population domain | 128-byte coverage bound | 85 / 90 / 95 / 99% frontier bounds, bytes |
|---|---:|---|
| coinbase | 99.71%–99.71% | 118–118 / 118–118 / 118–118 / 118–118 |
| non_coinbase | 90.32%–90.89% | 68–75 / 99–106 / 1,302–1,302 / 9,635–9,639 |

| Actual era | Sample blocks | Eligible total estimate | Worst-fee 128 coverage | Coinbase | Noncoinbase |
|---|---:|---:|---:|---:|---:|
| Sprout | 1,024 | 3,613,796 | 92.10% | 100.00% | 91.26% |
| Overwinter | 1,024 | 510,372 | 94.03% | 100.00% | 93.06% |
| Sapling | 1,024 | 1,558,165 | 94.40% | 100.00% | 93.41% |
| Blossom | 1,024 | 1,044,850 | 94.85% | 100.00% | 93.23% |
| Heartwood | 1,024 | 664,205 | 87.98% | 100.00% | 84.67% |
| Canopy | 1,024 | 3,105,287 | 92.18% | 100.00% | 90.15% |
| NU5 | 1,024 | 3,255,920 | 87.19% | 99.02% | 81.64% |
| NU6 | 583 | 1,337,219 | 95.69% | 100.00% | 93.60% |
| NU6.1 | 276 | 1,126,197 | 96.81% | 100.00% | 96.09% |
| NU6.2 | 82 | 249,185 | 98.80% | 100.00% | 98.41% |
| NU6.3 | 1,107 | 453,549 | 96.62% | 100.00% | 95.98% |

The recommendation concerns the complete genesis-to-anchor population, not
a promise above 80% for every subdomain. NU5 noncoinbase worst-fee coverage
estimates **81.64%**, with pointwise interval **79.52–83.76%**. Coinbase is
99.71% globally, noncoinbase 90.32–90.89%. The full machine-readable
[analysis](../transparent/evidence/txid-sizing/day-analysis.json) reports all
seven cutoffs, output/fragment counts and frontiers by era and coinbase/noncoinbase,
with uncertainty. Those domain totals use original design weights, including
NU6.3’s two source strata.

## Joint routes and real candidates

Each model intersects **initial lookup bucket, overflow route, revision,
segment vector, initial/page request counts and modeled timing**. Candidate
counts are distinct real txids after global deduplication. Hash lookup and hash
overflow use separate domains. Temporal lookup means 50,000-height buckets;
coarse/broad means million-height buckets, not a replay of the live shard map.
Base scenarios freeze revision and assume the same normalized segment vector
within each bucket. Actual uneven segment layouts, refresh epochs, wall-clock
timing, wallet selection and prior linked script-history retrieval are not
measured. A prior history transcript can narrow every subsequent route.

`cover0` uses current distinct-choice queries and actual page counts: coincident
choice hashes leak one rather than two initial queries, with the correct bucket
salt. `cover3` sends two initial queries even when choices coincide and at least
three page queries for every opening, including inline records. Dummy queries
change the transcript; they never create real candidates. Records requiring
more than three pages still reveal excess. Fee ambiguity contributes to the
intersection only when both size endpoints have the same transcript; possible
membership is their union. Fragment counts are never counted as candidates.

| 128-byte joint model, cover3 | Observed classes | Possible point estimate <1000 / <10000 | Possible 95% upper <1000 / <10000 |
|---|---:|---:|---:|
| global/global:1/1 | 13 | 1 / 10 | 0 / 10 |
| coarse/global:1/1 | 20 | 3 / 13 | 1 / 13 |
| temporal/global:1/1 | 154 | 49 / 83 | 15 / 83 |
| hash/global:4/1 | 48 | 20 / 40 | 3 / 40 |
| hash/global:16/1 | 130 | 82 / 114 | 14 / 114 |
| hash/global:64/1 | 273 | 187 / 209 | 30 / 209 |
| hash/broad:4/1 | 67 | 22 / 51 | 4 / 47 |
| hash/hash:4/4 | 130 | 85 / 114 | 16 / 114 |
| hash/hash:16/16 | 538 | 275 / 282 | 51 / 282 |

These count **observed classes**, not population minima or class-weighted user
failure rates. Sparse normal intervals are weak and unseen classes have no
bound. Absence of an estimated class below five does not certify five candidates.
For four hash lookup/global overflow, the smallest observed excess-page class
contains **one sampled real record** and estimates **70 candidates**, interval
**0–206**. Its transcript has 12 page requests plus two initial requests; the
lookup bucket and global overflow are already intersected. Global overflow
cannot widen that initial lookup class. Fully global routing still has a thin
class estimating 339 candidates, interval 0–1,004. No sample-derived population
minimum is asserted.

For four hash/global buckets, cover3 reduces 65 observed classes without cover
to 48, but 20 still have possible point estimates below 1,000 and 40 below
10,000. The analytical 100-height refresh cohort yields 18,089 observed classes
(12,075 below 1,000); a 10-height timing cohort yields 22,065 (16,162 below
1,000). These are sensitivity refinements, not recorded HTTP timing or live
revision histories. Independent 4/4 and 16/16 lookup/overflow hashes add public
intersections; privacy cannot be assessed from either marginal bucket count.

Five-real-candidate controls are preserved and tested: repeated fragments and
padding cannot increase five; a narrower initial route remains narrow with
global overflow; coincident choices, segment responses, excess pages and
revision/timing refinements split classes as expected. K=1000 and K=10000 are
engineering policy candidates, not formal security guarantees. Before deployment,
a complete distinct-record/transcript audit must address every thin class and
retained revision. Any cover/geometry policy needs a common observable cohort;
a per-record global fallback advertises a special route and does not repair a
narrow lookup or history transcript. Fail publication/qualification when policy
conditions cannot be met, rather than claiming padding qualifies them.

## Packing and geometry costs: projected, not hardware qualification

At 128 bytes the sample-derived additive entry-byte bounds are **1.62–1.70 GiB
directory** and **4.45–4.47 GiB overflow**. These are fee bounds at HT point
totals, not statistical capacity upper bounds or a census packing replay. Row
slack/placement and skew still require complete-data measurement.

The following upper-fee-point scenarios assume 75% density, equal bucket load,
4,096-byte row width, 8,192 directory rows and 32,768 page rows per segment,
uniform openings, two initial queries and cover3. Average page queries are
3.0185. Setup reservations use the existing source formula, including roughly
64 MiB scratch per segment, not measured RSS. Native latency, concurrency and
RSS were **not benchmarked**: they cannot certify the unresolved privacy tails.

| Lookup / overflow buckets | Segments per bucket, lookup / pages | Allocated tables GiB | Source reservation GiB | Upload body MiB/open | Response body MiB/open | Scan-byte proxy GiB/open |
|---|---:|---:|---:|---:|---:|---:|
| 1 / 1 | 73 / 48 | 8.28 | 15.85 | 0.806 | 1.562 | 22.67 |
| 4 / 1 | 19 / 48 | 8.38 | 16.13 | 0.806 | 0.982 | 19.30 |
| 16 / 1 | 5 / 48 | 8.50 | 16.51 | 0.806 | 0.832 | 18.42 |
| 4 / 4 | 19 / 12 | 8.38 | 16.13 | 0.806 | 0.399 | 5.72 |
| 16 / 16 | 5 / 3 | 8.50 | 16.51 | 0.806 | 0.102 | 1.44 |

For four/global buckets, no page cover projects 0.175 MiB upload and 0.236 MiB
response per opening; it exposes inline/overflow, page counts and rare initial
choice counts. With cover3 the 128→192 cutoff change leaves modeled segment
counts and body cost unchanged. At 256 the global page count drops 48→47,
about 1.7% response reduction; at 768 the directory count rises 19→20. This is
not evidence to raise the cutoff simply to meet the >80% requirement.

The larger 32,768/65,536-row comparison for four/global has 5/24 segments,
projects **1.671 MiB upload**, **0.443 MiB response**, and **11.25 GiB source
reservation**. Smaller uploads versus fewer responses/setup allocations remain
a workload tradeoff; native latency and concrete client uplink/deadline constraints
are needed to choose final dimensions. Four/global is therefore a routing
baseline, not a qualified production fleet configuration. A small paired native
benchmark is a future gate only if those constraints can change this choice;
no exhaustive hardware/concurrency/recovery exercise ran here.

A row query uploads its query body once; segment evaluations multiply response
bodies and work, **not uploads**. Body estimates exclude HTTP/TLS/setup discovery
overhead. The scan-byte proxy is not CPU time or latency.
[Geometry evidence](../transparent/evidence/txid-sizing/day-geometry.json) includes
all seven cutoff sweeps and smaller/larger row alternatives, fee bounds and
explicit assumptions. Broad/hash overflow can reduce work substantially, but
the observed joint classes become narrower and remain unqualified.

## Exact provenance, retained work and validation boundary

This isolated task branch starts at wallet-pir **4ce39fbb7f605d94d08a9cdb0f0f2d6505fb93b1**.
Canonical parser: Zakura **af944f5194ef2e9921bc96af017629450375013c**.
Exporter release-fast binary SHA256:
`54fa6f2ae2caf4105d2d8d85581171f8ba5092e3abd67ed61a5783cc9b47115c`.
[Build pins](../transparent/evidence/txid-sizing/day-parser-build-pins.json) name
the resolved parser source-file hashes, rustc 1.97.1 and Python 3.12.3.
[Day source pins](../transparent/evidence/txid-sizing/day-sources.json),
[receipt](../transparent/evidence/txid-sizing/day-receipt.json.gz),
[compact statistics](../transparent/evidence/txid-sizing/day-statistics.json.gz),
[bootstrap](../transparent/evidence/txid-sizing/day-bootstrap.json) and
[checksums](../transparent/evidence/txid-sizing/SHA256SUMS) pin every input/output,
raw frame, canonical-record inventory and final summary. The node executable
SHA is unavailable through the allowed interface; parser pins do not attest it.

Measured acquisition plus final recheck took **165.73 minutes**,
with **36,867 read calls** and **405,888,419 raw block bytes**.
Canonical extraction's 129.16-minute elapsed receipt includes waiting for source
files; it is not a sustained capacity benchmark. Raw frames and canonical
records remain immutable outside Git at
`/home/ai-dev/.cache/wallet-pir-day-t-c3f424facb15494f/`. Final summary replay
checks their pinned checksums; only compact sanitized statistics/inventories
are checked in. No private wallet facts, secrets or encrypted queries are retained.

PR #124 head **7d46f7618b94f2a889dcfebd5b3dc9229bd98b5a** and its original
[vector source pins](../transparent/evidence/txid-sizing/sources.json),
[canonical data](../transparent/evidence/txid-sizing/canonical-sample.json),
[analysis](../transparent/evidence/txid-sizing/analysis.json) and
[geometry controls](../transparent/evidence/txid-sizing/geometry-projections.json)
are retained unchanged. Original wallet-pir source was
**ccbfd35fc21935f8a6878f93d288cc3107ea90e2**, metadata dependency
**3cfbc4848b92b1385b9215174251f8e51fbfa804**. Its 41 convenience vectors
(354,315 raw bytes, 134 eligible, only 60 prevout-resolvable, 74 missing-parent
exclusions) are conformance oracles, never pooled into these estimates. The
[new parent-free conformance receipt](../transparent/evidence/txid-sizing/day-vector-validation.json)
includes all 134 eligible records, preserves the known 60, checks independent
bytes/flags/outputs and refuses wrong hash/height/trailing bytes.

The old 0–30,811 prefix, raw cache and
[resume checkpoint/receipt](../transparent/evidence/txid-sizing/resume-receipt.json)
remain untouched and biased, not representative sample evidence. The historical
73.38h projection/72h stop is retained only as provenance, not an instruction
or prerequisite. Replacement work addresses #124’s eligibility, probability
source, fee-size bounds, present-era sizing and joint-route estimation gaps. It
does **not** close #124 or claim to complete its full-population/native gates.

Validation boundary: focused Python acceptance, release-fast exporter build and
canonical/vector checks, formatting, documentation links and offline deterministic
report/checksum regeneration; exact-head Fast and Full CI are owned by the
findings PR. Pending versus completed results are reported in that PR. The affected
`make check-fast` planner broadens these standalone tool paths to all 21 packages
and six groups, so broad local CI is skipped, not reported passing. PR CI carries
the repository gates. Full CI on main, exact-SHA release artifacts and hardware
qualification remain release gates; this work authorizes no merge or deployment.
