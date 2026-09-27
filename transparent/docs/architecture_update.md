# Transparent PIR architecture update: six-month wallet catch-up

Date: 2026-09-27. Status: architecture review and proposed design; no implementation,
deployment, or new performance qualification is claimed. Source was rechecked at
`2fb119ba`; measurements cited below retain their original dates and provenance.

## Objective and review outcome

Optimize fast, inexpensive transparent sync for an ordinary wallet waking after
up to six calendar months offline. Older recovery may incur more latency and
work. Early-chain spam and unusually large histories must remain recoverable,
but should not determine the latency target or storage layout for ordinary
recent wallets.

Review verdict: **request changes to the performance design**. Preserve public
time-based partitioning and the existing recovery contract. Prioritize fewer
private directory queries, smaller public membership data, and a bounded warm
recent set. This is an architectural assessment, not approval of a new
cryptographic construction or a production readiness verdict.

| Recommendation | Review outcome | Expected benefit | Evidence status |
|---|---|---|---|
| Single-lookup directory for sealed recent shards | Preferred prototype | Remove one directory query per matching script and reduce directory rows | Layout and wire-cost projection |
| Smaller membership filters, optionally combined with directory mapping | Preferred prototype | Lower public download cost for wallets testing tens of scripts | Probability and size projection |
| Separate ordinary and large-history page storage | Evaluate after the directory baseline | Prevent a small fraction of histories from controlling ordinary shard boundaries | Motivated by the recent census; no implementation measured |
| Warm rolling recent window with on-demand archive runtimes | Preferred infrastructure experiment | Reduce permanent archive memory requirements | Existing persistence is a foundation; cold-service performance is unqualified |
| Bounded concurrent retrieval | Near-term latency experiment | Reduce sequential network waits | Supported by source inspection and historical query timing |
| `recent-4k-8k` with the existing directory algorithm | Useful comparison and smaller first step | Lower selection upload without reducing page capacity | Already proposed in deployment documentation; requires placement validation |
| Larger inline allowance and different time intervals | Secondary parameter sweeps | Potentially avoid page queries or repeated shard matches | Workload-dependent; no selected replacement values |

Candidate values in this document are experimental inputs, not accepted
deployment settings. [Deployment](deployment.md) remains authoritative for
operating targets, [status](status.md) for observed implementation and live state,
and [remaining work](remaining-work.md) for tracked milestones. The
[contract](contract.md) continues to govern correctness, coverage, and privacy.

## 1. Baseline and workload interpretation

The current path downloads public shard filters, matches wallet scripts locally,
retrieves both candidate directory rows privately, and retrieves private event
pages when the two inline events do not cover the history. Directory entries and
pages are checked against exact script bytes. Public routing identifies a shard,
revision, and table; the selected row and script remain private.

Relevant sources are the [layout](../crates/transparent-shard/src/layout.rs),
[directory records](../crates/transparent-shard/src/records.rs),
[wallet retrieval](../crates/transparent-wallet/src/sync.rs), and
[query encoder](../crates/transparent-wallet/src/client.rs).

The September 8 [mixed-tier census](../evidence/census-2026-09-08/README.md)
covered genesis through height 3,473,686. Its recent range began at 3,262,749.
That frozen interval is the basis of the arithmetic here; it is not a current
rolling-window census.

| Recent-range quantity | Recorded value or derivation |
|---|---:|
| Shards | 14: thirteen sealed and one provisional tail |
| Directory/page rows in the baseline | 8,192 / 8,192 |
| Row width | 3,584 bytes |
| Directory slots per row | 14 |
| Scripts per shard, median / maximum | 34,509 / 45,454 |
| Directory slot utilization | 30.6% |
| Script-shard occurrences | 491,458 |
| Distinct scripts with recent activity | 378,346 |
| Mean recent shard matches per active script | 491,458 / 378,346 = 1.299 |
| Packed page rows | 103,767 |
| Histories requiring more than one page fragment | 7,694 |
| Page rows occupied by those histories | 74,512, or 71.8% |

The 7,694 long histories are 1.57% of script-shard occurrences. With two inline
events and 36 events per fragment, this class has more than 38 events within a
shard. These are not wallet population percentages or a classification of spam.
The [raw census](../evidence/census-2026-09-08/census.txt) shows that all thirteen
sealed recent shards closed on page-row demand.

The [third fleet-series repetition](../evidence/runs/fleet-series-2026-09-08-r3/README.md)
reported the following approximate payload costs at 32 concurrent wallets:

| Workload | Filters | Directory traffic | Page traffic | Setup | Total |
|---|---:|---:|---:|---:|---:|
| Six-month restore, ten scripts | 1.3 MB | 1.6 MB | 0.16 MB | 0.12 MB | 3.1 MB |
| Forty-script recent wallet | 1.3 MB | 14.1 MB | 3.9 MB | 0.4 MB | 19.7 MB |
| Old-birthday restore | 63.2 MB | 6.1 MB | 3.5 MB | 0.3 MB | 73 MB |

MB is decimal; traffic excludes HTTP headers and TLS framing. The small manifest
cost is included in the raw stage totals but not separately displayed in this
rounded table. These synthetic public-script wallets do not establish the
distribution of actual wallets, HD discovery costs, or mobile performance.
The six-month restore trace is a screening baseline, not a substitute for a
persisted wallet waking with an existing ledger. See the
[sample definition](../evidence/workload-sample-2026-09-08/README.md).

## 2. Single-lookup directories for sealed shards

### Proposed layout

Build a minimal perfect hash function (MPHF) over each sealed shard's indexed
scripts. It assigns each script a unique entry number in a dense directory.
Store the entries in that order, retaining exact script identity, inline events,
total event count, and private overflow locators.

The wallet obtains the public MPHF metadata, evaluates it locally, computes the
containing row, and performs one private retrieval:

```text
raw script -> local MPHF -> entry number -> row = floor(entry / slots_per_row)
                                             |
                                             v
                                      one private row query
                                             |
                                             v
                               exact script and record validation
```

An MPHF is not a membership test. An absent script can map to another script's
entry. Keep membership filtering and exact-key validation, or use the combined
fingerprint design in section 3. No row index or MPHF entry number is sent in a
plaintext request.

Established constructions offer a few bits of mapping metadata per key;
[PtrHash](https://arxiv.org/abs/2502.15539) is one implementation candidate.
Its published construction and lookup performance is not a measurement on this
repository's scripts or target devices. Budgeting three bits per key is a
screening assumption to replace with serialized artifact measurements.

At fourteen entries per row, a 4,096-row table has 57,344 slots. Every recent
shard in the frozen census fits this dense directory, including the maximum
45,454-script shard. This establishes capacity for that dataset, not future
capacity, construction success, or an implemented schema.

### Cost outcome

For one matching script in a one-segment shard, using the existing Transparent
PIR encoder and unchanged row width:

| Directory | Queries | Packing-key upload | Selection and binding upload | Response download | Total private traffic |
|---|---:|---:|---:|---:|---:|
| Current two-choice, 8K rows | 2 | 172,032 B | 83,984 B | 10,272 B | 266,288 B |
| Two-choice, 4K rows | 2 | 172,032 B | 40,976 B | 10,272 B | 223,280 B |
| Proposed MPHF, 4K rows | 1 | 86,016 B | 20,488 B | 5,136 B | 111,640 B |

The baseline sizes agree with the retained
[September 20 wire measurements](../../evidence/live-pir-2026-09-20/REPORT.md).
The 4K rows are encoder arithmetic, not measured candidate wallet runs. Public
filters, MPHF metadata, manifests, table setup, overflow pages, and retries are
additional. Historical setup responses were approximately 19.4 KB per opened
table segment and revision; they are separate from the fresh packing keys sent
with every private request.

Halving directory rows saves about 16.2% of directory traffic. Combining dense
4K placement with one query saves about 58.1%, before public mapping costs.
Mapping all 491,458 recent entries at three bits each costs approximately
184,297 bytes before artifact framing. This is why removing a query is the
larger opportunity: the packing-key upload is a substantial fixed cost.

### Offline evaluation, 2026-09-27

The [single-lookup census](../evidence/single-lookup-census-2026-09-27/README.md)
evaluated a lighter variant first. A PIR response returns a whole row, so the
wallet needs the row, not the entry. A per-shard xor-retrieval table (about 1.23
bits per script) records which of the two existing candidate rows holds each
script. Placement, directory rows and the server are unchanged.

- It built on all 14 recent shards with no seed retries, at up to 7,006 bytes per shard.
- A wallet fetches the table only for shards it already opens.
- Projected restore-6m payload falls about 24% at recent-8k, and 40-script payload
  about 35%. These are projections, not measurements.

Two-choice placement at 4,096 directory rows reached 14 of 14 slots in one shard.
recent-4k-8k therefore needs a denser placement, and is not selected with this
variant. Wallet measurement remains open.

The source now implements this variant as an optional manifest field, not a
schema change. Manifests without it keep their bytes and digests, and existing
wallets ignore it and query both rows. The publisher adds tables only to
revisions it builds anew (`--directory-choice`, default `off`). The builder and
the server verify every entry's route. The wallet sends one query per matched
script when a table is present. None of this is published or deployed; see
[status](status.md).

### Scope and requirements

Use this layout first for sealed recent shards. Retain the existing directory
for the provisional tail so that every tail update does not require distributing
a new MPHF. Tail-to-sealed publication remains an explicit revision transition.

Bind mapping bytes, entry count, hash algorithm and seeds, row geometry, and
directory contents into the manifest. Verify construction against every exact
input script, including detection of collisions introduced by any preliminary
fixed-width key hashing. Validate malformed metadata and out-of-range outputs
before indexing local arrays or creating a query.

Fetch complete metadata artifacts for the public coverage interval. Fetching
only a script-selected piece of an MPHF would expose script-derived information.
Unknown layouts must fail explicitly. Introduce a new schema/profile rather
than changing the meaning of an existing registry name.

As a smaller implementation step, compare the existing two-choice algorithm
under the already proposed [recent-4k-8k geometry](deployment.md#geometry-optimization-not-a-launch-dependency).
Retaining 8K page capacity avoids the extra boundaries that shrinking both
tables can cause. Actual two-choice placement and block-boundary capacity must
still be replayed; dense capacity arithmetic is insufficient for that algorithm.

## 3. Membership metadata tuned to the wallet workload

### Lower-precision direct filters

The [current filter profile](../crates/transparent-filter/src/profile.rs) fixes
`P=19` and `M=784931`, the Bitcoin basic-filter values. Its nominal false-positive
rate is approximately `1/M`. [BIP 158](https://bips.dev/158/) describes the
tradeoff between filter size and falsely matched block downloads. Here, a false
match causes directory retrieval, so the optimum should be measured for that
cost and for the number of scripts a wallet actually tests.

An illustrative candidate is `P=12, M=4096`. For `A` absent scripts tested
against `S` shards, the expected false script-shard matches are approximately
`A * S / M`. Linearity of expectation does not require independent outcomes;
tail probabilities and adversarial cases still need separate testing.

| Scripts tested across fourteen shards | Expected false matches | Expected extra traffic with current directory | Expected extra traffic with proposed 4K MPHF directory |
|---|---:|---:|---:|
| 10 | 0.034 | 9.1 KB | 3.8 KB |
| 40 | 0.137 | 36.4 KB | 15.3 KB |
| 100 | 0.342 | 91.0 KB | 38.2 KB |
| 1,000 | 3.418 | 910.2 KB | 381.6 KB |

These estimates treat all tests as absent, charge query payloads only, and omit
additional table setup or manifest downloads caused by a first match. A false
match is resolved by exact directory comparison and never creates a ledger
event. Include unused HD derivation candidates, imported scripts, and discovery
passes when counting wallet tests.

Under the usual geometric-gap approximation, a GCS uses approximately
`P + 1 + 1 / (exp(2^P / M) - 1)` bits per element. The example changes this from
about 21.05 to 13.58 bits, roughly a 35% reduction. Actual encoded sizes must be
measured on complete filter element sets, including supported coverage rules
and any elements excluded from private tables. The candidate is a new versioned
application profile; it is not an in-place change to the current profile.

### Combined mapping and fingerprints

An MPHF can also index a public array of short fingerprints. The wallet computes
the mapped entry, checks an independently derived fingerprint locally, and
queries the private directory only on a match. This replaces the separate GCS
and MPHF downloads with one membership-and-location artifact.

With three mapping bits and twelve fingerprint bits per indexed entry, the
491,458-entry recent set would require about 921,484 bytes before framing.
The nominal absent-key false-positive rate is `2^-12`, subject to the selected
construction and hash assumptions. Use separate hash domains for placement and
fingerprints, verify inclusion for every indexed script, and measure absent
probes. Unsupported scripts must remain explicitly outside coverage rather than
being reported absent because the combined structure indexes supported scripts.

The full raw script in the private directory remains the authority for an exact
match. A fingerprint hit alone cannot advance address-use discovery or authorize
events. Artifact digests provide revision consistency; they do not prove that
the publisher included every chain event.

### Whole-wallet screening projection

The raw [fleet report](../evidence/runs/fleet-series-2026-09-08-r3/report.json)
contains 123 `restore-6m` syncs at concurrency 32. Summing its stage payloads gives:

| Quantity | Mean per recorded sync |
|---|---:|
| Total payload | 3,146,413 B |
| Filter download | 1,293,378 B |
| Directory upload and responses | 1,573,914 B |
| Directory requests | 11.821 |

Holding the workload and all other costs fixed, replace each pair of directory
queries with one 111,640-byte request/response exchange:

```text
directory-only substitution = total - old_directory + (directory_queries / 2) * 111640
                            = approximately 2.232 MB per sync

retain GCS; add 3-bit MPHF    = approximately 2.417 MB per sync

replace GCS with 15-bit
mapping plus fingerprints   = approximately 1.860 MB per sync
```

The combined design therefore screens at roughly 1.9-2.0 MB versus 3.1 MB.
These are arithmetic substitutions, not replayed candidate outcomes. They assume
the same matching pairs and page work, omit new artifact framing and changed
setup traffic, and apply the single-lookup substitution to all directory work.
The first implementation's retained two-choice tail must be accounted for
separately. New false positives add work; construction, mobile CPU, and network
latency are unmeasured. The 58.1% directory saving is not a 58.1% whole-sync saving.

## 4. Isolate large histories and then choose time boundaries

Introduce separate logical storage for inline histories, ordinary overflow,
and bulk histories within a public interval. Keep the directory compact. Let
bulk storage grow independently of the capacity that decides when an ordinary
directory or small-page table seals.

The census provides the motivation: 1.57% of recent script-shard histories
consumed 71.8% of packed page rows, while every sealed recent shard closed on
page-row demand. A wallet with a small history consequently pays for boundaries
partly determined by unrelated large histories.

This change is not permission to truncate, omit, or publicly retrieve a large
history. Preserve total counts, private locators, complete event replay,
durable pending work, and bounded resumption. Receiving spam is not necessarily
a wallet owner's choice. Work limits leave recovery explicitly incomplete.

The existing packed-page design already saves space by sharing rows between
short histories. Preserve that benefit. The proposed change separates capacity
and sealing pressure; it does not assume short-page packing is missing.

A public small/bulk table choice can expose a history-size class. Establish
whether the selected policy reveals anything beyond the accepted query-count
leakage, or hide table selection. Never route by an exact page locator or an
address-derived partition. Merely adding overflow segments is not sufficient:
under the current privacy design every segment answers a query, so segment
growth can still increase ordinary query work and responses.

After separating bulk pressure, compare current capacity-driven intervals with
approximately fortnightly and monthly intervals. Use a small registry of
explicit geometries and retain an oversized-block escape path. Compare larger
per-query tables against fewer matched intervals, fewer setup downloads, and
publication cost. A calendar boundary must not create an unbounded table under
a busy interval.

A single six-month table is not the preferred starting point. The frozen census
averaged only 1.299 matched recent shards per active script. A global table
removes limited duplication for the average active script while making each
private query search a larger table. Reused-address wallets can benefit more;
evaluate them separately. This mean does not establish wallet-level latency or
the optimum interval length.

## 5. Rolling warm residency and colder archive service

Keep the latest six calendar months fully warm and replicated, including the
shard crossing the cutoff and a measured transition margin. Retain immutable
older publications and compatible prepared runtime snapshots on durable
storage, restoring archive tables into a bounded RAM cache on demand.

The historical mixed census attributed 56.4 GB of 57.2 GB plaintext to archive
data and approximately 90 GiB of runtime reservations to the two archive
assignments. This makes archive residency a stronger infrastructure-cost lever
than a modest reduction in recent directory rows.

This is a proposed change to the current full-warm archive policy. Define archive
readiness, setup/query admission, cold-restore deadlines, retry behavior, and
cache eviction explicitly. Public routing may identify the requested historical
interval, but must continue hiding selected rows. Separate archive queues and
resource budgets so cold restores cannot delay recent publication or sync.

Prepared snapshot restore is preferable to cryptographic preprocessing on every
cold query. Measure both the compatible-snapshot case and missing/corrupt-cache
rebuilds, including disk bandwidth, scratch memory, and concurrent requests.
Bound work under churn or an adversarial sequence of archive shard requests.
No smaller host specification, monetary saving, or archive latency is accepted
by this review alone.

Aging changes ownership and residency after verified copying. It must preserve
shard ids, geometry, revisions, and coverage lineage. Re-cutting old shards into
a different archive geometry remains a separate migration with explicit replay
and rollback semantics. See [architecture: aging](architecture.md#wallet-state-and-aging).

The recent memory benefit also needs accurate accounting. The current
[runtime reservation](../services/transparent-shard-server/src/runtime.rs)
includes 96 MiB of packing matrices per one-instance table, independent of its
row count. An 8K directory reserves approximately 128 MiB; a 4K directory
reserves 112 MiB. With the page table unchanged, a shard changes from roughly
256 to 240 MiB, a 6.25% reservation reduction. This is not a process RSS
measurement or a claim that halving rows halves server cost.

## 6. Bounded concurrency and request scheduling

The current wallet retrieves the two candidate directory rows sequentially.
They can be issued concurrently without changing their count or stopping on a
first-bucket hit. Other independent scripts and shards can also be scheduled
with a bounded number of requests in flight.

The September 20 [query measurements](../../evidence/live-pir-2026-09-20/REPORT.md)
recorded about 500 ms median end-to-end latency for a recent directory query
and about 12 ms for its worker evaluation stage. These are different timing
scopes, and independently computed percentiles must not be subtracted. The
observations support testing fewer sequential waits and smaller uploads; they
do not establish universal mobile latency or server capacity.

Preserve revision binding, accepted-anchor checks, atomic coverage commits,
durable pending pages, cancellation, and bounded retry budgets while scheduling
requests. Coordinate concurrent overload responses rather than creating retry
bursts. Measure publication contention and throughput as well as one wallet's
latency.

Batch evaluation is a later server-throughput experiment. Concurrency alone
does not save bandwidth. Reusing uploaded secret-dependent packing keys is a
protocol change requiring a separate security argument; it is not an ordinary
HTTP caching optimization. The client must also avoid skipping a private query
because an unrelated response happened to include another watched script:
that can expose row-sharing information through the transcript.

## 7. Secondary alternatives and rejected shortcuts

| Alternative | Outcome and reason |
|---|---|
| Raise the inline allowance | Re-sweep on recent workloads. Two inline events cover about 73.9% of recent script-shard histories; four would cover about 85.6% at the same boundaries. Wider entries reduce slots per row and can require a larger directory, so page-query savings alone are insufficient. |
| Compact event encoding | Secondary layout experiment. The current event representation is 96 bytes. A smaller canonical encoding could improve inline and page density, but must retain exact receive/spend identities and all ledger fields. Smaller records do not automatically reduce the fixed PIR row response. |
| Recent parent filters | Lower priority. The retained experiment found no improvement in its primary recent objective. It tested parent/child traversal, which does not settle the separate direct-filter precision proposal. |
| Archive parent filters | Retain as a separately evaluated archive option. Offline filter savings do not establish total HTTP performance; the paired performance experiment remained incomplete. |
| One global six-month table | Comparison candidate for reused addresses, not the default. Larger query geometry can outweigh the limited duplicate recent shard matches for ordinary scripts. |
| Smaller directory and page tables together | Do not assume a win. More frequent page-capacity seals can add filters, setup, and queries. Test the independent directory/page dimensions. |
| Public address-prefix or script-hash sharding | Incompatible with the current protected-recovery routing boundary. Hiding routing or querying every partition changes the cost calculation. |
| Stop after the first successful two-choice bucket | Rejected: the request count reveals placement-dependent information. A single-lookup structure removes the ambiguity instead. |
| UTXO-only snapshot as a replacement | Rejected for equivalent functionality. It omits outputs received and spent entirely while the wallet was offline and loses zero-balance history. Snapshots may supplement a complete delta/event path. |

Inline percentages are derived from the same recent census, not wallet
percentages. Its 120,733 short paged histories plus 7,694 long histories leave
363,031 of 491,458 histories entirely inline. Adding the 26,629 three-event and
31,169 four-event histories gives the four-inline estimate. The
[event model](../crates/transparent-events/src/lib.rs) defines the information
that any alternative encoding must preserve. The
[parent-filter evidence](../evidence/parent-filters-2026-09-08/README.md)
records the experiment's scope and incomplete HTTP outcome.

## 8. Recovery, privacy, and compatibility invariants

A six-month-only sync is valid for a returning wallet because it retains its
earlier ledger and complete coverage. New spends must still consume outputs
received before the recent window. Moving the birthday forward is not ledger
continuation. Fresh seed restoration, newly imported scripts, and newly derived
scripts can require earlier discovery or recovery under the wallet's existing
rules; they are separate workload cases, not silently reclassified as recent.

Preserve these properties across every proposed layout:

- Recover every required receive and spend, including zero-balance and entirely
  offline history, using the wallet's independently accepted chain anchor.
- Keep exact raw script identity and explicit supported-script coverage. Do not
  turn unsupported or excluded data into a false absence result.
- Bind setup, requests, responses, public metadata, and stored coverage to the
  same publication revision and geometry.
- Commit ledger changes and coverage atomically, recover interrupted pending
  work, and rewind to the accepted ancestor on reorgs.
- Keep script-derived routes, row indices, page locators, addresses, outpoints,
  and transaction identifiers out of plaintext protected-recovery requests.
- Retain explicit incomplete state after budget exhaustion, stale revisions,
  malformed artifacts, or outages. Never fall back to public address retrieval.
- Keep fixed serving geometry within each declared table and review any new
  observable table-class, request-count, or timing behavior.

The trust model remains the one in the [contract](contract.md): the publisher
and retrieval service are trusted for complete, correct indexing, while PIR
protects the selection. MPHF metadata, fingerprints, manifest digests, and exact
script checks do not prove chain completeness or publisher non-equivocation.

Roll out changed formats under new versioned schemas and geometry names.
Publish and validate candidate artifacts before activation, retain a compatible
predecessor for rollback, and make coverage/cache handling for format changes
explicit. None of these recommendations permits silently reinterpreting an
existing profile or migrating a wallet's stored coverage by assumption.

## 9. Evaluation sequence and decision criteria

The recommended experiment order is:

1. Reproduce a baseline on one pinned recent interval and workload. Establish
   directory/query counts, public bytes, setup cost, and returning-wallet ledger
   equality before comparing variants.
2. Compare the existing two-choice layout with `recent-4k-8k`, retaining page
   capacity and checking actual placement under identical data.
3. Compare a sealed-shard MPHF directory with unchanged filters, isolating the
   directory benefit and charging for the full public mapping download.
4. Compare lower-precision direct GCS and combined MPHF/fingerprint metadata,
   measuring false matches and complete wallet cost.
5. Evaluate separate bulk-history capacity and alternative public intervals;
   include their new table setup, preparation, and privacy costs.
6. Evaluate bounded client concurrency, first independently and then with the
   selected layout, under realistic network conditions and active publication.

Cold archive service is an independent infrastructure experiment and can be
evaluated alongside the recent-layout work. This sequencing describes evaluation
dependencies; operational milestones remain in [remaining work](remaining-work.md).

Use persisted wallets waking after one, three, and six calendar months, plus
shorter absences to catch regressions. Include inactive wallets, multiple
accounts, reused addresses, old outputs spent recently, newly imported scripts,
and discovery candidates that never received funds. Explicitly vary tested
script counts; 10, 40, 100, and 1,000 are useful sensitivity points rather than
assumed population frequencies. Keep fresh restoration and heavy histories as
separate cohorts. Do not infer wallet population weights from the synthetic
sample or from uniform sampling of on-chain scripts.

Measure public metadata, manifests, database-dependent setup, packing-key
upload, non-key query upload, and response download separately. Include retries,
false matches, boundary overfetch, and fresh versus retained client caches.
Record client CPU and peak memory, request rounds, end-to-end latency, server
work, runtime residency, cold restore latency, and publication cost. Test both
regional and realistic mobile/WAN paths; server evaluation time alone cannot
select the wallet design.

Compare exact events and UTXOs against an independent reducer, not balance
alone. Exercise malformed and absent-key lookups, construction collisions,
revision replacement, accepted prefixes, reorgs, cancellation, interruption,
unsupported coverage, multi-segment overflow, and cold-cache failures. Verify
that routing and query transcripts respect the declared privacy boundary.

Select a design on whole-wallet cost and latency for the intended ordinary
cohorts, with correctness and privacy preserved. Heavy histories may have a
different performance objective, but failures and incomplete recoveries must
remain visible in reporting. Do not silently remove them to improve aggregate
statistics. Report mean, median, and tail outcomes with sample counts and an
explicit workload mix; do not call synthetic weights an average real wallet.

After qualification, record accepted parameters in [deployment](deployment.md),
implementation and observed rollout results in [status](status.md), and raw
measurements with provenance under [evidence](../evidence/README.md). Until then,
the projected savings in this review remain candidate-design estimates.
