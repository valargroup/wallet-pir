# Transparent PIR deployment plan

Date: 2026-09-07. Status: **committed architecture and geometry**. The
residency, revision and profile work this rests on is implemented in the
working tree (§2); the fleet is conditional on the six measurements in §7 and
the six outstanding items listed at the end of §2. Two of the geometry
figures here have never been censused and are labelled where they appear.

This replaces the earlier draft of this file, which proposed four candidate
profiles and left the archive geometry pending. It commits to a geometry per
height range and prices every trade-off, including the ones that go the wrong
way.

## 1. Objective, constraints, and the decision

Optimize the ordinary returning wallet: daily or weekly synchronization with
fast catch-up through the most recent six months, while keeping complete
restoration from genesis possible. The
[recovery contract](transparent_pir_contract.md) requires that a fresh
restoration cover genesis through its accepted anchor, so the archive is
mandatory, not optional.

Operator constraints:

- DigitalOcean, in the existing deployment environment.
- Approximately **$800–1,200/month for serving**, excluding the chain node,
  event indexer, and publisher.
- A measured capacity envelope rather than an assumed user count.
- Single serving copies for the archive; the recent tier is replicated because
  replication there is free (§6).
- Preserve the trusted-indexer model and PIR row privacy conditional on public
  shard selection, timing, and query counts.

**The decision.** Two serving tiers, split by *age*, each published as its own
shard set and served by its own process:

| Tier | Heights (2026-09-07 anchor) | Directory rows | Page rows |
|---|---|---:|---:|
| Recent | 3,261,694 – tip | **4,096** | **8,192** |
| Archive | 0 – 3,261,693 | **32,768** | **65,536** |

The boundary is a six-month timestamp-derived cutoff from the pinned anchor's
chain timestamps, snapped to a forced shard boundary, and it advances in
discrete epochs rather than continuously (§5).

**Why two tiers.** Not because a single geometry cannot work, but because the
two ends of the chain are bound by different resources:

- The archive is wide **because of the RAM budget**. One geometry at 8,192
  chain-wide costs 273 GiB of runtime — 23 hosts bare, 26 with the measured
  allocator overhead, roughly $1,900–2,200/month and outside the envelope.
  Widening to 32,768/65,536 cuts that to 90 GiB.
- The recent window is narrow **because of republish cadence and the
  newest-shard hotspot**. Its tail is rebuilt every block period and read by
  every wallet on every sync.

Both halves of that are argued with numbers in §4. Note up front what the
archive geometry does **not** buy: it is not cheaper in wallet bytes, and it
costs about 2.2× the server memory bandwidth per restoration (§4.4).

## 2. What has already landed, and what has not

Most of what an earlier draft listed as prerequisite work is **implemented in
the working tree** (uncommitted at the time of writing). This section records
the state so the plan is not read as asking for work that is done.

Landed, in `server/transparent-shard-server/src/runtime.rs` and
`shardset.rs`:

| Concern | State |
|---|---|
| Plaintext retained for process lifetime | **Fixed.** `LoadedShard` now holds `SegmentSource` — a path, row shape and digest. `verify()` streams the file to check length and digest "without retaining the bytes", so a fleet-sized set no longer needs its whole plaintext resident to prove it is intact. |
| No eviction, no byte budget | **Fixed.** `RuntimeCache` is a byte-bounded cache that reserves budget *before* a build starts, from the projected size, so the budget is enforced rather than merely exceeded. |
| Runtimes evicted while in use | **Fixed.** `RuntimeHandle` pins a runtime for the life of a request; the cache will not count pinned bytes as freeable. |
| No single-flight | **Fixed.** `RuntimeCache` is single-flight, so a cold-shard request herd builds once instead of N times. |
| Builds on the reactor thread | **Fixed.** The build runs under `tokio::task::spawn_blocking`, with a `build_slots` semaphore bounding concurrent construction. |
| Overload answered as a client error | **Fixed.** Exhaustion is a distinct retryable condition answered with `503`, not the previous `400`. |
| A withdrawn revision failed the whole sync | **Fixed**, client and server. The wallet carries `409` and `503` as types and recovers from both; see §8.3. |
| Second tail republish made the set unloadable | **Fixed.** The publisher writes one directory per shard *revision* named by that revision's manifest digest, and `ShardSet::open` now treats a directory the map does not name as a superseded revision rather than junk, with a bound on retained revisions. A wallet that took setup for a revision and then queried is answered from *that* revision or told to refresh. |

The protocol side has also moved further than "proposed": `manifest.rs` is at
schema **`transparent-shard-v7`**, `layout.rs` carries a closed `PROFILES`
registry, `SharedParams::build` takes a `Geometry` and a `Table`, and the
wallet prepares parameters per geometry with an explicit `UnknownGeometry`
failure and a `by_name` registry lookup rather than trusting a server's
numbers. The builder takes a `&'static Geometry` instead of reading the raw
constants.

That settles a question this document previously left open. Two geometries live
in **one published set addressed by profile**, not in two sets served by two
processes. The plan follows the implementation.

**What is still outstanding**, and what §8 is about:

1. **The recent profile this plan wants does not exist in the registry.**
   `PROFILES` holds `recent-8k` (8,192/8,192), `recent-4k` (4,096/**4,096**),
   `archive-32k` (32,768/32,768) and `archive-wide` (32,768/65,536). §3.2
   argues for **4,096 directory over 8,192 pages**, which is none of these —
   `recent-4k` narrows the page table too, which doubles the shard count for no
   gain. Either add that pairing or amend `recent-4k`.
2. **Neither `shard-census` nor `shard-publish` takes a height range**, so the
   two-tier partition still cannot be scored or published (§7, item 6).
3. **The wallet still has no verified source for a shard's geometry.** It
   validates a *declared* geometry against its compiled registry, which stops
   an unknown shape, but nothing yet proves the declared shape is the one the
   shard was built at (§8.2). Revision binding, by contrast, is now complete on
   both sides (§8.3); what remains adjacent to it is the caller-side coverage
   composition described in §8.5.
4. **`archive-wide` has never been censused** (§3.3, §7).
5. **`--placement` has never been run over the full journal** (§3.4).
6. **The router does not exist** (§6.4).

Re-verify this section against the tree before acting on it; it describes
uncommitted work by a concurrent session.

## 3. Geometry

### 3.1 The two profiles

| | Recent | Archive |
|---|---:|---:|
| Directory rows / page rows | 4,096 / 8,192 | 32,768 / 65,536 |
| Seal policy | `49152:57344,7936:8192` | `393216:458752,63488:65536` |
| Directory query upload | 106,504 B | 258,056 B |
| Page query upload | 128,008 B | 430,088 B |
| Published setup, per table | 14,336 B | 14,336 B |
| Response, per segment | 5,136 B | 5,136 B |
| Runtime, directory / page segment | 112 / 128 MiB | 224 / 352 MiB |
| Runtime per shard | 240 MiB | 576 MiB |
| Plaintext per shard | 42 MiB | 336 MiB |
| Scanned per query, dir / page | 16 / 32 MiB | 128 / 256 MiB |
| Events per shard | ~313,000 | ~2,178,000 |

Row bytes stay at 3,584, two inline directory events, 14 directory slots per
row, packed short histories, and the existing vetted PIR parameter derivation.
Both row counts are multiples of `POLY_LEN = 2048` and pass
`Geometry::validate()` (`pir/transparent-shard/src/layout.rs:528`).

Per-segment runtime is `rows × 2048 × 2` bytes of `u16` database plus a
**fixed 96 MiB** of pack matrices, because `db_cols` is 2,048 at every
candidate row count. That is why table width is nearly free in memory while it
divides the shard count, and it is calibrated against the 128.5 MiB/segment
measured by `shard-residency` at 8,192.

Query, setup and response bytes are the values pinned in
`server/transparent-shard-server/tests/geometry_costs.rs`. Setup and response
follow `db_cols`, which no candidate changes, so **only upload moves with row
count**.

### 3.2 Why the recent directory is 4,096 rows, not 8,192

`shard-utilisation/census-v5.txt` covers heights 3,428,143–3,473,474 at
19.2 events/block — an almost exact density match for the recent tier's 19.9.
Across its five script targets, rows per script climbs steadily:

| Script target | Scripts/shard | Packed rows | Events/shard | Rows/script |
|---:|---:|---:|---:|---:|
| 4,096 | 4,078 | 357 | 17,385 | 0.088 |
| 8,192 | 8,018 | 954 | 43,464 | 0.119 |
| 12,288 | 11,712 | 1,653 | 72,440 | 0.141 |
| 16,384 | 16,171 | 2,547 | 108,660 | 0.157 |
| 24,576 | 20,298 | 3,482 | 144,880 | 0.172 |

Extrapolating that trend to the 7,936-row page target gives roughly **36,000
scripts per recent-tier shard** — 31% of a 8,192-row directory's 114,688
capacity, and 63% of a 4,096-row directory's 57,344.

Narrowing the directory therefore costs **nothing in shard count**, because
pages bind either way, and buys:

- directory query 106,504 B instead of 128,008 — **17% less**;
- **64 MiB scanned per incremental query instead of 96** — the query every
  wallet makes constantly;
- 240 MiB of runtime per shard instead of 256;
- 63% directory fill instead of 31%, which matters for §3.4.

### 3.3 Why the archive is 32,768 directory over 65,536 pages

The three full-chain censuses measure the symmetric geometries. Observed
maximum scripts per shard against directory capacity is **86%** at 8,192,
**49%** at 32,768 and **31%** at 65,536 — the directory has slack that grows as
tables widen, and at 32,768 and 65,536 *no* shard reaches the script target at
all. Pairing a 65,536-row page table with a 32,768-row directory puts the
observed 284,221 maximum at **62% of 458,752 capacity** while keeping the
directory query at 258 KB rather than 430 KB.

It is also the only candidate that wins on **both** plaintext and resident
memory. Halving the directory removes 160 × 117.4 MB from the 65,536/65,536
figure:

| Geometry | Shards | Fleet plaintext | Runtime RAM |
|---|---:|---:|---:|
| 8,192 / 8,192 | 1,091 | 64.1 GB | 273 GiB |
| 32,768 / 32,768 | 314 | 73.8 GB | 137 GiB |
| 65,536 / 65,536 | 162 | 76.1 GB | 111 GiB |
| **32,768 / 65,536** | **162** | **57.1 GB** | **90.0 GiB** |

This contradicts, as a general rule, the observation in
[the deployment measurements](transparent-pir-deploy.md) that fleet plaintext
grows as resident memory falls. It grows when both tables widen together; it
falls when only the page table does.

**This pairing has never been censused.** Every archive figure in this document
is derived from the 65,536/65,536 run plus the pinned cost formulas. Gate 0
scores it for real (§7).

**24,576 directory rows is a live alternative, not a rejected one.** At 344,064
capacity the observed 284,221 maximum sits at 82.6% — the earlier draft
reported that as 3.6% headroom by comparing against the derived *target*
rather than the capacity. `seal.rs`'s `capacity` is a hard limit and
`WouldExceedCapacity` closes a shard *before* it overruns, so ordinary
accumulation cannot spill into a second segment at any headroom; the real cost
of a tighter directory is that a few shards seal on scripts instead of pages.
24,576 would save 96 MiB of runtime and 28 MiB of plaintext per shard and drop
the directory query to about 215 KB, for perhaps three to five extra shards.
Score it in gate 0 and take it if placement holds.

### 3.4 The binding risk is directory placement, and it fails silently

Directory capacity is `rows × 14`, but placement is two-choice hashing with
BFS relocation. On failure `place_directory` does **not** error —
`pir/transparent-shard/src/build.rs:486` increments the segment count and
retries, and only errors once `segments > MAX_DIRECTORY_SEGMENTS` (1,024). So
placement failure is silent for the first thousand segments. So a shard that fails to place
becomes a two-segment shard **with no operator signal**, and every wallet that
touches it pays double directory queries forever, plus another 128 MiB (at
8,192) or 256 MiB (at 32,768) resident.

Two-choice placement into 14-slot rows is safe to about **77% of slot
capacity**. The shipped pin sits at **86%**, and `--placement` has never been
run over the full journal — the only evidence is a `shard-publish` comment
about placing 511 of 511 shards, which describes the *genesis* journal
(heights 0–330,000, 9.4% of chain height), the same run whose shard-count
extrapolation turned out 55% wrong.

**This is a larger unquantified risk than `g`, and it is cheap to close.**
It is also the real argument for the geometry chosen here: the archive lands at
62% fill and the recent tier at 63%, both inside the safe band, whereas the
current pin is above it.

### 3.5 What is not a lever

**`INLINE_EVENTS` cannot vary per tier.** `records.rs:46-57` derives
`DIRECTORY_ENTRY_BYTES` and `DIRECTORY_SLOTS` as compile-time constants from it
behind a static assertion, and `encode`/`decode_directory_row` are hard-wired
to them. A per-tier inline allowance is a **record-format change**; a per-tier
row count is only a PIR-parameter change. Report the asymmetry and vary rows,
never inline.

Lowering it is strictly worse. Two inline events keep 79.18% of active scripts
off the page table entirely; at zero inline events roughly 38.65M of 60.65M
script-shard occurrences would acquire page rows, adding about 27% to page rows
and therefore about 27% to shard count at the same page geometry, plus 50% more
queries per script. The four-fold directory capacity buys nothing, because
pages bind. Raising it to three slots is interesting at wide geometry where the
directory is idle, but 327,680 capacity against the observed 284,221 is 86.7% —
exactly the placement trap of §3.4.

### 3.6 Superseded evidence

[The page-geometry decision summary](transparent_pir_geometry_summary.md) is
**superseded and must not be cited for a row count.** It concludes for 5,120
page rows and also scores 7,168 and 3,072; none of the three is a multiple of
`POLY_LEN = 2048`, so all three are rejected by `Geometry::validate()`. That
sweep scored unservable geometry.

The `genesis-*` censuses cover heights 0–330,000 — 9.4% of chain height, and
the densest part. They report 511 shards and 30.01 GB where the whole chain is
1,091 shards and 64.1 GB. Only the `fullchain-*` files describe the whole
chain.

## 4. The height map, and what each tier is worth

### 4.1 Event distribution

Integrated from the 849 four-thousand-block density buckets in
[the full-chain census](transparent-pir-evaluation/shard-utilisation/fullchain-8192.txt),
whose sum reproduces the census total of 352,873,356 to within 0.0013%. Buckets
crossing a boundary are apportioned by block count.

| Heights | Blocks | Events | ev/blk | Share | Shards at 8,192 pages | Shards at 65,536 pages |
|---|---:|---:|---:|---:|---:|---:|
| 0–348,159 | 348,160 | 167.05M | 479.8 | 47.34% | 516 | 77 |
| 348,160–599,999 | 251,840 | 38.23M | 151.8 | 10.83% | 118 | 18 |
| 600,000–999,999 | 400,000 | 24.60M | 61.5 | 6.97% | 76 | 11 |
| 1,000,000–1,499,999 | 500,000 | 43.80M | 87.6 | 12.41% | 135 | 20 |
| 1,500,000–1,999,999 | 500,000 | 33.10M | 66.2 | 9.38% | 102 | 15 |
| 2,000,000–2,499,999 | 500,000 | 20.67M | 41.3 | 5.86% | 64 | 9 |
| 2,500,000–3,049,701 | 549,702 | 15.10M | 27.5 | 4.28% | 47 | 7 |
| 3,049,702–3,261,693 | 211,992 | 6.11M | 28.8 | 1.73% | 19 | 3 |
| 3,261,694–3,439,125 | 177,432 | 3.51M | 19.8 | 0.99% | 11 | — |
| 3,439,126–3,473,686 | 34,561 | 0.70M | 20.3 | 0.20% | 2 | — |

Both shard columns use the chain-wide mean events per shard for that page
geometry, so they are comparable with the censuses: the left column sums to
**1,090** against the measured 1,091, and the archive column sums to **160**
over heights 0–3,261,693. The recent tier's own count is calibrated separately
against recent-era density in §4.2, which gives ~313,000 events per shard
rather than the chain mean of 323,440.

Activity is concentrated early: 47.34% of all events fall below height 348,160
and 58.17% below 600,000. The last six months are **1.19%** of all events.

### 4.2 How the shard counts are derived, and where the derivation is weak

Page-row demand binds in 1,042 of 1,091 shards at the current pin, so events
per shard is near-constant within a geometry, and
`shards(range) = events(range) / mean_events_per_shard` follows. Three
independent checks:

1. The estimator reproduces all three full-chain censuses exactly — 1,091 at
   8,192 (mean 323,440 events/shard), 314 at 32,768 (1,123,800) and 162 at
   65,536 (2,178,230).
2. The underlying constant is **events per page row**, and it barely moves
   between eras: the full chain needs 8,637,252 packed rows for 352,878,022
   events (**40.9 events/row**), while the recent-era journal needs 3,482 rows
   per 144,880-event shard (**41.6**). It exceeds `EVENTS_PER_PAGE = 36`
   because two inline directory events keep most short histories off the page
   table.
3. Applying that constant directly: 4.21M recent-tier events at 40.9
   events/row is 102,933 page rows, over the 7,936-row target — **13.0 sealed
   shards**, matching route 1.

**The weakness is where the plan leans hardest.** Events per shard spreads
±25% at 8,192 (244,145 to 413,456) but only ±10% at 65,536, so the estimator
is *least* reliable for the recent tier and most reliable for the archive,
where the answer is measured anyway.

The 48 script-bound shards can be located without `--per-shard`: the sparsest
buckets run 5.8–8.2 events/block in **2,750,000–3,080,000**, and at
5.87 events/block the reported minimum of 244,145 events spans 41,593 blocks —
exactly the reported maximum block span. That trough is three times sparser
than the recent tier, so it does not contaminate it, but it means the archive
contains a roughly 330,000-block band where scripts, not pages, bind at 8,192.
At 32,768/65,536 that band is absorbed, since no shard there reaches the script
target.

### 4.3 The published totals

| Tier | Heights | Shards | Runtime RAM | Plaintext | Filters |
|---|---|---:|---:|---:|---:|
| Recent | 3,261,694 – 3,473,686 | 14 | 3.3 GiB | 0.62 GB | ~1.3 MB |
| Archive | 0 – 3,261,693 | 160 | 90.0 GiB | 56.4 GB | 59.0 MB |
| **Total** | | **174** | **93.3 GiB** | **57.0 GB** | **~60.3 MB** |

Recent is 13 sealed shards plus a tail, and the tail pins its full geometry
however empty it is. The archive's last shard seals on *height* rather than a
limit, so it carries on average half a shard of slack — about 1.09M events —
at a full 576 MiB of runtime. Both are one-off costs, but they are real.

Recent filter sizes are derived from script counts rather than measured: filters
scale with distinct scripts, and a recent shard's ~36,000 against an archive
shard's ~147,670 gives roughly 90 KB per recent filter against the archive's
369 KB.

### 4.4 What the archive geometry costs, honestly

Widening the archive is a RAM and host-count decision. It is **not** cheaper in
wallet bytes, and it is worse on server work.

`g`, the number of shards a script appears in, falls **sublinearly** with shard
count. Measured script-shard occurrences across the three censuses are 60.645M,
32.277M and 23.923M — a ratio of about **0.73 per doubling**. Divided by the
**9,264,547 distinct indexable scripts** the tool counted, mean `g` runs
**6.55 → 3.48 → 2.58**, with p99 140 → 58 → 35 and maxima 986 → 297 → 156.
Scripts per shard rise 1.44× when shards halve, eating half the benefit.

The occurrence figures and the 0.73 ratio above are measured. An earlier
revision of this paragraph scaled them from the frequently cited "27.4 mean" to
give 27.4 → 14.6 → 10.8; that anchor implies 2.21M distinct scripts and is
wrong. "27.4 mean, 563 p99" belongs to a **genesis census covering 9.4% of
chain height**, not to this journal, and nothing derived from it describes the
whole chain. Full-chain per-script figures are in
[fullchain-geometry-comparison.md](transparent-pir-evaluation/shard-utilisation/fullchain-geometry-comparison.md),
which is the authoritative cross-geometry comparison.

Whole-chain restoration, summed over every script in the chain:

| Geometry | Requests | Upload | Download | Server scan |
|---|---:|---:|---:|---:|
| 8,192 / 8,192 | 146.69M | **18.78 TB** | 753 GB | **1.00×** |
| 32,768 / 32,768 | 83.68M | 21.59 TB | 430 GB | 2.28× |
| **32,768 / 65,536** | **63.80M** | 19.21 TB | **328 GB** | 2.17× |
| 65,536 / 65,536 | 63.80M | 27.44 TB | 328 GB | 3.48× |

So the archive pairing is the best of the wide options and within 2% of 8,192
on upload, with 2.3× fewer round trips and less than half the download — but it
costs **2.17× the server memory bandwidth**. Since evaluation is
bandwidth-bound and saturates at two threads, restoration throughput per dollar
is roughly flat. What widening actually buys is **host count**.

**And it is a bet against evaluation-key reuse.** 86,016 bytes of every query
are packing keys at every row count — 67% of a 4,096-row query, 81% of an
8,192-row one — and that fixed term currently masks the row term. With the
measured four-way reuse applied, whole-chain restoration upload becomes 8,192
→ **6.16 TB**, 32,768/32,768 → 14.40 TB, and the archive pairing → 13.72 TB.
If reuse ever lands, 8,192 is 2.2× cheaper on upload and the wide archive
becomes the worst wide option.

Reuse is measured and unbuilt: [the reuse study](transparent_pir_reuse.md)
records upload falling from 106 KB to 49 KB per query, at four times the
preparation time (11.1–11.8 s against 2.8–3.2 s) and four times the combined
client RSS (4.0–4.5 GB against 2.07 GB) — a real barrier on mobile. **Record
the bet: re-sharding is the only way out of it**, and re-sharding is expensive
for the reasons in §5.

Do not budget a navigation saving either: page-skipping measured **negative**,
7.4% worse cold and 8.0% worse warm.

### 4.5 What the recent tier is worth

This is the strongest claim in the plan, and it is about cadence and the
hotspot rather than about totals.

| | Recent geometry | Archive geometry |
|---|---:|---:|
| Shard block span | ~16,300 blocks (~14 days) | ~110,000 blocks (~95 days) |
| Tail rebuild | 0.67 s/segment, 29.4 MB | ~1.8 s/segment*, 235 MB |
| Daily sync, scanned | **64 MiB** | 512 MiB |
| Measured query p50 / p95 | **27.62 / 28.66 ms** | 91.01 / 92.35 ms |
| Filter for the touched shard | ~90 KB | ~369 KB |

The latency figures are the only measured latency-versus-geometry curve in the
repository, from
[the mainnet study](transparent_pir_mainnet_study.md) on an 8-vCPU Xeon: the
3.3× is sublinear in the 8× scanned bytes, consistent with a bandwidth-bound
kernel. The earlier draft's caution that halving scanned bytes does not halve
latency is correct, and this curve settles it rather than leaving it open.

\* Build time is measured only at 8,192 (0.67 s per 128.5 MiB segment). Wider
build times assume linearity in runtime bytes and are **unmeasured**. The
plaintext figures, `rows × 3,584`, are exact.

The recent tier costs 3.3 GiB of runtime — 3.5% of the fleet — to make the
request every wallet issues constantly eight times cheaper in server work and
3.3× faster.

### 4.6 The objection to per-height geometry, answered

[The full-chain geometry notes](transparent-pir-evaluation/shard-utilisation/fullchain-geometry-notes.md)
record the position that density changes are "a burst plus a continuous climb,
not a threshold", so "a geometry chosen per height era would have no stable
boundary to key on."

That is right about density and it is not an objection to this design, because
**the boundary here is keyed to age, not density.** Density is already absorbed
by content-based sealing: at a single geometry the block span already varies
1,260×, from 33 blocks to 41,593. What the two tiers separate is *access
pattern and republish cadence*, and that boundary is sharp — the newest shard
is rebuilt every block period and read on every sync, and nothing behind the
cutoff is ever rebuilt again.

## 5. Aging: the boundary advances in epochs

A sealed shard cannot change geometry. `RevisionError::SealedChanged` forbids
content change, `ShardMap::check_shape` requires `shard_id == index` and
gapless heights, and `ShardSet::open` requires `parent_manifest_digest` to
chain. Re-sharding a range therefore renumbers every later shard and rewrites
every subsequent parent digest. So exactly one of two things is true:

**(a) Shards never migrate.** Recent shards keep their geometry forever and
accumulate in the archive tier: 420,480 blocks/year at 19.9 events/block is
8.37M events/year, about **27 shards and 6.5 GiB of runtime per year**.

**(b) Shards migrate**, which means republishing with renumbered ids from the
boundary onward: rebuilding the range, twice the disk and — without a router —
twice the RAM through the changeover, and **every wallet's cached coverage past
the boundary invalidated**, so wallets re-sync six months of archive every six
months. That defeats the tier.

**Decision: (a) for the pilot, with the growth budgeted, and epoch-based
re-cutting specified but deferred**, triggered when aged recent-tier runtime
exceeds one host. An epoch transition needs the same coverage rollback and
replay path as a tail replacement; clients must re-cover the range rather than
treat new shard ids as disjoint new history.

One consequence worth stating: because the window's cost is a growth rate
rather than a level, **the width of the initial window is a one-time initial
condition, not a standing RAM ranking.** A six-month window is the cheapest at
t=0, but the gap between a six-month and a three-year window — roughly 17 GiB —
is erased in about two years whichever is chosen. Pick the boundary late, and
budget about one extra host per twenty months.

## 6. Fleet, placement, and routing

### 6.1 Reference deployment

| Component | Size | Count | $/mo |
|---|---|---:|---:|
| Archive workers | 4 vCPU / 16 GB, 12 GiB cap, ~18 shards each | 9 | $756 |
| Recent workers | 4 vCPU / 8 GB, each a **full replica** | 4 | $192 |
| Router / coordinator | 2 vCPU / 4 GB | 1 | $24 |
| Filters, map, published setup | Spaces / CDN origin | 1 | $5 |
| **Total** | | **15** | **~$977** |

Prices are as of this document's date and must be re-verified before
provisioning. The estimate excludes the chain node, indexer and publisher,
taxes, backups, and transfer overages.

Archive sizing uses **650 MiB per shard** — 576 MiB of runtime plus the
measured 12.8% allocator fragmentation — against the deployed `MemoryMax=12G`
with swap off, where an overrun is an OOM kill rather than graceful
degradation.

Small hosts rather than large ones: evaluation is memory-bandwidth-bound and
**saturates at two threads** (sixteen threads return 1.39× one core), while
`$/GB` is flat at $5.25 from 16 GB to 256 GB. Capacity scales with host count,
not core count. Four vCPUs match two evaluation slots at two threads each.

**This budget depends on the plaintext fix in §2.** Were plaintext retained, an
archive shard costs 912 MiB, the tier costs **142.5 GiB**, and the fleet needs
13–14 archive hosts at roughly **$1,310–1,400/month — outside the
envelope.**

### 6.2 The recent tier is replicated, not sharded

At 3.3 GiB the whole recent set fits one 8 GB host, so replication is free —
and it is strictly better than splitting the tier by table or by height:

- The hotspot is a single shard, the newest. Height-splitting concentrates it
  on one host; table-splitting halves it across two but pins each wallet's
  serial directory-then-page dependency to two fixed hosts.
- With four full replicas, any host answers any request, so the router spreads
  the hotspot across **four independent memory paths**.
- The tier every wallet touches daily stops being a single copy.

### 6.3 Assignment and routing

- Archive: contiguous ascending shard ranges, one per worker, append-only.
  Shard ids are assigned by the sealer in height order and stable once sealed.
  Balance by estimated resident bytes, not equal block counts, and keep
  published tables on included local SSD.
- Route on `(tier, shard_id, table, segment)` only. Every one of those is
  public. Never route on the private directory result and never on a script
  hash.
- The directory/page split is public and independent of the selected script.
- Serve immutable public artifacts — filters, map, published setup — from the
  Spaces/CDN origin so filter-only checks never occupy a PIR worker. Keep the
  same paths available on the worker as well, as the transparent Caddyfile
  already argues, so a wallet can fetch public and private bytes over different
  network paths by changing only a base URL.

Two geometries make a request body's length reveal which tier it addresses. The
tier is already a public function of the shard id, so this is not a new leak —
but the request-body bound must become exact **per profile**. It is not today:
`service.rs:169-173` computes `max_query_bytes` as the *maximum* over every
profile the process holds, so a recent-tier endpoint would accept a 430 KB
archive-sized body. That reintroduces the size channel the fixed-length check
exists to close.

### 6.4 What the fleet line items do not include

- **The router is unimplemented software, not a $24 line item.** The unit pins
  one `--shard-dir` and one process serves the whole set; shard-range
  assignment and request routing do not exist yet. This is the largest
  engineering item in a multi-host fleet.
- **The archive keeps single copies.** Losing a host makes a height range
  unqueryable, and there is no health-based routing to notice. The recent
  tier's replicas fix this only for the recent tier. Recover a lost
  assignment from verified published artifacts; do not claim high availability.
- **Egress is the real bandwidth line, and it is filters, not PIR.**
  Whole-chain restoration downloads only 328 GB summed over every script in
  the chain, while each full sync fetches about 60.3 MB of filters — a thousand
  full syncs a day is roughly 1.8 TB/month against a pooled droplet allowance.
  Filters are immutable and already served `max-age=31536000, immutable`; move
  them onto object storage and off the droplets entirely.
- **Publisher peak RSS at 65,536 is unmeasured.** `shard-publish` buffers each
  shard's events at once — 2.18M events at 96 bytes, plus scripts, the
  per-script history map, 352 MB of emitted tables and placement structures
  over 32,768 rows. Plausibly 1.2–1.5 GB against roughly 200 MB at 8,192, on
  the coordinator alongside the event ingester. The censuses' 321 MiB proves
  nothing here: a census never builds.

### 6.5 Residency and scheduling

Keep the recent working set prepared at all times, including the advertised
tail revision. Size each archive worker's byte-bounded cache to hold its whole
assignment, so cold construction is a restart concern rather than a
normal-path one.

Cache misses load local bytes and rebuild. Use the byte-bounded LRU with
single-flight construction per runtime identity and bounded construction and
query concurrency. Keep active runtimes pinned until their queries finish. If
there is no safe capacity, return an explicit retryable overload response —
never silently exceed the cache budget, and never advance wallet coverage on
failure. Keep archive work off recent workers.

## 7. Gate 0: measurements required before provisioning

Each is one full-journal census pass, about three and a half minutes at
321 MiB resident, through the `census` action of
`.github/workflows/backfill-transparent-events.yml` against
`/srv/zakura/transparent-event-data`.

1. **`--placement` at the current pin and both candidates.** The unmeasured
   risk that dominates `g` (§3.4). Every full-chain census to date models
   placement as `scripts / slots`, which is a **lower bound** on segments.
2. **`--per-shard` at the current pin.** Confirms the 48 script-bound shards
   sit in 2,750,000–3,080,000 as derived, and reads events per shard for
   3,261,694–3,473,686 directly instead of extrapolating.
3. **Score both candidates.** `--directory-rows-per-segment 4096 --policy
   49152:57344,7936:8192`, and `--directory-rows-per-segment 32768
   --page-rows-per-segment 65536 --policy 393216:458752,63488:65536`. Score
   24,576 directory rows too (§3.3). **The archive pairing has never been
   censused.**
4. **`--shard-matches` at one wide geometry.** The *ratio* between geometries
   is already measured (§4.4), so only one absolute anchor and the tail
   distribution are still needed. Record and remove the spill space.
5. **`shard-scaling` on a droplet.** The saturation curve was measured on
   16-core Apple Silicon and no raw output is archived. Also measure publisher
   peak RSS at 65,536.
6. **New tooling: `--split-at HEIGHT` for both census and publish.** Neither
   tool takes a height range — `shard-publish` always starts at
   `store.start_height()`, and census sealing is stateful from genesis — so
   **the two-tier partition cannot be scored today**, and the recent tier's
   shard count remains an extrapolation until it can. This is a prerequisite,
   not a convenience.

**Rejection rules, stated in advance.** If `--placement` shows second directory
segments at the archive pairing, fall back to 65,536/65,536 — 162 shards,
111 GiB of runtime, a 430 KB directory query. If the 4,096-row recent directory
seals shards on scripts rather than pages, fall back to 8,192. If neither
archive candidate validates, hold one geometry chain-wide and say plainly that
the fleet then needs either a working eviction path or a larger budget; do not
report the archive decision as merely pending.

## 8. Remaining implementation

§2 records what has landed. This is what is left.

### 8.1 Add the recent pairing, and range-aware tooling

The registry's `recent-4k` narrows both tables to 4,096. That is not the shape
§3.2 argues for: halving the page table halves events per shard and therefore
**doubles the shard count**, which costs filter bytes, setup bytes and `g`,
while the whole benefit of a narrow recent table is in the *directory*. Add a
profile at **4,096 directory over 8,192 pages**, or amend `recent-4k` to that
pairing and rewrite its doc comment, which currently justifies itself on
"43,008 fewer upload bytes per matched shard" — a directory argument attached
to a page change.

`Geometry::validate()` already accepts it: both counts are multiples of
`POLY_LEN = 2048` and the row width is a whole `INSTANCE_BYTES` instance.

Then make the tools range-aware, which is the blocker for every number in §4.3:

- `shard-publish` starts at `store.start_height()` unconditionally, so a
  recent-tier set anchored at 3,261,694 cannot be published at all.
- Census sealing is stateful from genesis, so an archive sealed at
  `archive-wide` and stopping at a chosen height — with the recent tier
  re-sealing from that height at its own geometry — cannot be scored.

Both need a `--split-at HEIGHT` with a per-range geometry and policy. Until
they have it, the recent tier's shard count stays an extrapolation.

### 8.2 Give the wallet a verified source for geometry

The wallet now rejects a geometry outside its compiled registry, and its own
comment has the reason exactly right: whoever chooses the geometry chooses what
the response reveals. But rejecting an *unknown* shape is not the same as
verifying a *known* one.

Geometry used to be trustworthy because it was a compiled constant on both
sides. Now that a shard declares it, a service that declares `recent-8k` for a
shard actually built at another shape changes the wallet's `candidate_rows`
modulus: the wallet asks for the wrong row, finds nothing, and records the
shard as unproductive. That is a **silent, targeted false negative that reads
to the user as "no history"** — and no signature covers the map.

Close it by binding the declared geometry to the shard's committed identity:

- route `manifest.json`;
- have the wallet recompute `ShardManifest::digest()` and compare it against
  the `manifest_digest` the map gives for that shard;
- take the row shape from the verified manifest rather than from the map or the
  init response;
- verify `parent_manifest_digest` chains, so a substituted shard cannot stand
  alone.

Every manifest field already reaches the digest, so geometry is bound into
shard identity for free once the wallet checks the digest. That makes lying
about geometry require forging the whole committed sequence, which the wallet
already binds to its accepted chain through `terminal_block_hash`.

Also worth pinning while the profile surface is new: keep the fixed-length
request-body check exact **per profile**. A single `max_query_bytes` taken as
the maximum across profiles would let the recent tier accept a 430 KB body,
reintroducing the size channel that check exists to close. Two profiles do make
body length reveal which tier a request addresses, but the tier is already a
public function of the shard id, so that is not a new leak.

### 8.3 Revision-bound service state

Include the manifest revision digest, tier, table and segment in runtime,
setup and request identity. Reject a query against a different revision rather
than answering it with the newest state. Bound retained revisions and make
expired ones explicitly retryable through map refresh and correct coverage
recovery. `PublishedRevision::next` and `RevisionError::SealedChanged` already
supply the rules.

**Both halves are now built.** The service refuses a withdrawn revision with
`409` carrying its current map digest, and an unfreeable cache budget with
`503` and a `retry-after`. On the wallet side, `transport.rs` carries those two
refusals as types rather than letting `error_for_status` reduce them to a
status — the map digest lives in the body, so the body has to be read before
the reply is discarded — and `sync()` acts on them: a withdrawn revision
refetches the map, checks it is the same set *continued*, and re-derives that
shard from whatever replaced it, while an overload waits and re-asks without
spending a map refresh.

Three properties are worth stating because they are what make that sound:

- **Coverage never advances over a range that was not read.** It is set only
  after a shard is fully retrieved, so a refusal leaves it where it was.
- **A refreshed map is not independently trusted.** The caller validated the
  map the sync started from against the wallet's accepted chain; a map fetched
  mid-sync inherits that validation only over the range the two agree on, so
  `check_continuation` requires identical set identity and an entry-identical
  already-covered prefix. Sealed content is immutable, so a disagreement there
  means a different set rather than a longer one — and once it passes, the shard
  the sync resumes at provably begins exactly where coverage stopped.
- **The digest the service reports is never a trust input.** It arrives from the
  private query origin, and letting it decide which map bytes a wallet accepts
  would give that origin a say in the wallet's view of the set — the very thing
  separate filter and query sources exist to prevent. It is used only to
  recognise that a refresh cannot help, and for diagnostics.

Recovery is bounded: four map refreshes per sync, and every refresh must
actually change the refused revision, so a tail republished faster than a slow
wallet reads it ends in a named refusal rather than an unbounded re-read.

Note that `DEFAULT_RETAIN_REVISIONS = 3` means a wallet merely racing one
republication is still answered. A `409` indicates a wallet several revisions
behind, or one holding a map from a different publication lineage.

### 8.4 Publication readiness and ownership

Force a shard boundary at the tier transition. Prepare a new tail revision on
its assigned recent workers before advertising it, and publish the coherent map
and routing state atomically. On an epoch transition, copy and verify shard
artifacts at the destination before changing ownership.

### 8.5 Returning-wallet correctness

Retain the wallet ledger and checkpoint. Fetch public discovery data for
uncovered ranges, reuse validated immutable setup, and retrieve only the
history required after the checkpoint where the navigation protocol permits it.

An output received years ago and spent today must be handled from the retained
ledger plus the recent spend event. **A six-month recent tier is not permission
to discard old UTXOs**, and a fresh wallet restoring from an older birthday
must still retrieve complete historical coverage — through the archive tier —
before claiming a synchronized balance. Cache reuse must be revision-bound; a
provisional replacement replaces the range it covered rather than extending it,
and must not be appended as if it were disjoint new history.

An earlier draft of this section said that replacement "requires the existing
coverage rollback and replay behavior". **There is no such behavior**, and the
phrase should not be read as describing one. `Ledger` has no rollback and no
truncate-at-height, `sync()` builds its ledger from the birthday on every call,
and nothing yet consumes `SyncOutcome::provisional` — so the replace-not-extend
contract that `ProvisionalCoverage` documents is enforced *within* a sync (§8.3)
and remains unimplemented on the caller's side of the checkpoint.

That caller-side half is what a returning wallet needs, and it is a distinct
piece of work: composing two syncs requires a way to sync into a ledger the
caller already holds. Resuming by moving the birthday forward is **not** that
mechanism — a spend in the resumed range of an output received before it would
land in `unresolved()`, which is the correct answer for a genuinely later
birthday and the wrong one for a continuation.

### 8.6 Deployment configuration

Per-worker tier, shard and table assignments; cache budgets; concurrency
limits; prewarming and readiness checks. Generate router configuration from the
public assignment map. Keep health, queue and cache diagnostics on the private
operational surface — `/v1/health` reports shard counts and how many runtimes
are built, which is operator information.

Raise `transparent_worker_count` and correct the stale sizing comments in
`ops/infra/digitalocean/production/variables.tf` and
`deploy/transparent-shard-server.service`, which describe a 21-shard pilot; the
deployed set is **3 shards, 169 MB, heights 3,428,143–3,473,474**. Mind the
recorded Terraform hazards before any fleet apply: SSH open to `0.0.0.0/0` as
drift from Terraform, untracked local state that wants to create a
`transparent_spend_worker` nobody decided to provision, and a coordinator
volume looked up by an id that does not match the volume's name.

Fix in passing: the 28,672-script directory capacity claims at
`layout.rs:380` and `shard-census.rs:184`, which predate
`DIRECTORY_ROWS = 8192` — real capacity is 114,688. And the claim at `layout.rs:112`
that fragments rise as tables widen ("9.94M to 12.12M over the genesis
journal") is contradicted at full-chain scale, where they fall: 25.40M →
19.13M → 15.95M. The surrounding argument — that page queries fall with shard
count — is right; the 22% rise it concedes is a genesis-journal artefact and
should be restated.

## 9. Validation and rollout

### 9.1 Dataset and workloads

Pin a complete journal and derive the six-month boundary from its anchor
timestamp. Run the census with real placement, per-shard reporting, and exact
shard-match aggregation for each candidate.

Cover unused wallets; small active wallets; daily, weekly and 30-day catch-up;
six-month restoration; old-wallet restoration; and heavily reused or spammed
scripts. Report the last separately without removing them from the overall
evidence. Chain-script groupings are synthetic wallet proxies, not a measured
wallet population: exchanges and reused addresses dominate the chain-level
tail, and the same `g` statistic differs by more than tenfold between the
genesis and recent journals.

Measure total upload and download, setup and filter cache state, query counts,
completion-time distributions, revision churn, retries, peak client memory,
**whole-process server RSS under concurrent HTTP load**, cold construction,
cache misses and queue time. Use at least 100 traces per ordinary workload
class and three independent runs. Report device and network; desktop or
loopback measurements do not establish mobile latency, which remains
unmeasured.

### 9.2 Required correctness cases

- Exact event multiset, UTXO set and transaction-history equality against
  independent journal replay. Equal balances alone are insufficient.
- Cross-tier boundaries and multi-segment queries, with uniform segment access
  and no script-dependent routing.
- Old output with a recent spend; cached ledger continuation; complete
  restoration from an old birthday across both tiers.
- Wrong geometry, corrupted table, stale setup, mixed revision, and a manifest
  whose digest does not match the map — each rejected explicitly.
- Interrupted queries, overload shedding, eviction while requests are active,
  concurrent cold requests for one shard, worker restart, expired revisions.
- Tail replacement, reorg and ownership handoff without skipped or duplicated
  coverage and without prematurely advancing the checkpoint.
- **A second and third tail republish against a live set**, against the
  multi-revision handling recorded in §2.

### 9.3 Staged rollout

1. Commit and verify the residency, revision and profile work recorded in §2,
   then close the outstanding items in §8. Produce candidate publications into
   new directories, preserving the existing published set as the rollback
   baseline.
2. Run gate 0 and apply the rejection rules. Publish the outputs beside the
   existing `fullchain-*.txt` with their command inputs, journal coverage,
   elapsed time and peak RSS.
3. Benchmark one recent worker and one archive worker on the intended
   DigitalOcean sizes, over real HTTP, while publishing revisions and running
   archive restorations concurrently.
4. Report the maximum sustainable completed sync rate and define normal
   operating load at no more than half of it, with stable queues and no OOM.
5. Expand toward the reference fleet only when measured working-set size and
   request distribution justify the hosts. Validate assignment balance and the
   newest-shard hotspot at fleet scale.
6. Advertise the new publication only after readiness and exact replay checks
   pass. Roll back routing and publication coherently; clients retain
   revision-bound coverage and resume rather than silently accepting a lower or
   inconsistent anchor.

The pilot accepts service interruption if an archive worker fails. Replicas for
the archive, epoch re-cutting, bounded evaluation-key reuse, and stronger
traffic-pattern privacy remain separately measured follow-up work — with the
note from §4.4 that key reuse would invert the archive geometry decision and
that re-sharding is the only way to act on it.
docs/transparent_pir_deploy_plan.md