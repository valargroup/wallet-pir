# Txid display genesis journal census, 2026-10-07

Raw output of the read-only journal census (`txid-sizing-export --journal-census`,
wallet-pir PR #129 head `452f50d4`) over a genesis-to-3,508,673 txid display journal
ingested on the coordinator on 2026-10-07, and a layout analysis of that output
done on 2026-10-08. The analysis is derived arithmetic. Nothing was built,
deployed or measured on a production host.

## Result

**Today's `txid-2k` layout does not fit the 64 GB host planned for genesis.** The
earlier plan assumed one page segment per archive: 850 runtimes and 33.2 GiB.
Old archives have far more large transactions than the live ones, so their page
tables span up to 9 segments of 2,048 rows. Real page demand gives 899 page
segments and 426 directories, which is **1,325 runtimes and 51.8 GiB held** at
built size (93.2 GiB at the reservation). With a year of growth that needs a
71G `MemoryMax`, and a 64 GB host allows 49.7G.

**Recommendation:** keep the current layout (`txid-2k`, 40,000-record archives,
pages not shared, 128-byte inline cutoff) with the reservation true-up, on an
`m-16vcpu-128gb` host. That is about $672 a month at list price; check the price
before approval. It needs 71G of the 100.9G usable, which leaves about 5.9 years
of growth before a resize. It needs no layout, client or wallet change.

**Decision for Roman:** whether to pay about $336 a month more for that host,
or fund about two weeks of layout work to stay on the planned 64 GB host. The
work would use 80,000-record archives before NU6, with a 4,096-row directory
and a new 8,192-row page geometry: 30.5 GiB held, a 45G `MemoryMax`, and about
1.9 years before that host needs a resize. It costs 25 KB more per lookup in
pre-NU6 archives (117.6 KB against 92.5 KB warm, inline), needs a client release
for the new geometry, and doubles the anonymity sets there.

The true-up is required either way. Without it, no option fits 64 GB, and
today's layout needs 127G, more than any host listed. A 128 GB host fits only
the options that change the layout.

## What the census establishes

The orchestrator's first read checks out exactly against the JSON:

- **Inline coverage.** 92.50% of 17,024,724 records are at most 128 bytes,
  94.40% at most 256, and 95.37% at most 1,024. The census found **no unknown
  fees**, so the stored sizes are the exact sizes. The exact-fee scenarios
  equal them, and the "safe row" bounds are loose generic bounds that do not
  apply here.
- **Page rows per 40,000-record archive.** Rows are 4,096 bytes.

  | Era | Archives | p50 | p99 / max |
  |---|---:|---:|---:|
  | Sprout | 86 | 8,034 | 17,375 |
  | Overwinter | 12 | 4,055 | 7,245 |
  | Heartwood | 16 | 3,009 | 3,875 |
  | Canopy | 73 | 2,471 | 4,445 |
  | Sapling | 39 | 2,036 | 3,772 |
  | NU5 | 86 | 1,400 | 5,150 |
  | Blossom | 25 | 1,138 | 1,452 |
  | NU6, NU6.1, NU6.2, NU6.3 | 33, 29, 5, 12 | 176, 197, 176, 116 | 993, 250, 181, 620 |
  | mixed-era (crossing an activation) | 10 | 901 | 7,222 |

- **The census's own layouts**, which use 4,096-row directories at the
  reservation: 4,096-row pages give 79.3 GiB at k=1, 60.0 at k=16 and 59.4 at
  k=64, where k is the number of adjacent archives sharing one page table.
  1,024-row pages give 121–137 GiB, and 256/512-row pages 204–381.
- **Those small page tables cannot be built.** The native scheme's minimum and
  quantum of rows is 2,048 (`POLY_LEN` in `transparent-shard/src/layout.rs`).
  They would also be worse, because every table segment carries a fixed
  32.05 MiB hint at built size (64.05 MiB reserved), whatever its row count.
  The hint, not the rows, is 80% of a `txid-2k` runtime. That is why fewer,
  fuller segments win.
- **Directories fit in one segment per archive.** Directory entries fill 45–54%
  of one 2,048-row directory at 40,000 records in every era, and of one 4,096-row
  directory at 80,000. The census replayed placement only at 4,096 rows, where
  every archive fit one segment; at 2,048 rows this is an assumption from load.

## `txid-2k` and `txid-4k` at real page demand

These figures re-price the census's exact shared allocations. One directory
segment is counted per archive, at 426 archives, which includes the partial
final one (24,724 records). For 2,048-row pages with k>1, the census gives
only 1,024- and 4,096-row roundings, so the result is a range. "Held" is the
built size (`held_bytes`); "reserved" is the bound (`reserved_bytes`).

| Geometry | Pages shared by k archives | Page segments | Held | Reserved |
|---|---:|---:|---:|---:|
| `txid-2k` | 1 (today) | 899 | **51.8 GiB** | 93.2 GiB |
| `txid-2k` | 4 | 685–739 | 43.4–45.6 GiB | 78.2–82.0 GiB |
| `txid-2k` | 16 | 667–681 | 42.7–43.3 GiB | 76.9–77.9 GiB |
| `txid-2k` | 64 | 663–666 | 42.6–42.7 GiB | 76.6–76.8 GiB |
| `txid-4k` | 1 | 588 | 47.6 GiB | 79.3 GiB |
| `txid-4k` | 4 | 391 | 38.3 GiB | 63.9 GiB |
| `txid-4k` | 16 | 342 | 36.0 GiB | 60.0 GiB |
| `txid-4k` | 64 | 334 | 35.7 GiB | 59.4 GiB |

**Sharing pages does not reach the 64 GB host**, which needs at most about
34.7 GiB held. It also costs bandwidth. A client queries every segment of a page
table, because asking for only one would reveal which part of the txid-sorted
table it wants. Each page query therefore returns 5,648 B per segment of the
whole shared table. With 16 Sprout archives sharing a `txid-2k` table (about 63
segments), one page query costs about 396 KB. Sharing pays only where archives
have little page demand, which is NU6 onward. There, groups of 16 need 2–3
segments instead of 16. That saves 2.6 GiB at cutover and cuts growth from 5.0
to 2.9 GiB a year (below).

## Per-era options

All options keep 40,000-record archives after NU6 at `txid-2k`. Growth is 63.7
archives a year: the NU6.3 era's 488,166 records over 80,530 blocks at 75 s.
That is above the earlier sizing's 58.4 a year. Each new archive costs what the
option charges an NU6.3 archive. For most options that is one `txid-2k` pair
(80.1 MiB built, 5.0 GiB a year), since NU6.3 page demand is at most 620 rows.
With shared NU6+ pages it is about 46 MiB (2.9 GiB a year).

`MemoryMax` uses the genesis sizing's rule. The cache holds the archives at
cutover, plus a year of growth and one staged seal, divided by 0.9. On top of
that come eight of the largest runtime for work, and the sum is divided by 0.9
again. Usable is RAM × 0.8 − 1.5 GiB: 24.1G at 32 GB, 49.7G at 64 GB and 100.9G
at 128 GB. "Mean warm" is the average warm lookup if every display record is
equally likely to be looked up.

| Option | Runtimes | Held | `MemoryMax` (built) | Host | Reserved / `MemoryMax` | Disk | Mean warm |
|---|---:|---:|---:|---|---:|---:|---:|
| **A. `txid-2k` (today)** | 1,325 | 51.8 GiB | 71G | 128 GB | 93.2 GiB / 127G, none | 66.8 GB | 99.3 KB |
| `txid-4k` everywhere | 1,014 | 47.6 GiB | 67G | 128 GB | 79.3 / 112G, none | 68.1 GB | 125.2 KB |
| 2k directory, 4k pages | 1,014 | 44.2 GiB | 62G | 128 GB | 75.9 / 107G, none | 60.9 GB | 100.1 KB |
| 2k directory, 8k pages before NU6 | 895 | 44.1 GiB | 62G | 128 GB | 72.1 / 102G, none | 64.7 GB | 102.5 KB |
| 2k directory, page geometry per archive {2k, 4k, 8k} | 895 | 39.4 GiB | 56G | 128 GB | 67.4 / 96G | 54.6 GB | 100.6 KB |
| same, plus NU6+ pages shared in groups of 16 | 828 | 36.7–36.8 GiB | 49.6G | 64 GB, 0.1G spare | 62.7 / 85G | 51.2 GB | 100.6 KB |
| B. 80k before NU6 as `txid-4k` | 756 | 33.5–34.2 GiB | 48.9G | 64 GB, 0.8G spare | 57.9 / 84G | 48.1 GB | 121.2 KB |
| **C. 80k before NU6, 4k directory, 8k pages** | 590 | 29.4–30.5 GiB | 44.4G | 64 GB, 5.3G spare | 48.9 / 73G | 45.6 GB | 123.3 KB |
| 80k before NU6, 4k directory, pages {4k, 8k} | 590 | 28.6–29.6 GiB | 44G | 64 GB | 48.1 / 72G | 43.8 GB | 122.6 KB |

Where held memory is a range, it covers 200 seeded orderings of archives within
an era. The census does not record that order, and it decides which archives
pair or share. Each `MemoryMax` is taken at the top of its range. "Page geometry
per archive" means the seal picks, for each archive, the page table size that
holds its pages in the least memory. The archive's demand is public chain data,
so the choice reveals nothing about any lookup. A 16,384-row option saves a
further 1.2 GiB, but puts the median Sprout 4-page lookup at 651 KB; it is in
`layout.json`.

Years until a host needs a resize, counted from cutover:

- A: none at 64 GB (it does not fit at cutover); 5.9 at 128 GB.
- B: 1.1 at 64 GB; 9.5 at 128 GB.
- C: 1.9 at 64 GB; 10.2 at 128 GB.

### Bytes per lookup

Warm lookups use cached setups. Cold lookups fetch the split map at 426 entries
(4,817 B gzipped), the manifest and every queried segment's setup (20.1 KB
each). Each request adds 400 B of headers, as in `txid-bandwidth formula`.
Every lookup sends two directory queries, then one query per page.

**Option A** (today), for the median archive and the largest archive of each era:

| Era | Page segments, median / largest | Inline, warm / cold | 1 page, warm / cold | 4 pages, warm / cold | 4 pages, largest archive, warm |
|---|---|---:|---:|---:|---:|
| Sprout | 4 / 9 | 92.5 / 120.5 KB | 155.7 / 265.7 | 345.3 / 455.3 | 458.2 |
| Overwinter | 2 / 4 | 92.5 / 120.5 | 144.4 / 213.4 | 300.1 / 369.1 | 345.3 |
| Heartwood, Canopy | 2 / 2–3 | 92.5 / 120.5 | 144.4 / 213.4 | 300.1 / 369.1 | 300.1–322.7 |
| Sapling, Blossom, NU5 | 1 / 1–3 | 92.5 / 120.5 | 138.7 / 187.3 | 277.5 / 326.0 | 277.5–322.7 |
| NU6 onward | 1 / 1 | 92.5 / 120.5 | 138.7 / 187.3 | 277.5 / 326.0 | 277.5 |

**Option C**, before NU6, for one pairing (NU6 onward is as in A):

| Era | Page segments, median / largest | Inline, warm / cold | 1 page, warm / cold | 4 pages, warm / cold | 4 pages, largest archive, warm |
|---|---|---:|---:|---:|---:|
| Sprout | 3 / 4 | 117.6 / 145.6 KB | 212.8 / 302.3 | 498.3 / 587.8 | 520.9 |
| Overwinter | 1 / 2 | 117.6 / 145.6 | 201.5 / 250.0 | 453.1 / 501.6 | 475.7 |
| Sapling to NU5 | 1 / 1–2 | 117.6 / 145.6 | 201.5 / 250.0 | 453.1 / 501.6 | 453.1–475.7 |

Inline lookups are 92.5% of records, and lookups of four or more pages are
0.59%. The inline cost therefore dominates the mean. Under A, 4-page lookups
are already above 300 KB warm in Sprout, Overwinter, Heartwood and Canopy, as
cold 4-page lookups are everywhere today.

These cold figures are about 10 KB above the plan's "110 KB cold inline". That
figure started from the earlier sizing's 104.7 KB base, which omits setup JSON
and headers. The measured cold inline lookup on 2026-10-07 was 132.4 KB, over
HTTPS with the old 8.9 KB full map
([tiered evidence](../txid-display-tiered-2026-10-07/README.md)), which supports
the higher base.

## Anonymity (`joint_routes`)

Today's client looks a txid up in the archive that covers its height, so the
shard id names a chronological 40,000-record archive. That stays true in A and
C. An observer of one lookup at one revision learns:

1. **The archive.** That is about 40,000 transactions, a window of a few days of
   recent blocks or a few weeks of early ones.
2. **Whether the transaction is large.** It is large if the client fetches pages
   after the directory, which happens for 7.5% of records.
3. **How many pages it fetches.**

What the census measured, at the 128-byte cutoff, as candidate txids per class
(the set of real transactions a lookup could be):

- **Archive and inline/paged only.** There are 852 classes. An inline lookup
  hides among 20,723–39,697 transactions; the median is 37,759. A paged lookup
  hides among 303–19,277, with a median of 2,241. 53 classes, all paged
  lookups in archives with few large transactions, have fewer than 1,000
  candidates. They hold 0.19% of records. None has fewer than 1,000 among
  inline lookups.
- **Adding era, page count, and whether the two directory rows coincide.**
  There are 4,301 classes. 868 have fewer than 5 candidates; they hold 1,695
  records (0.010%), and 409 records are unique. 3,509 classes have fewer than
  1,000 (1.85% of records). These are transactions with unusual page counts.
  For example, in an average archive these hold fewer than 5 transactions each:
  - Sprout: 16 or more pages;
  - Overwinter: 8 or more;
  - Sapling, Blossom, Canopy: 5 or more;
  - NU6: 3 or more.
  This is a conservative bound. Today's client always sends exactly two
  directory queries, so the coincidence split the census counts is not visible.
- **Sharing pages changes nothing under per-archive lookup.** The census
  reports identical classes at k=1, 4, 16 and 64. A shared group always contains
  the whole archive, and the archive is already named. A larger page geometry is
  public per shard, and every segment is always queried, so it reveals nothing
  either.
- **Sharing would matter under hashed lookup.** That routing, a proposal not
  built, sends a txid to one global hash bucket. There, a shared-pages group id
  puts back the chronology that hashing removed. With one global bucket, the
  smallest paged class is 1.28M txids; a page fetch from a group of k archives
  narrows it to the group: 1,038 at k=4, 11,654 at k=16, 55,960 at k=64. Shared
  pages and hashed routing therefore do not combine well.
- **Option C doubles every class before NU6**, because each archive holds
  80,000 transactions. Option A keeps today's per-archive sets.

The page-count leak exists in every option. Padding page fetches would close it,
for example by rounding up to 1, 2, 4 or 8 pages, but it costs 46–63 KB per
padded query. The census does not model it. It is listed as follow-up work.

## What each option needs in code

**A (recommended)** has no change to the seal rule, controller, worker, client
or wallet. Multi-segment page tables are already built, served and queried; a
client answers every segment, and the worker's warm-fit check charges each
segment's runtime. The operations work is the plan's item 3, with two
corrections:

- Raise the request's `cache_bytes` cap in `transparent/ops/lib/txid_display_poc.py`,
  which is 64 GiB today. The display cache needs about 63.2 GiB with a year of
  growth. Use 96 GiB, with `MemoryMax` at 71G or more.
- The window check must count the page segments in each archive manifest, not
  two runtimes per archive. That gives 1,325 runtimes at cutover, not 850.
- Point the Terraform droplet at `m-16vcpu-128gb`.

Effort is the planned 1–2 days of ops work plus a test. Cold prepare is about
1,325 runtimes at 2.0–3.3 s each on one CPU (2026-10-05 bench rates): 43–73 min
serially, faster in parallel. That is within the 7,200 s `ready_timeout_seconds`,
but it can lengthen the cutover outage beyond the planned 30–50 min unless the
new host prepares before the route switch.

**C (the 64 GB alternative)** needs about 1.5–2.5 weeks:

- **Seal rule** (`display/seal.rs`): an `archive_target` and geometry per height
  range, here 80,000 below 2,726,400, in the parameters and the root.
  `plan_seals`, `check_entry`'s `min_bucket_records >= archive_target` and
  verify must apply the range's target.
- **Geometry**: a new `txid-4k-8k` (4,096 directory rows, 8,192 page rows) in
  `DISPLAY_PROFILES`. That needs setup seeds, `SharedParams`, the bandwidth
  formula and a native certificate check at 8,192 rows. Rows up to 32,768
  already pass at 128 bits.
- **Controller and bootstrap**: build each range with its geometry, which today
  is one per root.
- **Worker**: none beyond the new profile.
- **Client**: a release that knows the new name. Today's client returns
  `Unsupported` for a geometry it does not know, so wallets on older builds
  would lose pre-NU6 lookups.
- **Tests**: seal-boundary and rebuild-digest tests across the range change,
  and a requalification run.

B uses the existing `txid-4k`, so it skips the geometry and client work (about
1 week). But it leaves only 0.8G to spare after one year at 64 GB.

Per-archive page geometry without 80,000-record archives (39.4 GiB) needs
similar seal and controller work and does not fit 64 GB. Adding shared NU6+
pages fits with only 0.1G to spare, and it needs page tables that span
archives. A live seal cannot finish such a table until its group of 16 closes,
about three months, so sealed manifests would change or pages would wait. It
is not worth that for 64 GB.

## Limits

- Derived from the census's per-archive page-row demand, which replays the v1
  packer on 40,000-record chunks cut at exact record counts. The seal rule cuts
  at block boundaries, so real archives hold slightly more than 40,000 records,
  and there are about 425 sealed archives plus the recent shard.
- Directories at 2,048 rows are assumed to need one segment from their 45–54%
  load. The 4,096-row replay confirms this at 4,096 rows only.
- Page counts per lookup use each era's record-size mix applied to every archive
  of that era.
- The 80,000-record and shared options pair or group archives in seeded random
  orders within an era. Their page demand is the sum of the halves, which is
  within about one row of a real 80,000-record packing per half.
- Built size is the planning figure the true-up charges. It was measured on
  synthetic rows at 2,048 and 4,096 rows; 8,192- and 16,384-row runtimes are
  unmeasured. Real-table runtime sizes remain P0.
- Prices are DigitalOcean list prices carried from the genesis sizing; verify
  them before approval. No anonymity guarantee is claimed: the K thresholds
  are the census's engineering controls.

## Files

- `census.json.gz`: the census JSON, gzip -9 -n. The uncompressed JSON's SHA-256 is
  `ce0f3fff30d963b99cd6431634a66f50e781c63c4675e4c8c0dc7fef9b29c86b` (as in the receipt).
- `receipt.json`: written by the run. Its `compiler` field is wrong: it records the
  host's default `rustc` (1.91.0); the binary (`f1ce7749…`) was built with the
  repository-pinned 1.97.1 (build log, unit `txid-census-build-452f50d4`).
- `layout.py`: the analysis. Python 3 standard library, reads only `census.json.gz`.
- `layout.json`: its full output, including every option, per-era lookup bytes,
  page-count mix and constants.
- `layout.txt`: its printed summary.
- `manifest.json`: run metadata. `SHA256SUMS` covers the raw files.

## Census provenance

- Ingest history: `transparent-event-ingest` (release `d191f86b`) with `--txid-display`
  from height 0, stop 3,508,673. Heights 0–~414k ran on `/srv/zakura` with 4 workers;
  it was stopped when `/srv/zakura` crossed its 20% headroom floor and the journal was
  moved to a dedicated volume (`/srv/txid-display-genesis`, Terraform `983cbedb`).
  The rest ran under `eatmydata` with 6 workers; one stop at 1,866,000 (the RocksDB
  secondary lost an SST file to node compaction) resumed from its checkpoint. Complete at
  22:56:45 UTC: 3,508,674 blocks, 354,009,971 events, 17,024,724 display records.
- Census run: 2026-10-07 22:57:23–23:36:46 UTC on the coordinator, anchor
  3,508,673 (`0000000000477ed8…d13c`, taken from the journal, not independently
  attested). Shielded-only exclusions and total chain transactions are not in
  the sidecars (null in the JSON).
- Memory figures in the JSON are the source reservation formula, not native RSS, and
  predate the reservation true-up (`3d727bfc`). The analysis re-prices them.
- Analysis: `python3 layout.py` (Python 3.12.3) on roman-dev-2 (DigitalOcean,
  8 vCPU, 31 GiB, shared development host), wallet-pir `1ceb5dbe`,
  2026-10-08 00:01 UTC; 7 s. Runtime and lookup formulas follow
  `SharedParams::held_bytes`/`reserved_bytes`
  (`transparent-shard-server/src/runtime.rs`), `shared/pir-native` lengths and
  `txid-bandwidth formula`. Host rules follow
  [the genesis sizing](../txid-display-backfill-sizing-2026-10-07/README.md).
