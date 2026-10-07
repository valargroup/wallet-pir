# Txid display backfill sizing, 2026-10-07

Planning evidence for extending the tiered txid display below height 3,407,001.
The plan and change sheet are in
[deployment](../../docs/deployment.md#txid-display-backfill-below-3407001-proposed).
Nothing was deployed and no production host was changed or logged into. No
`wallet-pir-deploy` inventory was available, so no SSH session was opened.

## What ran

| Step | File | What it does |
|---|---|---|
| Capture | [capture.py](capture.py) → [inputs/](inputs) | At 16:05 UTC, plain GETs of the public routes any wallet uses on `transparent-pir.valargroup.dev`: the history map `/v1/shards`, the display map `/v1/txid/shards`, and all 14 display manifests. Status, size and sha256 of each are in [inputs/capture.json](inputs/capture.json). Both maps end at 3,509,639. |
| Sizing | [sizing.py](sizing.py) → [sizing.json](sizing.json) | Offline arithmetic over those inputs plus maps already retained in this repository. No network access. `python3 -B sizing.py` reproduces `sizing.json` byte for byte. |

## Source of the counts

The display capability publishes one record per transaction with any
transparent input or output, coinbase included
([contract](../../docs/txid-display.md#published-facts)).

- **Exact at and above 3,407,001.** The live display map records each shard's
  `records`. That gives 567,880 records over 3,407,001–3,509,639: 13 sealed
  archives (40,001–40,013 each, mean 40,004.6) and a recent shard of 47,820.
- **Below 3,407,001 there is no display data.** The production display journal
  starts at 3,407,000. Display sidecars exist only for blocks ingested in
  display mode, so the full v3 history journal has none.
- **Proxy for display records: history `txids`.** Each history shard records the
  distinct txids of its journal events (`transparent-shard/src/seal.rs`). Events
  are emitted for every nonempty, non-OP_RETURN transparent output and every
  non-coinbase transparent input (`transparent-filter-server/src/extract.rs`).
  The display set can only be larger, by transactions whose sole transparent
  part is a coinbase input or an OP_RETURN or empty output.
- **Calibration.** Over 3,407,001–3,509,639 the live history map gives 570,034
  txids (bracket 460,492–595,359). The display map gives exactly 567,880.
  The ratio is 0.996, equal to 1 within the interpolation error, so the proxy is
  used unscaled.

Four published history maps bracket each range. Each map was built from a
production journal:

| Map | File | Journal | Through |
|---|---|---|---|
| Live v11 | `inputs/history-shards.json` (91 shards) | v3, genesis start | 3,509,639 |
| v9 served | [../v9-cutover-2026-09-28/served-map.json](../v9-cutover-2026-09-28/served-map.json) (139 shards) | v2 | 3,499,341 |
| v10 pre-cutover | [../v10-cutover-2026-09-28/load/before-map.json](../v10-cutover-2026-09-28/load/before-map.json) (86 shards) | v2 | 3,499,628 |
| v3 full publication | [../activity-metadata-2026-09-30/full-publication-terminal.json](../activity-metadata-2026-09-30/full-publication-terminal.json) (90 shards) | v3 | 3,500,738 |

The v3 publication and the live map share boundaries, so there are three
independent boundary sets. For each range and map:

- **Lower bound:** the shards wholly inside the range.
- **Upper bound:** every shard the range touches.
- **Estimate:** a straddling shard's txids are split in proportion to the
  [2026-09-08 census](../census-2026-09-08/census.txt) event density
  (4,096-block buckets; uniform blocks past 3,473,686).

The tightest bounds across maps are kept. The estimate is the median of the
per-map estimates, clipped to those bounds. Per-map estimates agree within 4%
for every range below.

## Counts per height range

| Heights | Blocks | Txids with a transparent input or output | Bracket | Per block |
|---|---:|---:|---|---:|
| 419,200–1,687,103 (Sapling to NU5) | 1,267,904 | 6,275,405 | 6,137,307–6,406,246 | 4.95 |
| 1,687,104–1,999,999 | 312,896 | 1,118,739 | 710,476–1,274,667 | 3.58 |
| 2,000,000–2,499,999 | 500,000 | 1,637,906 | 1,624,725–2,164,213 | 3.28 |
| 2,500,000–2,999,999 | 500,000 | 1,243,646 | 756,843–1,243,646 | 2.49 |
| 3,000,000–3,407,000 | 407,001 | 2,197,139 | 2,159,813–2,426,453 | 5.40 |
| 3,407,001–3,509,639 (served now) | 102,639 | 567,880 display records, exact | — | 5.53 |

Cumulative backfill from each candidate to 3,407,000, which brackets tighter
than the bands:

| Start | Backfill txids | Bracket | Display records at cutover |
|---|---:|---|---:|
| Sapling 419,200 | 12,463,419 | 12,350,188–12,594,040 | ≈13.03M |
| NU5 1,687,104 | 6,215,773 | 6,091,154–6,324,622 | ≈6.78M |
| 2,000,000 | 5,086,335 | 5,028,184–5,453,783 | ≈5.65M |
| 2,500,000 | 3,451,779 | 3,200,080–3,503,519 | ≈4.02M |
| 3,000,000 | 2,197,139 | 2,159,813–2,426,453 | ≈2.77M |

## Sizing per candidate start

The candidates assume today's parameters (`txid-2k`, N=1, `archive_target`
40,000, `recent_floor` 10,000) and a fresh lineage from the start to 3,509,639.

| Start | Sealed archives | Archive runtimes | Reserved memory | Lower bound | Archive-03 disk | New journal | Ingest | Publish / prepare |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| Sapling | 325 (321–329) | 650 | 45.7 GiB | 27.3 GB | 54.6 GB | 20.6 GB events + 3.09M sidecars | ≈5.1 h | 4.9 min / 21–36 min |
| NU5 | 169 (165–173) | 338 | 23.8 GiB | 14.2 GB | 28.4 GB | 8.2 GB + 1.82M | ≈3.0 h | 2.5 min / 11–19 min |
| 2,000,000 | 141 (138–148) | 282 | 19.8 GiB | 11.8 GB | 23.7 GB | 5.7 GB + 1.51M | ≈2.5 h | 2.1 min / 9–16 min |
| 2,500,000 | 100 (92–102) | 200 | 14.1 GiB | 8.4 GB | 16.8 GB | 3.2 GB + 1.01M | ≈1.7 h | 1.5 min / 7–11 min |
| 3,000,000 | 68 (66–72) | 136 | 9.6 GiB | 5.7 GB | 11.4 GB | 1.5 GB + 0.51M | ≈0.9 h | 1.0 min / 4–8 min |

How each column is derived:

- **Archives.** (records − ≈25k average recent shard) ÷ 40,004.6. The range
  comes from the bracket.
- **Runtimes and memory.** Two runtimes per archive: one directory segment and
  one page segment. All 13 live archives have one of each. Reserved memory is
  `SharedParams::reserved_bytes` at 75,545,144 B per runtime: the 8 MiB table,
  the masks, and the compiled matrix at 8-byte words (64 MiB). That reservation
  is what the worker's cache admits against. The lower bound charges the matrix
  at 4-byte words (41,990,712 B per runtime). Resident memory on archive-03 was
  not measured.
- **Archive-03 disk.** Per archive, two 8 MiB tables plus two disk-cache entries
  charged at the reservation.
- **New journal.** Event bytes use the v3 journal's 120.6 B per event (42.68 GB
  over 353.8M events through 3,500,738). Display mode adds one sidecar file per
  block, fsynced with its directories. That is at least 4 KiB of allocation per
  block (2.1 GB at 3,000,000; 7.5 GB at NU5) plus about 61.6 B of payload per
  record, measured from the live manifests.
- **Ingest.** Uses the full v3 history ingest rate: 167 blocks/s with 4 workers
  from 324,000 to 3,500,738
  ([handoff](../activity-metadata-2026-09-30/ingest-handoff-cache-a1c4b809.json),
  [completion](../activity-metadata-2026-09-30/full-ingestion-publication-start.json)).
  That rate includes no sidecar fsyncs, so treat it as a lower bound.
- **Publish and prepare.** Publish is ≤0.9 s per shard; worker prepare is
  3.9–6.6 s per two-table runtime pair at 2 or 1 CPU. Both come from the
  [synthetic 2026-10-05 bench](../txid-display-tiered-2026-10-05/README.md#benches-and-choosing-the-seal-parameters).

**Per-lookup bandwidth.**
- Query bytes are unchanged. Geometry, bucket count and the transcript stay the
  same, so a warm lookup is still 92.6 KB inline and 277.8 KB at 4 pages.
- The map grows. It holds one 632 B entry per listed shard (8,854 B for 14
  today). The router serves it uncompressed: no `Content-Encoding` even when
  gzip is offered. Gzip would shrink it to 1,865 B, 4.7×.
- A client fetches the map cold and again after a 409 from a moved recent
  revision. A cold inline lookup becomes about 148 KB at 3,000,000, 212 KB at
  NU5 and 311 KB at Sapling, against the 300 KB criterion. Cold 4-page lookups
  were already 325 KB.

## Window, memory budget and the floor it implies

`max_archive_shards` bounds the archive owner's memory. A seal beyond it drops
the oldest archive for good, so the window, not the start height, decides
coverage in the long run.

The cache holds the window plus one staged seal, with a 10% margin. That rule
reproduces today's 4 GiB cache and 24-archive window. Work memory is admitted
on top of the cache against 0.9 × MemoryMax: one build at 4× a table and two
queries at 2× each.

"Floor today" is the lowest height whose range to the tip fills the window at
the estimated counts.

| Display cache | MemoryMax | Window | Floor today | Coverage at the current 6.25 days per archive |
|---:|---:|---:|---:|---:|
| 4 GiB (live) | 6G | 24 | 3,322,430 | ≈150 days |
| 8 GiB | 10G | 50 | 3,137,368 | ≈312 days |
| 12 GiB | 14G | 75 | 2,818,889 | ≈468 days |
| 16 GiB | 19G | 101 | 2,465,864 | ≈631 days |
| 20 GiB | 23G | 126 | 2,132,693 | ≈787 days |

Archive-03 is an `m-8vcpu-64gb` host that it shares with the history archive
owner. That owner has a 48 GiB cache, 41.35 GiB of it reserved, with
`MemoryMax` 56G. On 2026-09-29, before display existed, it ran at 34.1 GiB RSS
with 52% of host memory available
([deployment](../../docs/deployment.md#sizing-and-availability)). That is the
~32 GB in the brief, about 33 GiB.

- The fleet's 20% available-memory floor (≈12.8 GiB) leaves about 20 GiB.
- History may still grow by up to 6.6 GiB into its own cache, which leaves about
  14 GiB for display `MemoryMax`.
- **12 GiB / 14G** is therefore the largest budget that keeps the floor in that
  worst case. 3,000,000 is the lowest candidate it covers.
- **16 GiB / 19G** keeps the floor only while history stays at its current size.
- **NU5 and Sapling** do not fit at all.

## Genesis addendum (Roman's decision, 2026-10-07)

Roman chose a genesis floor after the first pass. The sections above are kept
as that first pass; their 3,000,000 recommendation is now the no-spend fallback
in the [plan](../../docs/deployment.md#archive-03-budget).
[genesis.py](genesis.py) reuses `sizing.py`'s inputs and functions and writes
[genesis.json](genesis.json); `sizing.json` is unchanged.

**Counts**, with the display start at 1 because height 0 is the journal parent:

| Range | History txids | Bracket |
|---|---:|---|
| 1–419,199 (before Sapling) | 3,995,879 | 3,836,468–4,029,196 |
| 1–653,599 (before Blossom) | 5,600,755 | — |
| 1–3,407,000 (whole backfill) | 16,460,524 | 16,330,448–16,491,125 |
| 3,407,001–3,509,639 | 567,880 display records, exact | — |
| Total display records | ≈17,028,404 | — |

**Checks against the orchestrator's first pass:**

- **Confirmed:** about 4.0M before Sapling; 17.0M in total; 425 archives (it
  said 423) and 850 runtimes; 59.8 GiB reserved; 33.2 GiB at four-byte words;
  71 GB of archive disk; at least 5.8 h of ingest.
- **Corrected:** the new journal holds about **42.7 GB** of events, not 27 GB.
  It covers the whole chain at the v3 journal's 120.6 B per event. On top of
  that come at least 14.4 GB of sidecar allocation.
- **Refined:** the map is 259 KB at 426 entries in the live entry shape, with
  synthetic hashes, against the orchestrator's 268 KB.

### Measured runtime size

[residency/](residency) holds the full `shard-residency` output: six runtimes
per table, held at once, release-fast on this host. The source was `8cccfa44`
plus a two-line extension of the tool to display geometries and tables:
[residency/shard-residency-display.patch](residency/shard-residency-display.patch).
Apply it with `git apply` to reproduce; it is not on `main`. The later batched display hint on `main` (`07f90675`) changes build
time, not prepared size.

| Geometry | Reservation | Four-byte size | Steady increments (runtimes 3–6) |
|---|---:|---:|---|
| `txid-2k` directory | 72.05 MiB | 40.05 MiB | 48.5, 40.3, 40.3, 41.3 MiB |
| `txid-2k` pages | 72.05 MiB | 40.05 MiB | 54.2, 46.2, 47.4, 34.8 MiB (allocator noise; mean 45.6) |
| `txid-4k` directory | 80.05 MiB | 48.05 MiB | 51.3, 54.7, 48.3, 48.3 MiB |
| `txid-4k` pages | 80.05 MiB | 48.05 MiB | 48.3, 48.2, 48.2, 48.3 MiB |

The tool's printed slope includes the noisy second build and so reads higher.
The sizing uses the four-byte size. Rows were synthetic: the tool states that
size is a function of the geometry and bytes. Real-table confirmation is P0,
which reads the length of the display disk-cache entries on archive-03.

### Variants, hosts and map designs

All variants are in `genesis.json`. Usable host memory is the host's memory
minus the 20% floor and 1.5 GiB for the system. Archive-03 allows display at
most 14G.

| Variant | Accounting | Archives | Held | Cache, one year | `MemoryMax` | Disk | Fits |
|---|---|---:|---:|---:|---:|---:|---|
| `txid-2k` everywhere | reservation | 425 | 59.8 GiB | 75.7 GiB | 85G | 71.3 GB | `m-16vcpu-128gb` |
| `txid-2k` everywhere | four-byte | 425 | 33.2 GiB | 42.1 GiB | 48G | 42.8 GB | `m-8vcpu-64gb` and up |
| `txid-4k` at 80k below 3,407,001 | reservation | 206 + 14 | 34.2 GiB | 47.3 GiB | 54G | 43.8 GB | `m-16vcpu-128gb` |
| `txid-4k` at 80k below 3,407,001 | four-byte | 206 + 14 | 20.4 GiB | 27.9 GiB | 32G | 29.1 GB | `m-8vcpu-64gb` and up |
| `txid-4k` at 80k below Blossom | reservation | 70 + 285 | 51.0 GiB | 66.0 GiB | 74G | 61.9 GB | `m-16vcpu-128gb` |
| `txid-4k` at 80k below Blossom | four-byte | 70 + 285 | 28.9 GiB | 37.2 GiB | 42G | 38.1 GB | `m-8vcpu-64gb` and up |

Old-archive lookups cost 92.5 KB inline and 277.5 KB at 4 pages for `txid-2k`,
and 117.6 KB and 352.8 KB for `txid-4k`. These are computed as queries ×
(upload + 5,648 B + 400 B of headers).

Map designs at 426 entries:

- **One map:** 258,988 B raw, 59,757 B gzipped.
- **Split:** a 32-entry chunk is 19,681 B raw (4,896 gzipped); a recent map with
  an index pointer is 1,170 B (528 gzipped).
- **Cold lookups** (the formula's cold inline less its 15 KB map, plus the map):
  - uncompressed: 364 KB inline, 569 KB at 4 pages;
  - gzip: 164 KB and 370 KB;
  - split: 110 KB and 316 KB.

**Ingest from genesis:**

- 3,509,640 blocks and about 42.7 GB of events.
- 3,509,640 sidecar files in one directory: at least 14.4 GB allocated, about
  1.05 GB of payload, and 10.5M fsyncs.
- A publication root of about 7.1 GB.
- At least 5.8 h at the history ingest rate.

## Not measured, and why

No deploy inventory was available, so these could not be read:

- archive-03's current MemAvailable, the display worker's RSS per runtime, and
  free disk on `/srv`;
- the coordinator's free disk and inodes;
- the node state cache;
- ingest throughput with display sidecars.

Every count below 3,407,001 is a history-txid estimate, not a display build.
Old-era page usage is unmeasured. The 13 live archives use at most 776 of 2,048
page rows (archive 11) and typically 74–180, so a second page segment is
unlikely at 40,000 records. Rebuild, latency and anonymity criteria are not
re-measured here.

## Provenance

- Source: `8cccfa44` (wallet-pir `main`). Python 3.12 standard library only;
  residency runs as described in the genesis addendum.
- Host: roman-dev-2 (DigitalOcean premium Intel, 8 vCPU, 31 GiB), shared
  development host.
- Machine-readable metadata: [manifest.json](manifest.json); checksums:
  [SHA256SUMS](SHA256SUMS).
