# Decoupled directory/page shards: step-1 simulation, 2026-10-12

Draft-branch prototype of the [decoupled pages design](../../docs/decoupled-pages-design.md).
This is step 1, the census, which ends at the design's gate. No codec,
publisher, server or wallet code has been built for it, and nothing is
deployed. All bills are **derived arithmetic** (`pairs.py` over census
output), not wallet measurements.

## Gate result

The gate asks for a decoupled option that beats both today's one-shot bill and
bulk isolation without a worse tip catch-up bill.

- **Archive tier: it passes.** Two options qualify, with the recent tier left as
  today: decoupled 32k directory with 64k pages, and with 32k pages. The tip
  catch-up bill is then unchanged, because catch-up only touches the recent tier.
  - restore-old one-shot, mean MB per wallet: 7.9 (32k/64k) and 7.8 (32k/32k),
    against 16.1 today and 10.6 under bulk isolation.
  - restore-old p99: 25.4 and 23.9 MB, against 37.7 today and 90.4 under bulk.
  - The cost is round trips. p99 is 12 (32k/64k) or 22 (32k/32k), against 2.
- **Recent tier: it fails.** One-shot bills fall slightly, by 3–6%, but tip
  catch-up rises by about 50%.
  - Pooled over 8 anchors, mean catch-up MB for 1 / 7 / 30 days is
    0.29 / 0.29 / 0.43 with decoupled 4k/8k. Today's `recent-4k-8k` costs
    0.19 / 0.20 / 0.35.
- **The win is filters, not pages.**
  - In the archive window, ≈8.0 of the ≈8.2 MB saved per restore-old wallet is
    filter bytes. Two large directory shards repeat far fewer scripts across
    their filters than 26 page-bound shards do: 6.73 vs 14.76 MB.
  - Private query bytes barely move: 0.94–1.11 vs 1.21 MB.
  - In the recent tier the same effect works against catch-up. The open
    directory shard is larger, so its filter costs more to download again:
    0.245 vs 0.162 MB of the mean catch-up bill. Query bytes are 0.072 vs 0.070 MB.

Roman decides whether to pursue the decoupled layout (steps 2–6).

## Method

| Field | Value |
|---|---|
| Input | Two read-only windows of `/srv/transparent-activity/full-v3/journal` (v3 journal, schema v11 events, genesis `00040fe8…dce08`, source covered to 3,514,563 at checkpoint `[42721185853, 168699072]`). **recent** 3,262,749–3,473,686: 210,938 blocks, 4,185,700 events, anchor `0000000000755137…0be1d`, the frozen sample's cutoff to anchor. **archive** 200,000–419,999: 220,000 blocks, 112,314,589 events, anchor `00000000001f9bca…a9777`, which contains the chain's densest stretch. The windows carry no display sidecars, because the source journal has no sidecar directories. Provenance is in [raw/journal-provenance.json](raw/journal-provenance.json). |
| Transfer | [fetch-slices.sh](fetch-slices.sh) at `9d240446`, run by Roman from his Mac. It streamed read-only from the coordinator through the Mac, with `nice`/`ionice` and nothing written on the coordinator. [slice-hub.py](slice-hub.py) then rebased the offsets and wrote checkpoints and checksums on the hub. **Deviation:** the relay ran at about 1.2 MB/s, so Roman copied the archive window's `events.bin` tail directly from the coordinator to the hub in 8 parallel byte ranges instead. Both windows' `events.bin` ranges were hashed on the coordinator and match the hub byte for byte. The source block records were re-read and were unchanged, and `SHA256SUMS` passes. |
| Workload | The [frozen 1,080-wallet sample](../workload-sample-2026-09-08/README.md). Wallet ranges are clipped to each window; clipped classes are labelled "(clipped)". In the recent window the catch-up, restore-6m, multi-script and most small-active wallets are unclipped. In the archive window only reused-tail wallets that start inside it are unclipped. restore-old and unused are clipped to the window. |
| Tool | `shard-census`, built with `release-fast` (no LTO) from `9d240446`, binary sha256 `0c85e8a9…0715`, Rust 1.97.1. Runs: [run-census.sh](run-census.sh) for 11 layouts × 2 windows, and [catch-up-anchors.sh](catch-up-anchors.sh) for 4 recent-tier layouts × 8 anchors. Coupled layouts pass the publisher's derived policy explicitly: capacity minus 1/7 scripts, minus 1/32 page rows, at 58 minimal entries per 4,096-byte row. Directory bytes seal at 6/7 of rows × 4,092. |
| Pricing | [pairs.py](pairs.py), whose model is in its docstring: filters at ~2.5 B per script of every overlapping shard, choice tables at ~1.245 bits per script, one setup per (shard, table, segment), and the census `prices` row per query. Tables come from [bill-table.py](bill-table.py) and [catch-up-summary.py](catch-up-summary.py), shard tables from [summarize.py](summarize.py). |
| Host | Development hub, DigitalOcean `DO-Premium-Intel`, 8 vCPU (this task's quota is about 3 CPUs), 31 GiB RAM, Ubuntu 24.04.3, 2026-10-12 00:29–01:14 UTC |
| Cost | Recent window: 1–3.5 min per run, peak RSS ≤ 0.4 GB. Archive window: coupled runs 16–37 min, decoupled runs 18–42 min, peak RSS ≤ 1.03 GB. |
| Raw | `raw/<run>/*.txt.gz` (full census logs), `*.time.gz` (`/usr/bin/time -v`), `jobs.txt.gz`, `sample-<anchor>.json.gz` (shifted catch-up samples); checksums in [SHA256SUMS](SHA256SUMS) |

Derived outputs: [results-shards-recent.tsv](results-shards-recent.tsv),
[results-shards-archive.tsv](results-shards-archive.tsv),
[results-bills-recent.md](results-bills-recent.md),
[results-bills-archive.md](results-bills-archive.md),
[results-catch-up.md](results-catch-up.md) and
[results-pairs.txt](results-pairs.txt), which holds every class with means and
query counts.

## v11 baseline: seal reasons and directory fill

Today's geometries on v11 data. Fill is per sealed shard, p50 / max.

| Window, layout | Sealed shards | Sealed by | Script fill | Directory byte fill | Page-row fill |
|---|---:|---|---|---|---|
| recent, `recent-4k-8k` | 7 | page rows 7 | 26.2% / 29.2% | 45.6% / 50.4% | 96.9% / 97.2% |
| archive, `archive-wide` | 26 | page rows 26 | 11.4% / 15.8% | 19.8% / 27.2% | 96.9% / 97.2% |
| archive, `recent-4k-8k` | 181 | page rows 164, directory bytes 17 | 40.5% / 53.5% | 68.4% / 86.0% | 97.0% / 99.6% |

Pages still close almost every shard on v11. Archive directory byte fill is below
30% as the design estimated. Recent directory byte fill is 46%, higher than the
estimate.

## Shards per layout

| Window, layout | Directory shards (sealed + tail) | Page shards | Directory sealed by | Directory byte fill p50 / max | Runs per paged pair p50 / p95 / p99 / max | Pinned plaintext |
|---|---|---:|---|---|---|---:|
| archive, `archive-wide` (today) | 26 + 1 | — | page rows 26 | 19.8% / 27.2% | — | 10.9 GB |
| archive, coupled 16k/64k | 26 + 1 | — | page rows 26 | 39.7% / 54.3% | — | 9.1 GB |
| archive, coupled 8k/64k | 29 + 1 | — | page rows 17, directory bytes 12 | 81.4% / 85.9% | — | 9.1 GB |
| archive, bulk `archive-wide` | 2 + 1 | — | directory bytes 2 | 85.7% / 85.7% | — | 1.2 GB + bulk table |
| archive, decoupled 32k/64k | 2 + 1 | 29 (≤ 11 per directory) | directory bytes 2 | 85.7% / 85.7% | 3 / 10 / 11 / 11 | 8.19 GB |
| archive, decoupled 32k/32k | 2 + 1 | 57 (≤ 21) | directory bytes 2 | 85.7% / 85.7% | 5 / 20 / 21 / 21 | 8.05 GB |
| archive, decoupled 32k/16k | 3 + 1 | 116 (32 at p50) | **child limit 3** | 72.5% / 84.6% | 7 / 32 / 32 / 32 | 8.32 GB |
| recent, `recent-4k-8k` (today) | 7 + 1 | — | page rows 7 | 45.6% / 50.4% | — | 0.40 GB |
| recent, bulk `recent-4k-8k` | 3 + 1 | — | directory bytes 3 | 85.7% / 85.7% | — | 0.2 GB + bulk table |
| recent, decoupled 4k/8k | 3 + 1 | 9 | directory bytes 3 | 85.7% / 85.7% | 1 / 3 / 3 / 3 | 0.37 GB |
| recent, decoupled 8k/8k | 1 + 1 | 8 | directory bytes 1 | 85.7% | 1 / 3 / 5 / 5 | 0.34 GB |

- **Child limit versus a full directory.** Only 32k/16k reaches
  `MAX_PAGE_CHILDREN = 32`; every other decoupled layout seals on directory
  bytes. Over the archive window, decoupled 4k/8k makes 113 directory and 256
  page shards, with runs p99 4. Decoupled 8k/8k makes 24 and 238, with runs p99 15.
- **Page fill.** Sealed page shards sit at 96.9–97.0% of rows (p50), the 1/32
  target. Row bytes are 99.4–99.5% used. The 40-byte header's chain fields cost
  0.40% (32k/64k), 0.59% (32k/32k) and 0.90% (32k/16k) of archive page payload,
  and 0.33–0.43% in the recent window. That is not material, so the
  start-fragment-only header option is not needed.
- **Round trips.** A paged (script, directory) pair walks runs + 1 round trips,
  plus one more if its oldest walked run has more than one fragment.
- **Pinned plaintext** is per window, at the census's segment counts. The bulk
  rows exclude the bulk table itself.

## One-shot bills

Total MB per wallet: mean; p50 / p95 / p99. The mean comes from
[results-pairs.txt](results-pairs.txt) and covers all of the class's wallets in
range; the percentiles come from [results-bills-*.md](results-bills-archive.md).

Archive window. restore-old (120 wallets, clipped to the window) and
reused-tail (22 wallets that start at or before 419,999):

| Layout | restore-old | restore-old round trips p50 / p99 | reused-tail mean | Filter / query MB (restore-old mean) |
|---|---|---|---:|---|
| `archive-wide` (today) | 16.09; 14.76 / 22.21 / 37.70 | 0 / 2 | 61.2 | 14.76 / 1.21 |
| coupled 16k/64k | 15.88; 14.76 / 21.04 / 34.81 | 0 / 2 | 60.1 | 14.76 / 1.01 |
| coupled 8k/64k | 16.78; 15.69 / 21.49 / 36.36 | 0 / 2 | 60.3 | 15.69 / 0.97 |
| bulk `archive-wide` | 10.57; 6.73 / 22.43 / 90.37 | 0 / 2 | 449.8 | 6.73 / 3.73 |
| decoupled 32k/64k | 7.93; 6.73 / 15.19 / 25.41 | 0 / 12 | 55.6 | 6.73 / 1.11 |
| decoupled 32k/32k | 7.78; 6.73 / 12.62 / 23.92 | 0 / 22 | 34.6 | 6.73 / 0.94 |
| decoupled 32k/16k | 8.21; 7.21 / 12.20 / 25.91 | 0 / 33 | 24.8 | 7.21 / 0.85 |

Recent window:

| Layout | restore-6m | multi-script | multi-script round trips p50 / p99 | reused-tail mean (120) |
|---|---|---|---|---:|
| `recent-4k-8k` (today) | 1.66; 1.61 / 2.08 / 2.38 | 5.87; 5.09 / 6.67 / 11.85 | 2 / 2 | 17.27 |
| bulk `recent-4k-8k` | 1.69; 1.67 / 2.22 / 2.54 | 7.94; 5.92 / 8.33 / 17.49 | 2 / 2 | 39.61 |
| decoupled 4k/8k | 1.56; 1.52 / 1.94 / 2.14 | 5.64; 4.83 / 6.42 / 11.38 | 3 / 5 | 17.12 |
| decoupled 8k/8k | 1.60; 1.57 / 1.93 / 2.18 | 6.37; 5.56 / 6.93 / 11.78 | 4 / 7 | 17.03 |

Bulk isolation prices each bulk fragment query against every page segment of
the shard, as the [bulk census](../bulk-isolation-census-2026-09-28/README.md)
noted. That is why its long-history bills grow so large; a separately sized
bulk table would cost less.

## Tip catch-up bill

The catch-up wallets were synced through `required_from − 1`. The decoupled walk
stops at the first run in a page shard that ended before then (the run cache).
Coupled catch-up re-queries every shard overlapping the range. One anchor is
noisy, because the bill depends on how far the open shard has grown. So
[catch-up-anchors.sh](catch-up-anchors.sh) repeats the recent-tier layouts at 8
anchors, 3,428,186 to 3,473,686 in steps of 6,500, shifting the catch-up
wallets' ranges by the same distance. Cells give the mean pooled over anchors;
p50 / p95 / p99; the range of per-anchor means.

| Layout | catch-up-1d | catch-up-7d | catch-up-30d | Round trips p99 |
|---|---|---|---|---:|
| `recent-4k-8k` (today) | 0.193; 0.17 / 0.36 / 0.54; 0.087–0.254 | 0.204; 0.16 / 0.48 / 0.61; 0.079–0.438 | 0.349; 0.30 / 0.65 / 0.94; 0.183–0.603 | 2 |
| bulk `recent-4k-8k` | 0.344; 0.28 / 0.72 / 1.65; 0.087–0.511 | 0.314; 0.29 / 0.71 / 1.00; 0.122–0.458 | 0.450; 0.38 / 0.91 / 1.39; 0.275–0.740 | 2 |
| decoupled 4k/8k | 0.291; 0.28 / 0.49 / 0.67; 0.087–0.407 | 0.289; 0.29 / 0.50 / 0.67; 0.122–0.383 | 0.430; 0.38 / 0.76 / 0.98; 0.269–0.708 | 3 |
| decoupled 8k/8k | 0.425; 0.41 / 0.71 / 0.94; 0.307–0.707 | 0.370; 0.33 / 0.71 / 0.79; 0.251–0.673 | 0.399; 0.35 / 0.71 / 0.88; 0.239–0.685 | 3 |

Mean over all catch-up wallets and anchors:

| Layout | Filters | Private queries | Setup | Choice tables |
|---|---:|---:|---:|---:|
| today | 0.162 | 0.070 | 0.013 | 0.004 |
| decoupled 4k/8k | 0.245 | 0.072 | 0.013 | 0.007 |

The run cache works: catch-up page queries stay near today's (0.42 vs 0.39 per
wallet). The extra cost comes from the open directory shard's larger filter.
Parent or incremental filters would remove it, but the design excludes them.

## Privacy delta

Derived from the shard tables above, not measured on a wallet.

- **Archive.** Decoupled 32k/64k spreads the window's history over 2 directory
  shards, where today uses 26. A wallet without pages reveals only which of 2
  directory shards it queries, not which of 26.
- **Paged wallets.** Each queried page shard and each hop reveals roughly when
  events were pushed out, at page-shard granularity (29 page shards over the
  window, close to today's 26 shards). Chain length is visible as the number of
  sequential round trips, up to 11 runs at 32k/64k and 21 at 32k/32k.
- **Recent.** Directory shards span twice as many blocks as today: 3 against 7
  over six months.

## Limits

- Two windows, not the whole chain. The archive window holds 220,000 of the
  archive tier's ~3.26 million blocks, chosen for density. The per-block ratios
  (26 vs 2 shards, filter halving) need confirming over the full archive before
  they are used as fleet figures.
- The bills are `pairs.py` arithmetic on census counts, with no wallet, server
  or network in the loop. Response and setup bytes come from the native
  profile's constants, and filter bytes from the 2.5 B/script estimate.
- The archive bills are clipped to the window; a real restore-old covers genesis
  to tip. Catch-up is modelled in the recent tier only.
- Pinned plaintext counts the window's segments. It is not a fleet memory
  reservation, which also depends on retention and replicas.
- Publisher cost, the open directory's per-block rebuild and restart replay,
  was not measured.
