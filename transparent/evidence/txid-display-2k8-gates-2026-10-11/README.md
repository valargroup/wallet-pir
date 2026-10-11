# Txid display one-query layout gates, 2026-10-11

Development-host measurements of the `txid-2k8` layout
([design](../../docs/txid-display.md#lookup-one-row-of-one-table-per-bucket)),
taken before any production step, as the
[prepared cutover](../../docs/deployment.md#txid-display-one-query-cutover-prepared-2026-10-11)
requires. Nothing touched a production host. The only production input is the
public display map, fetched read-only over HTTPS.

## Result

| Gate | Criterion | Measured | Result |
|---|---|---|---|
| Placement census | Every archive one segment; median seed tries at most 2 | 189 archives of 90,000–90,185 entries; all one segment; seed tries p50 1, p95 2, max 2 (178 on the first seed); recent shard 90,017 entries, first seed | Pass, on the live-map census below, not on the journal itself |
| Recent rebuild, one table at 4 build threads | p95 at most 2× `txid-2k`, well under the ~75 s block interval | `txid-2k8` p50 2.36 s, p95 3.08 s; `txid-2k` p50 1.51 s, p95 1.62 s (30 builds each, interleaved): 1.56× at p50, 1.90× at p95 | Pass, with little margin at p95; see the host note |
| Runtime memory | Per table and total against the display unit's 40G `MemoryMax` | 80.04 MiB steady per runtime (85.75 MiB once in six), planned 80.09 MiB; 191 runtimes hold 15.2 GiB with four builds in flight (26.9 GiB if all were charged at the bound) | Pass: 38% of 40G |
| Native demo | Exact on found and absent txids, one query each | 14 entries match the Python oracle; every warm lookup, found or absent, is one 38,920 B query and one 11,280 B reply; the corrupted oracle is refused | Pass |

The real-journal census is still open: no copy of the v2x genesis journal was
reachable from the task's worker, which has no production credentials.
[Remaining work](../../docs/remaining-work.md#txid-display-one-query-layout-2026-10-11)
names the run that closes it.

## Placement census

[`census/census.json`](census/census.json), from
`txid-placement-census --map inputs/live-txid-shards-2026-10-11.json --trials 100`
([source](../../services/transparent-shard-server/examples/txid-placement-census.rs)).

**Input.** The public map `GET https://transparent-pir.valargroup.dev/v1/txid/shards`
at 2026-10-11 ~19:25 UTC ([`inputs/`](inputs/live-txid-shards-2026-10-11.json),
SHA-256 `4c6d0e62…`): the production `txid-2k` lineage, 426 archives sealed at
40,000 plus the recent shard, 17,053,658 entries from height 1 to 3,514,373.

**Method.** An archive seals at the first block that lifts it to the target,
so its count minus 40,000 is the part of that block past the boundary. The
426 overshoots (mean 7.8, max 185) are a sample, weighted by block size, of
what a seal at any target overshoots by. The census re-seals the 17.05M
entries at 90,000, giving archive `i` the target plus the `i`-th observed
overshoot, then places each table with the production `place_bucket` over
pseudorandom tags. A tag is a SHA-256 output and a row a SHA-256 of the tag
under the seed, so random tags have the placement statistics of real ones;
only which seed a given archive keeps differs from a journal run.

**Result.** 189 archives, every one a single segment, fullest row exactly 72;
11 archives needed the second seed. 41,508 entries are unsealed at the tip,
so the 90,017-entry recent window reaches back into the newest archive.

How often one seed fits, against the Poisson model the design was argued
from (100 tables per size):

| Entries per table | Fill | First seed fits | Poisson | Tries p50 / p95 / max | Overflowed (no seed of 64 fits) |
|---:|---:|---:|---:|---|---:|
| 90,000 | 61.0% | 90% | 92.6% | 1 / 2 / 2 | 0 |
| 92,000 | 62.4% | 87% | 86.1% | 1 / 2 / 3 | 0 |
| 94,000 | 63.7% | 78% | 75.6% | 1 / 3 / 6 | 0 |
| 96,000 | 65.1% | 64% | 60.4% | 1 / 4 / 7 | 0 |
| 98,000 | 66.5% | 34% | 41.3% | 2 / 7 / 11 | 0 |
| 100,000 | 67.8% | 26% | 22.1% | 3 / 11 / 23 | 0 |
| 105,000 | 71.2% | 2% | 0.6% | 64 / 64 / 64 | 74 |
| 110,000 | 74.6% | 0% | 0.0% | 64 / 64 / 64 | 100 |

One seed's pass over 90,000 tags takes 0.026 s on one thread here, so the
64-seed worst case is about 1.7 s of the publisher's build.

**The recent shard in dense eras** (derived from the map, not measured per
block). The recent shard holds the window or every unsealed entry, whichever
is more. Unsealed entries exceed 90,000 by at most the last 100 blocks plus
one. The densest stretch of the chain, heights 3,135,716–3,136,099, averages
104.2 entries a block, so the recent shard could reach about 100,700 entries
there: one seed fits about 22% of the time and 64 seeds fail with
probability below 10⁻⁶. A burst denser than that over 100 blocks spills the
recent shard into a second segment until the archive seals, and logs
`txid_display_overflow`.

## Recent rebuild

`hint_bench --runtime-only --geometry <g> --repeats 10 txdirectory=synthetic:<n>`
with `TRANSPARENT_BUILD_THREADS=4`: `TableRuntime::build`, which encodes the
database, computes the hint and builds the two-mask preprocessing, exactly as
a worker prepares a recent revision. Three rounds, base and new interleaved
([`rebuild/`](rebuild/)).

- **New:** `txid-2k8`, 90,000 synthetic entries laid out by the new
  publisher (2,048 rows × 8,192 B, 16 MiB).
- **Baseline:** `txid-2k` at `3b23148d`, the commit this work started from,
  40,000 entries (2,048 rows × 4,096 B, 8 MiB), with the same bench patch
  ([`rebuild/hint_bench-base-3b23148d.patch`](rebuild/hint_bench-base-3b23148d.patch)).
  The build time depends on the shape, not on the entry count: every row of
  both tables holds data.

| | n | p50 | p95 | max | mean | Peak RSS |
|---|---:|---:|---:|---:|---:|---:|
| `txid-2k` | 30 | 1.512 s | 1.624 s | 1.642 s | 1.523 s | 340–372 MB |
| `txid-2k8` | 30 | 2.359 s | 3.079 s | 3.247 s | 2.537 s | 747–778 MB |

The table holds twice the bytes, so twice the hint and preprocessing work;
p50 grows by 1.56×. The `txid-2k8` builds have a heavier tail (2.20–3.25 s)
that the baseline does not show; the host note below is the likely cause,
and a quiet 8-vCPU host would show whether it is real.

## Runtime memory

[`residency/txid-2k8-txdirectory-12.txt`](residency/txid-2k8-txdirectory-12.txt):
`shard-residency --geometry txid-2k8 --table txdirectory --runtimes 12`.
Runtimes 7–11 each added exactly 80.04 MiB of RSS and runtime 12 85.75 MiB;
the earlier ones absorb the allocator's warm-up, as in the
[`txid-2k` measurement](../txid-display-backfill-sizing-2026-10-07/README.md).
The planned built size is 80.09 MiB and the reservation 144.09 MiB: each of
the two 4,096-byte blocks of a row has its own compiled matrix, so a
`txid-2k8` runtime is about twice a `txid-2k` one (40.05 MiB), and there are
half as many tables. `runtime.rs` pins both figures, and the combined
worker's admission at 191 resident runtimes, in its tests.

At the census's 189 archives, the recent shard and one retired recent
revision: 191 × 80.09 MiB + 4 × 64 MiB in flight = 15.2 GiB, against 16.7 GiB
for today's 427 `txid-2k` tables. A year of growth, about 64 seals a year
at 40,000 entries, adds about 28 archives and 2.2 GiB.

## Native demo

`make transparent-txid-demo` ([`demo/report.json`](demo/report.json),
[`demo/demo.log`](demo/demo.log)) at `a887b004`; the report marks the tree
dirty because this README and `manifest.json` were not committed yet.

- Mainnet blocks 347,499–347,501 (an archive and a recent shard) and genesis
  (a recent shard starting at height 0): 14 transactions, every case the
  fixture distinguishes, each entry equal to the independent Python oracle's.
- Each warm lookup sent exactly one request: one query, 38,920 B up and
  11,280 B down. The absent txid's transcript is identical.
- The run with a corrupted oracle exits nonzero, as it must.
- The first attempt failed: a publication starting at height 0 has no last
  sealed height, and the recent map's `sealed_through` underflowed. The map
  now names `unsealed_start`, the first height no archive holds, instead.

## Host and provenance

- Host: development worker, DigitalOcean 8 vCPU Intel Xeon Platinum 8280
  (AVX-512, no SHA extensions), 31 GiB, Linux 6.8.0-142, shared with other
  agents' tasks. **The task's cgroup caps it at 3 CPUs** (`cpu.max 300000
  100000`), so 4 build threads ran on 3 CPUs' worth of time: absolute build
  times are upper bounds for an unthrottled 4-thread run, and the ratio is
  the comparison the gate asks for. `transparent-pir-txid-display-01` is
  `m-8vcpu-64gb`; its CPU was not measured.
- Toolchain: rustc 1.97.1, profile `release-fast`, the repository's
  `target-cpu=native`.
- Source: the census, residency and new bench binaries were built from the
  task branch's working tree; the commit that holds exactly this source is
  recorded in [`manifest.json`](manifest.json). Binary digests are in
  [`binaries.sha256`](binaries.sha256).
- The prepared production request is in [`deploy/`](deploy/README.md).
