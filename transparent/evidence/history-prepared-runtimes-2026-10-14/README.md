# Prepared history runtimes: paired benchmark, 2026-10-14

The six-arm paired benchmark of
[prepared history runtimes](../../docs/architecture.md#prepared-history-runtimes)
(`ship_runtimes` off against on), run on the shared development hub (roman-dev-2) by
Roman's direction, without an exclusive allocation. Development host only: nothing
deployed, no production host touched.

## Result: not accepted

Shipping halved the worker's CPU in every pair, answered every query exactly and needed
no worker build, but **acceptance fails**: the worst shipped query p95 and p99 and every
activation-delay quantile are worse than the worst baseline ones, every arm (both modes)
breaks the 30 s freshness bound, and every arm is contaminated by other work on the host
(foreign CPU on the measured groups 5–70%, bound 2%). `ship_runtimes` stays off.

| Arm | Shipping | Worker CPU-s | Worker build pool CPU-s | Publisher CPU-s | Queries / exact | Query ms p50 / p95 / p99 | Activation delay s p50 / p95 / p99 / max | Worker shipped / refused / built | Staged GiB | Peak RSS MiB worker / publisher | Foreign CPU % publisher / worker / load |
|---|---|---:|---:|---:|---|---|---|---|---:|---|---|
| pair1-off | off | 1824 | 1031 | 289 | 24000 / 24000 | 21.9 / 58.5 / 91.6 | 16.8 / 27.9 / 29.7 / 36.6 | 0 / 0 / 228 | 5.8 | 1265 / 301 | 35 / 5 / 19 |
| pair1-on | on | 826 | 61 | 1224 | 24000 / 24000 | 23.6 / 78.7 / 130.3 | 20.4 / 32.7 / 35.7 / 40.5 | 182 / 0 / 0 | 18.8 | 980 / 734 | 27 / 25 / 31 |
| pair2-off | off | 1600 | 839 | 248 | 24000 / 24000 | 38.7 / 90.7 / 115.4 | 20.9 / 33.2 / 36.6 / 41.0 | 0 / 0 / 182 | 4.7 | 1304 / 301 | 53 / 24 / 45 |
| pair2-on | on | 819 | 54 | 1122 | 24000 / 24000 | 34.2 / 96.9 / 162.8 | 23.0 / 37.9 / 41.2 / 47.5 | 162 / 0 / 0 | 16.7 | 1002 / 921 | 34 / 37 / 70 |
| pair3-off | off | 1725 | 939 | 275 | 24000 / 24000 | 27.5 / 83.2 / 110.0 | 18.4 / 28.5 / 32.1 / 36.0 | 0 / 0 / 206 | 5.3 | 1281 / 304 | 44 / 11 / 24 |
| pair3-on | on | 846 | 60 | 1222 | 24000 / 24000 | 25.3 / 84.9 / 130.1 | 21.6 / 36.5 / 40.2 / 44.0 | 178 / 0 / 0 | 18.3 | 993 / 725 | 27 / 29 / 34 |

Over the 300 measured blocks of each arm:

- **Worker CPU fell in every pair**: 1,824 → 826, 1,600 → 819, 1,725 → 846 CPU-s (−51% to
  −55%); its build pool, where builds and shipped self-checks run, 1,031 → 61, 839 → 54,
  939 → 60. The publisher took the work instead: 248–289 → 1,122–1,224 CPU-s.
- **Exact and healthy**: 144,000 of 144,000 answers decoded to the row read from the
  published segment file; no query failed or went unstarted; readiness held for every
  probe; shipping arms loaded 162–182 runtimes with 0 refused and 0 built; the publisher
  linked 1,046–1,178 unchanged files and built only the changing tail runtimes. The
  tail's directory shard sealed inside every window.
- **Latency and freshness worse**: worst query p95/p99 90.7/115.4 ms off against
  96.9/162.8 ms on; worst activation delay p50/p95/p99 20.9/33.2/36.6 s off against
  23.0/37.9/41.2 s on. The publisher's preparation runs on the publication's critical
  path, as Display's did ([2026-10-07 bench](../txid-display-shipped-runtimes-bench-2026-10-07/README.md)),
  and the larger staged copy (16.7–18.8 GiB against 4.7–5.8 GiB, local disk) adds to it.
  Freshness fails in the baseline too: this host's contention dominates both.
- **Memory**: host available memory never fell below 66.5% (gate 20%); the 4 GiB worker
  cache was fixed before the first pair ([budget.json](raw/attempt2/budget.json)).
- **Restart** (outside the window): with shipping the worker reloaded 8 runtimes and built
  none (7.0–8.4 s to ready); without, it restored 7 from its disk cache and built 1.
- The staged bytes are a local copy, not a network measurement: no claim about
  production transfer or fleet capacity follows.

Attempt 1 (raw/attempt1) is retained as failed: its load generator cached a
publication directory the publisher later collected, so 10,997 of 24,000 answers in
`pair1-off` (all HTTP 200) were never checked; it was stopped during `pair1-on`. Attempt 2
used the fixed load generator (commit `e68a6a93`) and service binaries unchanged from
`c46c75ac` (hashes in [raw/attempt2/binaries.sha256](raw/attempt2/binaries.sha256) and
[raw/attempt1/binaries.sha256](raw/attempt1/binaries.sha256)).

## What the arms measure

Each arm, from identical immutable inputs and empty runtime caches:

1. A mock node and journal writer (`history_bench_chain drive`) serve a copy of the
   fixture journal rolled back to the base height.
2. The worker, a recent replica under a one-replica assignment, starts on the bootstrap
   publication and builds its runtimes (cold, the same in both arms).
3. The real publication controller (`transparent-publish-controller`, `follow_journal`,
   `directory_choice: all`, `ship_runtimes` per arm) with [fleet.py](harness/fleet.py),
   which stages exactly as `transparent-live-fleet.py` does, on local disk. Its first
   cycle re-offers the bootstrap publication: with shipping on, the publisher's cold
   preparation of every runtime.
4. One warm-up block, then the measured window: 300 blocks appended at 4 s intervals
   under open-loop 20 queries/s (`history_bench_load`) cycling a sealed shard's
   directory, a sealed shard's page shard, the tail's directory and the tail's newest
   page shard. Every answer is decoded and compared with the row read from the
   published segment file; a query not started within 1 s of its slot is unstarted.
5. A worker restart, outside the window.

CPU groups: publisher 0–1, worker 2–3, load generator 4–5, node, adapter and sampling
6–7; publisher and worker each run two build threads; the worker one build slot and
two query slots, a 4 GiB cache and an 8 GiB runtime disk cache. [bench.py](harness/bench.py)
samples every 0.25 s each role's CPU and RSS, the worker's per-thread CPU (its
`runtime-build-*` pool), every CPU's busy time (so CPU spent on a measured group by
anything outside the arm is visible), host memory and worker readiness.
[analyze.py](harness/analyze.py) summarizes each arm and applies the plan's acceptance;
an arm with more than 2% foreign CPU on a measured group is reported contaminated and its
pair cannot qualify.

## Fixture

`bench.py fixture --source <slice> --blocks 90000` (default first height 3,262,749):

- Source: the read-only real mainnet v3 journal slice on the hub at
  `ai-dev/evidence/decoupled-pages-journal/recent` (heights 3,262,749–3,473,686,
  provenance and checksums in the
  [v12 offline end-to-end record](../v12-offline-e2e-2026-10-12/README.md)), never written.
- Fixture journal: its first 90,000 blocks rebased to heights 0–89,999 (events' heights
  rewritten, block hashes kept); recent tier (`decoupled-2k-8k`) from height 2,000,
  archive (`decoupled-32k-32k`) below.
- Probe publication of the whole fixture: recent directory shards seal at 33,186, 66,789
  and 80,938.
- Base height 66,637: the bootstrap publication holds the sealed recent shard 1
  (2,000–33,186) and the tail 33,187–66,637; warm-up block 66,638; measured blocks
  66,639–66,938, in which the tail seals at 66,789 and a new tail starts.

The fixture is built on the hub under the task workspace (`bench/work/fixture`), with
`initial.SHA256SUMS` and `journal-full.SHA256SUMS`; they record the exact bytes every arm
starts from, which the command above regenerates from the source.

## Files

- [summary.json](summary.json): `analyze.py` over attempt 2 (all six arms and the
  acceptance), computed from the uncompressed records; `gunzip` the `.gz` files in a copy of
  an arm directory to recompute it.
- `raw/attempt2/<arm>/`: arm events, appended blocks, every query, 0.25 s process and
  CPU samples, controller and worker logs, fleet stagings, worker and controller metrics,
  and the arm's controller, fleet and roster configs; `raw/attempt2/budget.json`,
  `binaries.sha256`, `revision` (the harness commit), `pairs.log`.
- `raw/attempt1/`: the failed attempt as recorded.
- `raw/fixture/`: the fixture record and the checksums of the bootstrap publication and
  fixture journal every arm copied.
- [SHA256SUMS](SHA256SUMS) covers `raw/` and `summary.json`.

Short harness validation runs before the measurement (5 blocks each) were not
measurements and are not retained.
