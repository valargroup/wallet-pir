# Prepared history runtimes: paired benchmark harness, 2026-10-14

The harness and fixture for the six-arm paired benchmark of
[prepared history runtimes](../../docs/architecture.md#prepared-history-runtimes)
(`ship_runtimes` off against on). **The six arms have not been run**: the benchmark
requires a host-exclusive allocation with no competing builds on the measured CPU
groups, and the shared development hub could not provide one to this task (see
[status](../../docs/status.md#prepared-history-runtimes-implemented-not-measured-2026-10-14)).
No result here measures a benefit, and nothing was deployed or touched production.

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

## Harness validation

Short runs that only check the harness works end to end are not measurements and are
not retained as results.
