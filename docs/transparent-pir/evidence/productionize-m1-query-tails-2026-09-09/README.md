# M1 query-tail qualification — 2026-09-09

Decision: the three two-slot repetitions passed the isolated screen on the
Amsterdam generator, but the matching live canary failed public freshness after
78.576 s and one completed block. Do not promote the fleet or treat the generator
result as target-host qualification. M1 remains open.

## Implementation and diagnosis

Batch little-endian database/NTT snapshot reads in 8 KiB buffers without changing
the format, coefficient bounds, checksums or admission limits. Query diagnostics
separate evaluation-slot admission, runtime acquisition, blocking dispatch and
evaluation. Retained-revision restoration can occur during setup before the timed
query; complete client completion gaps cover that work too.

The successful traces show negligible evaluation-slot/runtime waits in the timed
query, millisecond-scale blocking dispatch, and slower evaluation under concurrent
construction. Setup restores still cost roughly 0.7–0.8 s. Batching is a bounded
source improvement; these repetitions do not isolate its causal contribution from
scheduling variation. Successful-query p95 remains variable (two slots:
0.127–0.981 s), so this is not a claim of uniformly faster warm queries.

## Results

| Run | Worst worker visibility (s) | Exact queries | Retry fraction | Max client completion gap (s) | Combined screen |
|---|---:|---:|---:|---:|---|
| repeat-1-slots-1 | 14.198 | 148 | 0.00% | 1.068 | Fail (timing) |
| repeat-1-slots-2 | 10.923 | 87 | 5.43% | 1.212 | Pass |
| repeat-2-slots-2 | 11.298 | 105 | 0.94% | 1.189 | Pass |
| repeat-2-slots-1 | 14.070 | 141 | 0.00% | 1.143 | Fail (timing) |
| repeat-3-slots-1 | 14.254 | 150 | 0.00% | 1.016 | Fail (timing) |
| repeat-3-slots-2 | 11.211 | 99 | 3.88% | 1.281 | Pass |

All 730 successful queries were exact. All six runs started with 28 warm runtimes,
overlapped preparation with external clients, completed both publications and
drained clients successfully. Every cgroup high/max/OOM counter was zero; modeled
host headroom was 27.7–28.6%. One slot consistently misses the provisional 14 s
worker screen. All three two-slot runs meet it and remain below the original
7.79% retry / 2.089 s completion-gap references. The reference is one descriptive
baseline, not a statistical SLO. See the
[memory phase evidence](../productionize-m1-memory-phases-2026-09-09/README.md)
for its corrected complete client counts and preserved failed predecessors.

`readbatch/` contains the complete raw reports, logs, manifest and external client
records. `summarize.py` regenerates `comparison.json` using complete client logs.
Every report/client pair was checked for equality allowing only floating-point
JSON round-trip differences. Source archive and current committed files matched
all manifest hashes. The supervisor exits 1 because the one-slot controls fail;
that does not invalidate the three explicitly qualified two-slot results.

## Reproduction and validation

Existing Amsterdam generator and identical fixture/limits to the linked memory
phase experiment: fourteen recent shards, worker CPUs 0–3, clients 4–7 outside
its cgroup, 5 GiB cache, 10 GiB disk cache, MemoryHigh 5.5 GiB, MemoryMax 7 GiB,
no swap. Fresh process/cache each run. Three alternating one/two-slot pairs.
The 14 s screen leaves provisional fleet overhead; actual public 30 s, replica
60 s and host memory targets remain unchanged.

```sh
python3 ops/scripts/run-transparent-burst.py --systemd --external-clients \
  --worker-budget-seconds 14 --host-overhead-bytes 805306368 \
  --fixture /opt/transparent-full-fixture-20260909/fixture-canonical.json \
  --test-binary /opt/transparent-readbatch-20260909/burst-test \
  --source-sha 14f3919a86fab1c2be44877026a9d50270f70e88 \
  --build-slots 1 2 --repetitions 3 \
  --out /opt/transparent-readbatch-20260909/measured
python3 docs/transparent-pir/evidence/productionize-m1-query-tails-2026-09-09/summarize.py
```

The build base was `14f3919` plus the captured source, subsequently committed as
`d778c62`. Linux Rust 1.91.0 release flags:
`-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq`.
Test binary SHA:
`965a984912eff4e71ce87993aa4d57a199129af690ec4d236d62cdbb4ff8ebfe`.
Worker artifact SHA:
`c9d88ca7d73014c8217c4240c4af858921fbfaa2848da5a1d49f9fbb7727308f`.

`make check` passed 585 Rust tests with zero failures and two ignored manual
benchmarks. All 70 transparent operations tests passed. Targeted Linux disk
compatibility, cancellation and batched-reader boundary/canonicality tests passed.
Logs: `make-check.log`, `ops-tests.log`, `linux-tests.log`. `source.tar.gz` contains
manifest-listed benchmark sources; `rollout-ops.tar` captures the rollout helpers.

## Canary handoff

The installer now honors explicit roster build slots, replacing both CLI spellings
before staged verification. Canary provenance includes the roster digest, so a
changed build-slot roster cannot reuse acceptance. These changes are in `d778c62`.

`start-qualified-canary.py` checks the three qualifying runs, selected artifact and
fleet-script identity and absence of another running rollout. It saves predecessor
operator inputs, selects two slots for recent replicas (archive settings unchanged),
installs the tested operation helpers, then starts the existing gated supervisor.
The desired roster changes before rollout; each worker's running unit changes only
when upgraded. Predecessor inputs remain on the coordinator under
`/opt/transparent-publisher-build/readbatch-20260909/pre-canary`.

Supervisor: `transparent-m1-qualified-rollout.service` on the coordinator.
Output: `/opt/transparent-publisher-build/readbatch-20260909/rollout`.
It upgrades recent-01, requires six hours AND 300 new blocks with sustained exact
queries and the live deployment targets, then performs the approved fleet batch
and 24-hour observation. Any failed phase stops progression. Elapsed time alone
cannot pass a gate. Check the dated handoff observation below; M1 is still open.

## Failed canary and hardware mismatch

The canary began 20:00:53 UTC and failed at 20:02:11 UTC. Block 3,477,724 exceeded
the observer budget at 30.131 s; the controller subsequently recorded 30.228 s
actual freshness. The first block had 16.882 s public / 15.468 s replica visibility.
Clients completed 524 and 528 exact queries with no retries. No fleet batch ran.
Recent-01 retains the new binary with two build slots; the other five retain their
previous binaries. The supervisor is stopped in `failed`, not observing.

The generator exposes a Xeon 8280 with AVX-512, but the four-vCPU recent worker
exposes `DO-Regular`, family 6/model 79, without that backend. The library selects
`U16Avx512Kernel` when AVX-512F is available and `ChunkedSplitKernel` otherwise.
Matching CPU count and memory limits did not match the kernel or hardware speed.
Live worker prewarm took 8.911 and 10.572 s in the failed burst, versus shorter
generator preparations. Actual `--build-slots 2` was verified from the running
unit; an unapplied slot setting is not the explanation.

`canary-failed/` preserves the complete failed observer results, queries and
upgrade verification. `canary-worker.log` records the preparation times. Next
reproduce the existing fallback backend explicitly on the generator and measure
against actual target-host stages before another loaded gate. Backend matching
alone still does not reproduce shared-vCPU scheduling or target memory bandwidth.
