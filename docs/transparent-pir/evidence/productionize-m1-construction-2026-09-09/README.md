# M1: runtime construction optimization

The Amsterdam frozen-fixture experiment profiles and reduces publication runtime
construction. Live worker configuration and canary state are unchanged. M1 fleet
acceptance remains a separate gate.

## Workload and baseline

The inputs and host are the same as the [admission comparison](../productionize-m1-admission-2026-09-09/README.md):
14 assigned recent shards, 28 initially warm runtimes, and two successive
publications changing directory and page tables at heights 3,477,141 and
3,477,142. Both tables change their hashes in every publication, so whole-table
reuse cannot save these four builds. The generator is the existing Amsterdam
16-vCPU Xeon Platinum 8280 host; each isolated worker uses CPUs 0–3 and clients
use CPUs 4–7 outside its cgroup. Worker MemoryHigh is 5.5 GiB, MemoryMax 7 GiB,
swap is disabled, runtime cache is 5 GiB and each run has a fresh 10 GiB disk cache.
Full initial readiness and both warm activations are required. Two exact-query
clients run throughout the measured wave.

The stricter worker screen remains 14 seconds, derived in the linked prior
experiment. This is a provisional allowance within the public 30-second target,
not a measured fleet tail bound. The modeled host check includes 768 MiB of
non-worker overhead in an 8 GiB host; it is not an actual live-host measurement.

The instrumented baseline produced 20.592 seconds worst worker visibility.
Its four measured runtime builds averaged 3.343 seconds in packing preprocessing;
disk saves averaged 0.735 seconds. Public setup generation was negligible.
The baseline executable and source hashes were verified against its manifest.

## Changes

1. Reuse the fixed public mask images already resident for online packing.
   `ipir-sp` commit `3c9e82b` adds a constructor using the existing internal
   decomposition and matrix operations. It omits only zero reference-body work
   and duplicate public image creation. Client secrets, uploaded key bodies,
   randomness and database-dependent results are not reused.
2. Commit `61dc83e` runs the independent left/right c1 cascades in parallel,
   joining their digit outputs in the original order. Block parallelism is
   preserved. The application pins the full immutable dependency commit.
3. Snapshot export batches the same LE coefficients into an 8 KiB stack buffer.
   Its checksum input, flush, sync, atomic rename and file format stay unchanged.
   The observed disk saving is small; this is not the primary speedup.
4. Construction tracing splits encoding, setup, hint generation, packing and
   parameter publication. The burst runner accepts selected build-slot counts
   and records Cargo manifests/lockfile alongside worker source and binary hashes.

The [second review](review.md) checks reference equivalence, public-image
provenance, parallel digit order and unchanged persistence semantics. The frozen
known-answer vector and production-parameter differential tests bind the new
path to the original implementation. Existing snapshots retain format v1 because
all stored coefficients and setup derivation remain identical. Memory
reservations and admission policy were not weakened.

## Results

See `comparison.json` and the raw variant directories. `summarize.py` reconstructs
stage means from the final four builds in each log and query completion gaps
from per-client exact completions. A completion gap includes retry/backoff time;
it is not the latency of a single successful query.

The reuse-only ablation took 18.896–19.096 seconds across three repetitions and
failed the 14-second screen. Its mean packing time was 2.938–2.957 seconds.
This establishes that public-image reuse alone is insufficient.

The derived query-availability comparison uses retry attempts divided by exact
completions plus retries, and the largest per-client completion gap. It marks a
run worse than this experiment's baseline if either exceeds that baseline. This
is a descriptive no-regression screen, not a new production SLO or statistical
tail guarantee. The runner's exit status gates readiness, memory, exactness and
latency; `comparison.json` additionally records this availability comparison.
Runs were sequential by variant, not randomized/interleaved. Retain the earlier
one-slot observations as context rather than treating a single baseline as a
distributional bound.

## Decision

| Variant | Slots | Repeat | Worst visibility | Exact | Retries | Retry fraction | Largest completion gap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| baseline | 1 | 1 | 20.592 s | 211 | 18 | 7.9% | 2.089 s |
| reuse-only | 1 | 1 | 19.096 s | 209 | 16 | 7.1% | 0.842 s |
| reuse-only | 1 | 2 | 18.896 s | 191 | 25 | 11.6% | 1.618 s |
| reuse-only | 1 | 3 | 19.079 s | 187 | 24 | 11.4% | 1.809 s |
| parallel-batched | 1 | 1 | 15.831 s | 147 | 18 | 10.9% | 1.406 s |
| parallel-batched | 1 | 2 | 15.307 s | 147 | 23 | 13.5% | 1.722 s |
| parallel-batched | 1 | 3 | 15.870 s | 151 | 22 | 12.7% | 1.208 s |
| parallel-batched-two | 2 | 1 | 12.774 s | 77 | 60 | 43.8% | 4.721 s |
| parallel-batched-two | 2 | 2 | 13.009 s | 87 | 53 | 37.9% | 4.716 s |
| parallel-batched-two | 2 | 3 | 12.975 s | 87 | 53 | 37.9% | 4.747 s |

The combined one-slot change reduced worker visibility to 15.307–15.870 seconds
(about 23–26% below this baseline), and packing to approximately 2.07 seconds
(about 38% lower). It misses the 14-second screen in all three runs. Two slots
meet that timing screen in all three runs at 12.774–13.009 seconds, but overload
retries rise from 7.9% of attempts in the baseline to 37.9–43.8%, and completion
gaps rise from 2.089 seconds to 4.716–4.747 seconds. **Reject promotion: no final
configuration passes both latency and the availability comparison.**

All ten processes start 28/28 warm and activate both publications fully warm.
All 1,494 successful queries are exact, including 696 in the final six runs.
There are no fatal client failures, OOMs, OOM kills or hard-limit hits. Every run
passes client isolation and the modeled memory check (about 21.86% headroom).
Final complete cold startup is 102.2–102.7 seconds with one slot and 76.1–76.2
seconds with two. The one-slot supervisor exits 1 for latency; the two-slot
supervisor exits 0 for its timing/memory/exactness gates. Its exit 0 does not
override the separately derived availability failure.

The new bottleneck is visible in memory admission. At approximately 5.9 GB
current cgroup usage, two 512 MiB construction reservations leave insufficient
room for a 256 MiB query reservation under the unchanged 90%-of-MemoryMax guard.
Logs record those refusals; client retries all report the generic free-cache-
capacity error. This is temporary work-memory admission, not incorrect PIR
answers. The final one-slot first run also reports 5.406 GB anonymous memory and
0.469 GB file cache at its final sample. File-cache pressure and conservative
reservation overlap therefore need separate treatment; those samples do not
justify blindly subtracting reclaimable memory or reducing reservations.

The next bounded task is to qualify memory admission for concurrent preparation
and queries: measure allocation lifetimes, stop charging construction scratch
once it is actually released while retaining bounded serialization accounting,
and control disposable file-cache pressure if needed. Keep the same memory
limits and full residency. Require all repetitions to satisfy both the 14-second
screen and query availability before a matching loaded canary. No live change
or replacement soak was started by this experiment. M1 remains open.

## Validation

Final `make check` exited 0: **579 Rust tests passed**, zero failed, two intentional
manual benchmark tests ignored. Ops/report/docs checks, formatting and clippy
passed. The final upstream inspiring release suite passed, including the frozen
vector, production coefficient equivalence, boundary and malformed-input tests.
Application wrong-shard/wrong-table replays, malformed queries, disk byte-format
boundaries, restore equivalence and corrupt-cache fallback passed. Logs and the
conditional second-review closure are retained here. Checksums cover raw outputs,
source archives, upstream patches, analysis and validation evidence.

## Reproduction and provenance

Repository base: `14f3919a86fab1c2be44877026a9d50270f70e88` plus the captured source
archives. Final dependency: `61dc83e7410ff13ccfdd9ad1e830b711bd9080ed`, published on
`valargroup/ipir-sp` branch `perf/reuse-public-preprocessing`; both upstream patches
are retained here. Rust 1.91.0 release build, fat LTO, codegen-units 1, with
`-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq` on the existing coordinator.
The exact compiler-reported test artifact path and matching coordinator/generator
SHA-256 were checked before the final launch.

Final executable: `a32218307562b716e89f1b3354f3c7a350548c7462de67896c1e5f7fe85a2c04`.
Reuse-only executable: `4cef8431d865ed7bc43b16ec7ef039014595cb2b445c796cc2cb42d4b5996ff8`.
Baseline executable: `b122324f7d053e1e2f1a2f00da34a4460436eb8eed17941f584837af4692e1e0`.

Final generator root: `/opt/transparent-construction-parallel-20260909`.
Use its `measured` and `measured-two` directories for the one/two-slot results.
The shared fixture is `/opt/transparent-full-fixture-20260909/fixture-canonical.json`.
Each command uses the runner with `--systemd --external-clients`,
`--worker-budget-seconds 14 --host-overhead-bytes 805306368`,
`--build-slots 1` or `2`, `--repetitions 3`, and the fixture/binary/source identities
above. The run manifests preserve source, fixture and executable hashes.

Preliminary launch mistakes are excluded: the first profiler picked up an older
binary before compilation completed and was stopped. Two reuse launch attempts
encountered a copied existing output directory; the runner rejected its existing
manifest before running a workload. The first also selected an obsolete Cargo
artifact name. Only the verified `profile2/profile`, `reuse2/measured` and final
`parallel/measured*` outputs are used here. No old output was overwritten and no
live service was restarted. The final launcher derives Cargo's artifact path from
the completed build log, creates a fresh root and waits for preceding experiments
to finish before using the same pinned CPUs.
