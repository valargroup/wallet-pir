# M1: publication preparation and fair memory admission

This isolated Amsterdam experiment diagnoses the full-residency slowdown and
tests a targeted admission correction. It does not establish fleet acceptance
or change a live worker. The frozen inputs are the same fourteen recent shards
and three publications used in the [previous comparison](../productionize-m1-full-residency-2026-09-09/README.md).

## Baseline finding

The baseline moves both exact-query clients out of the worker process/cgroup and
onto CPUs 4–7. The worker retains CPUs 0–3, MemoryHigh 5.5 GiB, MemoryMax 7 GiB,
zero swap, a 5 GiB RAM runtime cache and fresh 10 GiB disk cache. The modeled
8 GiB host reserves 768 MiB for non-worker memory. The process snapshot independently
confirms disjoint CPU affinity and cgroups; the fixed runner also validates them.

| Run | Build slots | Initial warm runtimes | Worst worker visibility |
| --- | ---: | ---: | ---: |
| Repeat 1 | 1 | 28 / 28 | 20.683 s |
| Repeat 1 | 2 | **25 / 28** | 29.191 s |
| Repeat 2 | 2 | **25 / 28** | 28.506 s |
| Repeat 2 | 1 | 28 / 28 | 20.648 s |

**The two-slot rows are invalid full-residency publication comparisons.** Three
startup builds exhausted their admission retries. The benchmark only checked warm
readiness after activation, allowing the first candidate to repair missing startup
work. Its original success/exit-zero fields are preserved, but do not qualify those
runs. The earlier comparison also omitted a startup readiness assertion; its apparent
cold-start speedup is not sufficient evidence of completely warm startup.

`baseline-stages.json` is derived from the preserved debug logs. In repeat 1,
the two-slot startup had 1,164 memory-admission rejections and 1,161 prewarm retries,
built only 25 runtimes, and the next phase built five runtimes rather than the two
changed tables. One-slot startup completed all 28. Each one-slot publication spent
about 8.5 seconds in runtime computation and 1.4 seconds saving its two runtimes;
source loading was about 0.3 seconds. Stage durations may overlap and must not be
summed as a wall-clock critical path. Admission rejections also include query/restore
work; `prewarm_retries` identifies retries by the warmer.

## Correction

Cold builders now retain their construction permit and wait in a FIFO queue for
scratch admission. New builders cannot repeatedly overtake an older blocked builder.
The original reservation size, conservative accounting and ten-percent cgroup
reserve are unchanged. Query and restore reservations remain nonblocking. A builder's
memory wait is limited to 30 seconds after it obtains its FIFO turn; request deadlines
also cover runtime acquisition. Cancelling a waiter releases its turn, while
reservations already moved into blocking work remain owned by that work.

The benchmark now saves and requires complete initial `/v1/ready` state before the
wave. The runner rejects older fixture reports that lack that proof. Client failures,
missing isolation evidence, incomplete warming and missed budgets fail qualification.
Per-runtime debug stages and prepare loading/warming durations preserve the diagnosis.

## Corrected result and decision

| Run | Slots | Initial warm | Worst worker visibility | Exact queries | Overload retries | Largest gap between exact completions |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Repeat 1 | 1 | 28 / 28 | 20.376 s | 211 | 16 | 1.711 s |
| Repeat 1 | 2 | 28 / 28 | 13.037 s | 69 | 59 | 6.401 s |
| Repeat 2 | 2 | 28 / 28 | 16.842 s | 133 | 44 | 3.456 s |
| Repeat 2 | 1 | 28 / 28 | 20.365 s | 211 | 18 | 1.798 s |

All four runs completed full startup and both warm activations, passed exact query
validation and client isolation, and retained modeled host headroom of approximately
21.86%. There were **624 exact queries**, no fatal client errors, OOMs, OOM kills,
or hard-limit hits. Startup prewarm had no failed targets; the second two-slot run
still had 44 transient prewarm retries. Two-slot complete startup took 101.6–104.8 s,
versus 134.6–134.9 s with one slot. Those are now verified complete-startup timings.

**Do not promote two build slots.** Both one-slot runs and one two-slot run missed
the provisional fourteen-second screen, so the final supervisor correctly exited
**1**. All four passed the weaker worker-only thirty-second ceiling; that ceiling
alone leaves no adequate allowance for the rest of the fleet publication path.
No replacement canary or live worker configuration change was made.

The remaining variance is explained by the fixed trace: in the slower two-slot
run's second publication, cold-build memory admission waited **5.070 seconds**.
The two runtime builds effectively serialized; warming took 10.127 s, compared
with roughly 6.4 s when both could run together. Runtime computation still costs
about 8.3 seconds summed across the two tables when serialized, plus about 1.4 s
for disk saving. Increasing configured concurrency cannot guarantee that memory
admission will permit simultaneous construction.

The faster publication also has an availability tradeoff. Overload retries caused
up to a 6.401 s interval between one client's successful query completions. This
is a completion gap, not the latency of a single request. Successful query p95
alone omits retry delays: in that run it was only 0.216 s. `comparison.json`
therefore retains retries and per-client gaps beside ordinary successful-query
latencies. These observations motivate reducing runtime construction work and
checking query availability during preparation before another qualification.

## Screening budget and limits

`screening-budget.json` derives a **provisional 14-second worker screening budget**
from three earlier publication cycles. Subtracting the temporally corresponding
recent-04 warm duration leaves 4.575–6.893 seconds per cycle. Rounding the largest
residual to seven seconds and adding one second engineering margin per publication
reserves sixteen seconds for a two-publication burst: 30 − 16 = 14 seconds.

This residual is a conservative proxy, not a measured fleet tail bound: it also
contains worker loading already measured by this benchmark, and only three cycles
were available. It is suitable for screening candidates; matching loaded canary
and fleet observation must still enforce the deployment's public/replica budgets
and actual host headroom. Loopback transport, a frozen tail, debug logging and a
finite comparison cannot establish live capacity. No acceptance threshold was raised.

## Provenance

The baseline uses HEAD `14f3919a86fab1c2be44877026a9d50270f70e88` plus the source
snapshot in `baseline-source-and-results.tar.gz`. Its manifest hashes source files
and the portable release executable. Builds use Rust 1.91.0 with
`-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq` on the coordinator; experiments
run on the existing Amsterdam load generator. Fixture identity and file hashes
remain in the linked prior evidence. All runtime-cache mutations are disposable
and isolated from both live publication and the frozen fixture.

`fixed-source-and-results.tar.gz` preserves the exact corrected source and raw
reports; `build.json` verifies those source hashes against the reviewed workspace.
The debug-stage extraction is reproducible with `summarize-stages.py`.
`linux-builds.log` also retains the initial compilation failure: the test attempted
to call a private readiness method. It was corrected to inspect the actual
`/v1/ready` response before the measured qualification; no performance run used
the failed build. `generator-runs.log` preserves both supervisor outcomes.

## Validation

`make check` passed: 578 Rust tests, zero failures, and 2 ignored manual benchmark tests.
The five burst-runner tests passed, including preservation of worker/client failures
and rejection of an invalid screening budget. FIFO admission and cancellation
regressions passed. Documentation links and archived source hashes were checked.
`validation.json` deliberately records the separate performance qualification
exit code of 1; passing code checks do not turn that performance result into acceptance.
