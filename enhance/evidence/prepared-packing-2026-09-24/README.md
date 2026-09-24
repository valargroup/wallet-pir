# Coordinator-prepared packing rollout — September 24, 2026

Production coordinator, query ingress and packing router were switched to main
commit `3685438310a5630d16aa79c15436ac016f190e6c` at approximately 18:50 UTC.
The server SHA-256 is
`1638f7c75b07f2e13b0fc66fbe4877f57ce44015f598456955e7ac70d57062ad`.
Build: Rust 1.91.0, locked dependencies, `release-fast`,
`RUSTFLAGS=-C target-cpu=skylake-avx512` on the Linux build host.

APM was already deployed from `3685438` and was not restarted, preserving its
in-memory graph history. CPU workers already ran `47ac3e3`; server source is
identical between that commit and `3685438` (the latter changes one APM label).
The optional GPU placement policy and worker processes were preserved.

The prepared codec is pinned to ipir-sp
`a38f0965ef3af22ad3a9e42c239a2312e95bfb09`, published on branch
`prepared-packing-artifacts`. No worker evaluation algorithm or wallet wire
protocol changed. See the [artifact and rollout contract](../../ops/deploy/prepared-packing.md).

## Validation before cutover

- Codec library: 65 tests passed, including a prepared-state packing round trip
  and malformed input checks.
- Server library: 111 tests passed after final metadata guards.
- Final local `packing_http` integration: passed, including exact wallet answers,
  overload, disconnect, watchdog and revocation checks.
- [Isolated qualification](isolated-qualification.log): 30 successive new
  material publications, each overlapping 120 exact-answer queries at 2/s and
  init at 1/s. All 3,600 qualification answers were correct. All six complete
  five-minute packing latency windows passed, with all 600 samples per window
  at most one second. Intentional failure probes outside these windows explain
  nonzero lifetime rejected/failed counters.
- Isolated router: two assigned CPU cores, 7-GiB memory maximum, no swap,
  admission four and object limit six. Observed retained assignment reached
  five objects. Peak cgroup memory was 5,856,489,472 bytes; zero max/OOM events.
  It loaded 31 artifacts, had zero artifact load failures and performed zero
  preprocessing calls. This is not evidence for six resident objects or a
  six-hour hardware qualification.

The isolated 30-minute run used the implementation before two final descriptor
validation guards were added. The final guarded implementation passed the
server tests and HTTP integration above and was used for production migration.
The subsequent `47ac3e3` worker histogram instrumentation is included in the
production build; the packing implementation is unchanged.

## Migration and first public checks

Migration was rehearsed on a copied production controller: approximately 95
CPU-seconds initially, then ten CPU-seconds when reusing prepared files. Snapshot
fields, sessions and recovery state were unchanged. Opening the controller store
advances its fencing epoch by design.

The production deployment held `/run/lock/wallet-pir-production.lock`, paused
review load and public queries, stopped the old coordinator and serving roles,
and saved control state before migration. Four distinct prepared artifacts
covered five retained snapshots. All prior snapshot/controller fields were
verified unchanged, except the expected one-step fencing epoch increase.
Service executable overrides preserve current arguments and limits. Public
queries reopened only after router and ingress readiness, required replicas and
zero coordinator resident packing objects were confirmed.

[Public smoke test](public-smoke.json): 30/30 correct answers, zero errors,
142.847 ms client end-to-end p99. This is not a packing-only percentile.

A separate 30-minute production run started at approximately 18:52 UTC with
2 query/s and 1 init/s. It failed at approximately 19:01 UTC when publication overlapped an independent
eight-client GPU load test and the router reached its 7-GiB cgroup limit.
The sustained qualification is **failed**, not complete. See the incident below.

Rollback assets and exact previous unit definitions are retained under
`/root/prepared-rollout-3685438` on the affected hosts. Old binaries and raw
hints remain available. Restore all three previous executable definitions
together if necessary; preserve current publication and recovery history.


## Production OOM and retained latest release

At 19:01:01 UTC, the kernel killed the router for cgroup OOM. Kernel accounting
recorded 7,499,096,064 bytes of anonymous memory and only 20,480 bytes of file
cache. This was heap pressure, not downloaded artifact page cache.

For approximately 90 seconds before the failure, physical usage was about
6.65 GiB while the router reported four resident objects and only 3 GiB charged.
At 19:00:46 the charge rose to 4.75 GiB as another artifact load was admitted.
Physical memory reached the 7-GiB service limit, and the subsequent OOM restarted
the process. During the independent load, observed throughput was approximately
21 queries/second, compared with the isolated qualification's two queries/second.
The service still admitted at most four concurrent requests.

The router's fixed 6,400-MiB packing ledger does not reserve request allocations
or account for allocator-retained heap; query admission uses a separate semaphore.
It therefore admitted a load with insufficient real memory headroom. Samples do
not distinguish allocator retention/fragmentation from live request scratch;
heap profiling is required before attributing the entire discrepancy to a leak.

An automatic rollback was started, then explicitly cancelled by the operator.
The `3685438` overrides were restored on all three roles, readiness was verified,
and public queries reopened. A subsequent smoke test passed 20/20 exact answers
with no errors and 129.407 ms end-to-end p99. APM and workers were preserved.
The qualification load was stopped. The latest release remains deployed at the
operator's request, without a sustained-memory qualification claim.
