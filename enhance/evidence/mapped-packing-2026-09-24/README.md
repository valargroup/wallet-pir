# Mapped packing memory investigation — September 24, 2026

This update addresses the [production prepared-packing OOM](../prepared-packing-2026-09-24/README.md)
without lowering query admission or changing host size. The four-request router
setting, six-object assignment limit and 7-GiB/zero-swap service limit are kept.

The isolated baseline used the previous heap-backed reader, eight client lanes,
512 exact-answer queries per publication and a 600,000-record fixture. Its
[allocator samples](baseline-heap.jsonl) were collected with a small external
`mallinfo2` sampler loaded only into the isolated process. Maximum free bytes
retained in allocator arenas were approximately 1.92 GiB. After traffic stopped,
about 1.64 GiB remained free inside arenas. These are allocator measurements,
not a complete attribution of every byte of production RSS.

The [baseline run](baseline-test.log) completed seven publication cycles before
failing on HTTP 429 under saturation. It is profiling evidence, not a passing
qualification. Its router later stopped at its configured runtime limit.

The replacement maps authenticated public matrices directly, keeps only compact
permutation tables and offsets on the heap, reuses a persistent loader thread
and bounded packing scratch, and removes whole-view query pins. Publication
loading checks physical memory headroom in addition to logical charges. Existing
prepared files and control version 2 remain compatible.

Dependency: ipir-sp `66b05ac59139897674489a5ea6461118772ee1c0`.
Validation completed before the Linux sustained run:

- 66 inspiring library tests pass, including mapped/owned production-size
  two-block equivalence, boundary inputs and malformed data/key rejection.
- 113 server library tests pass, including six retained objects plus incoming
  replacement admission and physical-headroom boundaries.
- Encrypted `packing_http` integration passes: exact answers, overload,
  disconnect, watchdog and revocation behavior.
- Independent code review approved after addressing mapped-load budgeting and
  production-layout test coverage. That review does not establish physical
  memory qualification.

The stronger Linux run uses eight scheduled query lanes (8 queries/s), init at
1/s, 30 material publications, two dedicated router CPU cores and the unchanged
service limits. Completion measurements are pending. An initial eight-lane attempt returned
HTTP 429 and is not a qualification pass. Wallet crypto in the test fixture now
runs on dedicated blocking threads with independent async runtimes, so it cannot
occupy the simulated services' Tokio workers. The local HTTP regression passed
again after that fixture change (32.43 seconds).

The final physical-headroom check also subtracts dirty and writeback pages from
its inactive-file estimate. Independent follow-up review approved this
conservative adjustment; publication progress remains part of qualification.

## Direct production deployment and live load

At the operator's explicit request to deploy directly, the router was upgraded
before the extended isolated run completed. Wallet commit `4d14feb` is on main.
The deployed Linux binary SHA-256 is
`4655685bb2c96517b297d79cb5b7da5d89b5e53fc94ab246e4bc715ca09a23d9`;
Rust 1.91.0, `release-fast`, locked dependencies, Skylake AVX-512 target.

Only the packing-router process restarted, at 20:00:15 UTC September 24.
Coordinator, ingress, workers and APM stayed running. Public query admission
was briefly paused in Caddy and restored once the router's new incarnation was
ready. The production deployment lock was held through cutover and smoke, then
released. Existing limits and query settings are unchanged; no rollback occurred.

[Public smoke](production-smoke.json) passed 30/30 exact answers, zero errors,
129.599 ms successful-query end-to-end p99. This is not packing-only latency.

At 20:02:38 UTC, a 30-minute load began: 8 queries/s, eight load-client lanes,
and init at 1/s. It stops at approximately 20:32:38 UTC (00:32:38 Dubai Sep 25).
Service: `apm-mapped-load-4d14feb` on the coordinator. Per-minute exact-answer
reports are in `/root/mapped-rollout-4d14feb/load-*.json`; router resource/metric
samples are in the same directory on the router. Both jobs have runtime bounds.

The [first minute](production-load-01.json) verified 478/480 answers with two
HTTP 502 errors; the [second](production-load-02.json) verified 479/480 with one
502. There were no incorrect answers. Successful-query p99 was 180.095 ms and
158.079 ms respectively. Caddy logged broken pipes while uploading to ingress;
ingress recorded stale-routing 409 responses around publication changes.
The router recorded zero rejected/failed queries and zero artifact load failures.
This evidence points to ingress/proxy publication handling, but does not prove a
one-to-one cause for each public 502. The live run is not an error-free pass.

Early production memory observations: four artifacts used about 2.84 GiB total
with 76 MiB anonymous memory; after publication, total peak was about 4.26 GiB.
No max/OOM events or service restarts occurred in the first two load minutes.
See the [early cgroup snapshot](production-early-memory.json). These are early
measurements, not a completed long-duration memory qualification. The isolated
run's first five-minute window had 2,400/2,400 packing samples at most one second.
