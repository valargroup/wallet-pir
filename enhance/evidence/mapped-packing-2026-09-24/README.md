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
