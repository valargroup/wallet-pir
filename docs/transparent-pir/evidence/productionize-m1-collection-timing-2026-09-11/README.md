# M1 collection and preparation diagnostics — 2026-09-11

Source `3d13da6` adds duration telemetry without changing worker collection,
locking, preparation or publication behavior. The preceding
[failed burst canary](../productionize-m1-unpublished-reorg-2026-09-11/README.md)
remains unsuccessful. No diagnostic time counts toward M1.

`make check` passed: 591 Rust tests, zero failures, two ignored, plus repository
checks. The focused stage-timing test also passed, including the response-field
allowlist test. The Linux integration log verifies one executed passing
`live_prepare_activate_and_invalidate` test (19 filtered). The initial Linux
library filter selected zero tests and is not acceptance evidence.

The hashed source snapshot and artifact manifest accompany this record.
The diagnostic worker SHA256 is
`46645937893ef38238bce2ef1124b9a468e02742273f0948e6f004206eb16f7e`.
The standard single-worker upgrade completed at approximately 07:34:09 UTC,
verified 36 exact private queries and warm service, and reopened the public
origins. The controller binary remains the unpublished-reorg correction; only
the fleet timing adapter and recent-01 worker were updated. Deployment results
and verification are under `diagnostic-upgrade/`.

`transparent-m1-collection-timing-observe.service` runs a 900-second diagnostic
with two query clients and zero minimum blocks under
`/opt/transparent-publisher-build/collection-timing-20260911/diagnostic-observation`.
Even a successful result is insufficient for the six-hour/300-block gate.
Collect worker logs for runtime disk lock wait/prune, collection snapshot/disk/
directory work, and preparation load/warm durations. The earlier bounded BPF
trace recorded one 1,378 ms flock wait; it does not establish the cause of the
9.295-second collection stage in the failed run.

The existing bounded maintenance deferral still expires September 12 around
18:41 UTC and has not been extended.
