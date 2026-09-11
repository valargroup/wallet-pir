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

`transparent-m1-collection-timing-observe.service` ran a 900-second diagnostic
with two query clients and zero minimum blocks under
`/opt/transparent-publisher-build/collection-timing-20260911/diagnostic-observation`.
Even a successful result is insufficient for the six-hour/300-block gate.
Collect worker logs for runtime disk lock wait/prune, collection snapshot/disk/
directory work, and preparation load/warm durations. The earlier bounded BPF
trace recorded one 1,378 ms flock wait; it does not establish the cause of the
9.295-second collection stage in the failed run.

The existing bounded maintenance deferral still expires September 12 around
18:41 UTC and has not been extended.

## Completed diagnostics, 07:50 UTC

The [raw live observation](live-diagnostic-observation.tar.gz) completed after
900.427 seconds with 17 new blocks and 12,320 exact queries. Maximum public
visibility was 23.991 seconds and canary visibility 23.994 seconds. This is a
short diagnostic, not the required six-hour/300-block acceptance run.

The [worker journal](live-worker-timing.log) contains 16 preparation and
collection stage samples. Loading ranged from 0.862 to 2.254 seconds; warming
from 6.731 to 10.246 seconds. Disk lock waits were under 60 microseconds,
disk collection under 3.543 milliseconds, snapshot collection under 17.601
milliseconds and directory collection under 27.121 milliseconds. The earlier
9.295-second collection did not recur; these samples do not explain its cause.
Warming remains the dominant measured preparation stage.

The Amsterdam qualification service exited successfully after three alternating
repetitions per build-slot setting. [Full provenance and raw runs](amsterdam-slots-qualification.tar.gz)
include the source/binary/fixture manifest, reports, logs and external-client
traces; the [summary](amsterdam-slots-summary.json) records all six results.

| Build slots | Per-run maximum worker visibility (seconds) | Query p95 range (seconds) |
|---|---|---|
| 1 | 12.463, 12.410, 12.317 | 0.398–0.449 |
| 2 | 8.805, 8.046, 9.123 | 0.941–1.097 |

All runs completed with exact queries, overlapping isolated client load,
completed persistence, no kernel OOM events and passing modeled memory headroom
and 14-second worker budgets. The isolated-worker experiment excludes the
controller's serial publication queue and is not live-fleet freshness acceptance.
Keep two build slots: one reduces query p95 in this fixture but consistently
slows publication, the current failing gate. No slot configuration was changed.
A subsequent [retained-manifest survey](table-reuse-survey.json), using the
[read-only survey script](table-reuse-survey.py), compared ten successive tail
revisions spanning heights 3479434–3479445. Both directory and page table segment
descriptors changed in every comparison, while geometry stayed identical and
each manifest explicitly superseded its compared predecessor. Reusing whole
unchanged tables would save no builds in this sample. Do not prioritize a cache
identity rewrite on the assumption that these live table bytes stay unchanged.
This small retained-history sample does not establish behavior for every block.
