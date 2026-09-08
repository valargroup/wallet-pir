# Deployment-runtime live validation — 2026-09-08

Live validation is incomplete. The [manifest](manifest.json) pins revisions, publication,
workflow runs and measurement limits. Initial replica and archive canaries and the
replica rollback passed correctness. The single-slot archive restart missed the
ten-minute target; bounded restore concurrency is implemented and passed CI, but awaits live validation.

| Initial canary | Prewarm | Worker activation | Deployment phases |
|---|---:|---:|---:|
| Replica, first cache population | 244.283 s | 255 s | 369 s |
| Replica, warm restore | 47.093 s | 64 s | 164 s |
| Archive, first cache population | 939.612 s | 1066 s | 1224 s |
| Archive, warm restore | 481.353 s | 605 s | 746 s |

Both warm canaries had zero cache misses and zero cache-write failures. The
replica restored 28 runtimes; the archive restored 160. The archive reached its
56 GiB memory cap and recorded reclamation events, with no OOM events. Its cold
cache population used 48,313,149,440 bytes, leaving 127,928,152,064 bytes free.

The replica rollback restored release `8802cd0`; all five deferred worker process
IDs remained unchanged. The following canary restored from the retained cache.

Raw readiness, cgroup and filesystem snapshots are preserved beside deployment
plans, timings and deployment-step logs. Full workflow links follow from the run
IDs in the manifest. Source tables remain verified on warm restores. Filesystem
caches were not flushed. These observations do not assert fleet capacity or
interruption-free archive service.

All eleven frozen-fixture wallet cases passed across two baseline selections.
The final nine overlapped another session’s publisher worker upgrades; they are
correctness evidence, not a controlled performance baseline or a final rollout gate.

Publisher workflow `34225958778` subsequently activated continuous publication.
The queued four-slot canary `34228351961` was canceled before activation because
its fixed-publication deployment path would overwrite publisher control settings.
The compatibility guard preserves those settings in shadow and refuses active
publication. A coordinated validation window or a continuous-publication-aware
rollout procedure is required before the remaining live gates can run.

No under-ten-minute fleet result, live paired activation, live no-op rollout, or
post-rollout regression is claimed by this evidence.
