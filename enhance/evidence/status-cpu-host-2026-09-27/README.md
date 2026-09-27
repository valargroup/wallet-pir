# Status on a CPU droplet — 2026-09-27

The live Status router and worker moved from the Paperspace P4000
(`status-pir-p4000-ams1`) to `status-pir-01`, a DigitalOcean `s-4vcpu-8gb`
droplet in the production VPC. Both roles now run CPU-only. The controller,
its configuration and the public route were not changed; only the SSH tunnel
target moved. `manifest.json` records binaries, hosts, commands and timings.

The Enhance optional GPU mirror (`enhance-pir-gpu-01`) was out of scope and is
unchanged.

## Build and pre-cutover validation

The role binary is `c5a6486`, the controller's own revision, built without
`cuda` for Haswell. On the droplet, `validate-distributed` at the 1,572,864-entry
ceiling **failed** with one router preparation thread: the worker took 8.8 s and
the router 17.3 s, so the candidate outlived the 20-second freshness limit and
preparation returned 503. With `STATUS_PREPARATION_THREADS=4` all cases passed
(worker 8.7 s, router 6.9 s, source age 16.2 s at completion). The router unit
now uses four preparation threads. The full-occupancy margin is small; live
occupancy is far lower.

## Cutover

APM was put in shadow mode, then both tunnels were switched at 14:50:00 UTC. The
controller fenced epoch 9 and published at 14:50:19.9: public Status was
unavailable for about 20 seconds. The P4000 roles were stopped but kept
enabled until the gates passed.

## Dashboard and alerting

APM `6c43798` shows the topology as `Enhance host → CPU router + worker` and
shows GPU utilization only when a role reports it. After redeploy the shadow
drop-in was removed (`PIR_APM_ALERT_MODE=active`), all Status coverage and HTTP
rules read fresh and healthy, and a labeled test firing/recovery drained through
the real Slack outbox (HTTP accepted, queue empty). Slack receipt was not
confirmed by a person in this record. The external monitor stayed healthy.

## Load gate

30 minutes at 20 QPS through the router query origin, the same path as the
morning's P4000 soak:

| Measure | P4000 baseline (10-minute batches, same day) | CPU droplet |
| --- | --- | --- |
| Correct / offered | 12,000 / 12,000 per batch | 36,000 / 36,000 |
| p50 / p99 latency | 37–38 ms / 68–79 ms | 49 ms / 162 ms (max 401 ms) |
| Observation age at answer, p50 / max | 5.4 s / 15.1 s (one batch) | 5.5 s / 18.7 s |

The gate (p99 < 2 s, p50 < 700 ms, zero failures) passed. No APM incident
fired, no role restarted and there were no OOM kills. 364 publications were
queryable 1.6 s (p50) and at most 9.4 s after observation. One preparation was
deferred with a 429 while the router was busy; the committed view was retained.

The worker's anonymous memory rose to 4.2 GB in the first minutes, near its
initial 4 GB limit, and then held steady. The limit was raised to 6 GB live,
without a restart, and the template now uses 6 GB. The router peaked at 0.8 GB.
Host load reached 5.2 on four vCPUs.

A first run through the coordinator-forwarded query path was stopped after
2,031 arrivals: its 25 failures were all the coordinator's two-concurrent
per-client cap, which a single load client exceeds once CPU queries overlap.
Real clients are separate addresses. It is retained in `raw/load-full.tgz`.

## Retirement

After the gates, the P4000's `status-worker`, `status-router`, synthetic
`status-pir` units were disabled and archived under
`/home/paperspace/status-gpu-retired-20260927/`. The coordinator's synthetic APM
tunnel, its key directory and the P4000 `status-control` host key were archived
under `/root/status-cpu-cutover-20260927/`. Deleting the Paperspace machine is a
separate console action.

## Limits

This is one 30-minute run from one client host, not the six-hour qualification.
The oracle covers canonical mined answers only. Full-occupancy preparation uses
16 of the 20 freshness seconds on this droplet.
