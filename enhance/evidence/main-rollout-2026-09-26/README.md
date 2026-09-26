# Main commit rollout: Enhance, Status, and observability

PR [#114](https://github.com/valargroup/wallet-pir/pull/114) merged at
2026-09-26 20:04:09 UTC. The deployed commit is
`13ec1fbe875254d45ba2b37c99b7eb55a517f56d`.
All PR checks passed, including native distributed integration and optional CUDA.
The source tree of the merge exactly equals the tested/built branch tree;
`build-inputs.json` records that binding. The evidence branch adds no runtime changes.

## Deployed scope

All 12 services on seven hosts were restarted onto immutable paths under
`/opt/wallet-pir/releases/<commit>/native/`. `services-after.json` records actual
running process executable hashes, not merely staged file hashes. All match
`binaries.json`, are active, and had zero automatic restarts at verification.

- Enhance coordinator, query ingress, packing router, two CPU workers, GPU worker.
- Live Status controller, GPU worker and router; isolated synthetic Status service.
- APM and the external monitor.

Transparent PIR was explicitly excluded by the user and was not redeployed.
Existing node, proxy, tunnels, credentials, incident/outbox databases and live
serving data were retained. The synthetic Status fixture was upgraded from v1 to
v3 by creating `/var/lib/status-pir-synthetic-<commit>`, leaving its old fixture
and binary available for rollback. Live Status public serving remains disabled:
its config has `public_enabled=false`, and the public init endpoint returns 404.

## Build and validation

CPU binaries were built on Ubuntu 24.04, CUDA binaries on Ubuntu 22.04 with CUDA
12.2. Both use the Haswell CPU baseline. Native protocol remains Enhance v9 and
Status v3. Release hashes and features are recorded alongside this document.

Before rollout:

- Enhance native exercise on the Intel coordinator: 5,607 correct answers,
  zero errors, six publications; p99 194.943 ms.
- CPU and CUDA distributed Status validation: mined, mempool, forked, not-found,
  and remote-fence cases passed.
- Two earlier isolated GPU preflight attempts failed: the first omitted
  LD_LIBRARY_PATH; the second's one-core CPU quota caused cold preparation to
  exceed the 20-second source freshness gate. The successful run used the live
  library path and a three-core quota. No freshness checks were weakened.

After rollout:

- Enhance public HTTPS exact-oracle smoke: 60/60 correct measured answers at
  2 offered queries/sec, zero errors; 107 additional correct warmup answers;
  measured p99 368.639 ms (scheduled p99 369.151 ms).
- Live Status, direct router path, independent canonical-mined oracle:
  600/600 correct at 20 queries/sec for 30 seconds, zero failed/unstarted;
  scheduled-to-completed p99 81 ms.
- Enhance resumed publication at generation 305, anchor 3497223; three published
  replicas and ready packing; no pending commit/abort notifications.
- Enhance readiness, APM page and APM readiness returned HTTP 200.
- Both monitoring loops healthy; external canary passed; no active incidents,
  unknown checks, pending Slack deliveries or delivery failures at final check.

These are deployment smoke checks, not long-duration production qualification.
Rolling restarts temporarily interrupted router eligibility/publication and
produced shadow incidents. These recovered before the new observation window.

## Observation and rollback

New shadow collection started **2026-09-26 20:09:04 UTC** (September 27 00:09 Dubai).
Earliest shadow review is **2026-09-27 20:09:04 UTC** (September 28 00:09 Dubai).
The monitor runs `pir-observability-evidence-13ec1fb.service` for 72 hours, writing
`/var/lib/pir-monitor/rollout-evidence-13ec1fb/`. Earlier evidence directories
remain intact; the prior collector was stopped only after the new one started.
See `observation.json`. Promotion is manual after reviewing 24 hours of evidence,
followed by 24 further hours of active observation. New rules remain shadow;
legacy Slack alerting remains enabled. The previously confirmed Slack firing /
recovery tests were not resent during this deployment.

Each host retains original unit settings and binary hashes at
`/root/wallet-pir-rollout/<commit>/<unit>/`. `install-release.py` captures the
installer used; `rollout-driver.py` captures its invocation mapping. Rollback uses
the installer `rollback` mode with the same unit/source/hash/commit arguments,
restoring the previous drop-in and restarting that service. Run under the host's
`/run/lock/wallet-pir-production.lock`; retain incident databases and state.
The installer is staged under `/root/full-stack-stage/` on root-managed hosts,
and `/home/paperspace/full-stack-stage/` on GPU hosts (use sudo). Synthetic
rollback restores the original fixture path. Restart a distinct observation
window after any rollback. Do not blindly repeat apply mode or erase evidence.
