# Managed preparation and gated fleet rollout

The corrected recent-01 canary is deployed. The six-hour/300-block gate, the
coordinated six-worker batch and 24-hour fleet observation remain pending.
The current supervisor is `transparent-managed-hardening-rollout-3.service` on
the coordinator; it stops on failure and cannot promote a failed or mismatched
canary result. See [deployment](../../deployment.md) for operating targets and
[remaining work](../../remaining-work.md) for acceptance gates.

## Source and validation

- Worker source: `d5fea93`, built from a clean committed archive with portable
  `x86-64-v3` and `+pclmulqdq` flags. Binary SHA-256:
  `817b621cbffc5743750ed0d91280dc041ed4040f55764aadf1230ce6cc718eee`.
- Operations source: `7754d6c`; the worker source and dependencies are unchanged
  since the tested worker build. Foreground publication and the reconciler use
  the same `fleet.json`.
- 106 worker tests passed, including revision churn, reorg invalidation/restart,
  wallet continuation and exact row retrieval. Strict worker Clippy passed.
- 26 release-profile Linux library tests passed on the coordinator. The final
  operations suite passed 61 tests, covering owned preparation, cancelled
  waiters, restart recovery, routing eligibility, private intermediate progress,
  withdrawal/orphan refusal, maintenance guards and supervised gate ordering.

[manifest.json](manifest.json) records source identities, commands, workload,
network, journal and geometry. Raw test/build logs are compressed beside it.
Host facts and public authorities are preserved in [before.json](before.json),
[after.json](after.json) and [after-progress-fix.json](after-progress-fix.json).

## Defects reproduced and corrected

The previous third soak failed after 572.605 seconds at block 3,476,738.
Preparation started only after public activation and could lose the opportunity
to join during a burst. Persistent reconciler ownership now starts preparation
from the desired publication and survives foreground quorum cancellation.

The first managed attempt then exposed two separate defects. Startup stopped
with only 20 of 28 runtimes warm after transient admission refusals; a bounded
retry now retains those targets while preserving cancellation and corruption
failures. The deployment helper also reopened metadata before the controller's
listener was available, causing the gate's immediate HTTP 502 failure. Its
historical `canary-upgrade/result.json` reported success prematurely and must not
be interpreted as acceptance. Reopening now waits for HTTP readiness under the
maintenance guard and verifies both public origins afterwards.

The second managed attempt warmed all 28 runtimes with zero failures and passed
41 exact directory/page queries during maintenance verification. Its loaded
gate failed after 141.740 seconds: four blocks became publicly visible within
27.150 seconds, but block 3,476,803 exceeded the replica's 60-second budget. No
OOM or restart occurred. The trace shows a candidate completed preparation
before that deadline but was superseded before it could join public routing.

Correction `7754d6c` lets an **unrouted** replica retain that canonical progress
privately, provided the public authority already covers its height. Future or
orphaned candidates and withdrawn publications are refused. Public membership
still requires the exact current map and warm live attestation. This correction
is tested; sustained live acceptance remains the purpose of the current gate.

The immutable [attempt-1.tar.gz](attempt-1.tar.gz) and
[attempt-2.tar.gz](attempt-2.tar.gz) contain complete failed-run evidence;
[attempt-1-worker.log.gz](attempt-1-worker.log.gz) preserves the startup failure
and recovery. Previous-soak raw logs are also included in the first archive.

## Current live run

Run 3 began at **2026-09-09 00:55:04 UTC**, observing node height 3,476,812. It
reuses the verified worker binary after the operations-only correction and
starts fresh samples with two sustained exact private-query clients. The worker
was not restarted for this correction. Neither failed run contributes samples.

The [first snapshot](run-3-first-snapshot.tar.gz), taken at 00:59:43 UTC, contains
4,342 exact queries and one newly observed block: public visibility 17.792
seconds, replica visibility 20.937 seconds, no OOM/restart, 5,906,673,664-byte
cgroup peak and 26.6% available host memory in the latest sample. The supervisor
was still in `canary_observation`. This short prefix is not the completed gate.

The supervisor's state and complete ongoing evidence live at:

```text
/opt/transparent-publisher-build/managed-hardening-rollout-3/status.json
/opt/transparent-publisher-build/managed-hardening-rollout-3/canary/
```

A passing result must satisfy both duration and block count and match the
binary, fleet script and configuration. The supervisor then preflights and
upgrades all six workers under maintenance, verifies canonical authority and
exact private rows before reopening, and observes every worker for 24 hours.
Archive soft limits are implemented for that batch but are not yet deployed.
Rollback preserves current publication records and revocations; unverified
recovery retains the public maintenance response.

This evidence does not prove a particular user's balance, mobile performance,
or sustained capacity of the full fleet. The previously validated wallet
adapter and production parent-filter features are preserved.
