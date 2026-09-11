# M1 invalidated-preparation cancellation canary

The [preceding failed run and regression evidence](../productionize-m1-storage-policy-2026-09-11/README.md)
identify a managed worker rejecting a noncanonical candidate roughly 16 seconds
before the controller abandoned its preparation wait. Controller source
`4a23670addfe52bfc15a0eccff86bb68ceb982a9` cancels that wait when ingestion changes
the invalidation epoch. The existing canonical checks and post-activation checks
remain in force. Full make check passed 591 Rust tests, zero failures, two ignored,
and the other required checks. The held-preparation regression also passed on Linux.

The [deployment bundle](deployment.tar.gz) binds all 321 build source files,
source commit, build script, Linux regression, artifact hashes, guarded deployment
script and observed before/after state. Controller SHA256 is
`f74717691566bbfe50c4a0670f2cdbc933dbfaa1c5c7490618b72e414fee2a00`;
controller configuration SHA256 is
`be857e4ecfd87afc158927ca70cdf54199ca0628294cd6c345eb39e52f208818`.
Deployment verified both public origins against the canonical tip and a matching
warm canary worker at 2026-09-11T11:23:01 UTC before starting observation.

The worker remains source `2c4a2507523a39e76aef8a6db3076c40d54026ae`, SHA256
`cc6dabbd03ea2b1bccbe2d92d547e10bb9433e1d96215b7cd240c03dab9c8ec9`.
Fleet configuration SHA256 remains
`c844b3a1e04fc22a0ee95cfd49174e3bf35e8d848eb5a1069254b7ef8675c7fe`.
Persistent storage policy remains enabled and observed; the live fleet adapter
is unchanged. The rollout source argument identifies the reused worker binary,
not the new controller; the deployment bundle records both identities.

`transparent-m1-cancel-prepare-rollout.service`, PID 3540068, started a fresh
acceptance canary at approximately **2026-09-11 11:23:01 UTC**. Its output is
`/opt/transparent-publisher-build/cancel-prepare-20260911/rollout` on the coordinator.
The [initial checkpoint](initial-checkpoint.json) at 11:24:19 UTC records one
new block, 1,107 exact queries, no retries or mismatches, maximum public freshness
11.321 seconds and minimum available RAM 27.593%. Routing epoch
`b1a6acc302ca4bf88b6532837e3c5435` starts available at withdrawal count 22.
The [read-only poll helper](canary-status.py) targets this run.

M1 remains open. No time from the failed predecessor counts. The unchanged
six-hour/300-block canary precedes gated six-worker rollout and a separate
24-hour fleet observation. The earliest six-hour boundary is 17:23 UTC on
September 11; block production may extend it. The existing bounded package
maintenance deferral expires around September 12 at 18:41 UTC and must not be
silently extended.

## Fifteen-minute checkpoint

The [checkpoint](quarter-hour-checkpoint.json) at 2026-09-11T11:38:42.567303+00:00
confirms the same supervisor remains active after 941.325 seconds, with 13 new
blocks, 13,864 exact queries and nine retries, zero mismatches and no in-run
routing withdrawals. Maximum public freshness is 18.894 seconds and minimum
available RAM is 27.183%. Persistent storage policy and helper identity remain
verified, with no cache write failures. This partial window does not satisfy M1.

## Half-hour checkpoint

The [checkpoint](half-hour-checkpoint.json) at 2026-09-11T11:53:21.281376+00:00
confirms the same supervisor remains active after 1,820.040 seconds, with 28 new
blocks, 26,599 exact queries and 23 retries, zero mismatches and no in-run routing
withdrawals. Maximum public freshness is 21.460 seconds, maximum replica freshness
is 20.437 seconds, and minimum available RAM is 26.500%. Persistent storage policy
and helper identity remain verified; no cache write failures are recorded.
This remains a partial acceptance window.

## One-hour checkpoint

The [checkpoint](one-hour-checkpoint.json) at 2026-09-11T12:23:21.740032+00:00
confirms the same supervisor remains active after 3,620.499 seconds, with 45 new
blocks, 53,205 exact queries and 39 retries, zero mismatches and no in-run routing
withdrawals. Maximum public freshness is 22.031 seconds, maximum replica freshness
is 22.034 seconds, and minimum available RAM is 26.500%. Persistent storage policy
and helper identity remain verified; no cache write failures are recorded.
This remains a partial acceptance window, not an M1 pass.

## Terminal failure

This run failed at 2026-09-11T13:12:52.089430+00:00 after 6,590.864 seconds.
The [raw final run](failed-canary.tar.gz) records replica recent-01 freshness
exceeding 60 seconds at block 3479699 (60.466 seconds). It contains 96,160 exact
query results, 77 retries and zero mismatches. No in-run routing withdrawal was
recorded. This run does not qualify M1 and must not be resumed for acceptance credit.

The [reconciler log](failure-reconciler.log) isolates preparation of map
`3d82a3f77e8bc0fedc2431027d082539ae3f75583808d08a37f075d75b87eeb0`
to 58.918 seconds loading and 5.901 seconds warming, completing at 13:13:00 UTC.
The [controller log](failure-controller.log) records subsequent successful
publication of height 3479701 at 13:13:37 UTC. Its recent-01 preparation loaded
in 1.169 seconds and warmed in 4.485 seconds. The underlying cause of the
intermittent loading stall remains under investigation.
