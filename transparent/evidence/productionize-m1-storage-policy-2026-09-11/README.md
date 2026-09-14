# M1 managed storage-policy canary

The [preceding investigation and bounded comparison](../productionize-m1-shoup-reduction-2026-09-11/README.md)
reproduced activation blocked on durable publication-record writes alongside
runtime-cache flushes. A 900-second nodiscard diagnostic passed nine new blocks
and 13,682 exact queries, maximum public freshness 16.509 seconds. It supports
a managed-policy trial, not a claim that discard was the sole cause.

Operations source `864f741` adds the guarded storage helper, persistent worker
prestart, live observer checks, helper-identity gate and rollback. The source
includes the missing-cache-directory preflight correction, verified on all six
workers. Full make check passed 591 Rust tests, zero failures and two ignored,
plus 111 operations tests and the other required checks.

Worker binary remains from source `2c4a2507523a39e76aef8a6db3076c40d54026ae`,
SHA256 `cc6dabbd03ea2b1bccbe2d92d547e10bb9433e1d96215b7cd240c03dab9c8ec9`.
This is an operations change; the binary was not rebuilt or attributed to the
new operations commit. The startup bundle records both identities explicitly.
Storage-helper SHA256 is
`f862a88122c404775f5aa70951ea356d56cd15463c8d0c343aff076c343a54ff`.

The temporary discard experiment restored its original mount setting and its
timer terminated before the managed rollout. No delayed restoration remains
from that experiment. The independent bounded package-maintenance deferral
still expires around 2026-09-12 18:41 UTC and must not be silently extended.

## Deployment and fresh gate

The [complete successful canary upgrade and startup provenance](rollout-start.tar.gz)
include the exact operations manifest, artifact checks, all-worker read-only
preflights, supervisor start script and guarded upgrade/private-query evidence.
Only recent-01 was upgraded. Both public origins reopened after verification.
The persistent helper disables online discard before worker startup, retaining
other mount options and all publication fsync/durability behavior.

`transparent-m1-storage-policy-rollout.service`, PID 3401371, entered fresh
canary observation at approximately **2026-09-11 10:00:09 UTC**. Output lives at
`/opt/transparent-publisher-build/storage-policy-20260911/rollout` on the
coordinator. The [initial checkpoint](initial-checkpoint.json) at 10:00:44 UTC
records 531 exact queries, zero mismatches/retries, no newly visible blocks yet,
and verified persistent nodiscard/helper identity. The routing baseline is
epoch `b1a6acc302ca4bf88b6532837e3c5435`, unavailable-event count 22, available.
Historical/maintenance withdrawals before this baseline do not constitute
in-run withdrawals. The [read-only poll helper](canary-status.py) checks this run.

M1 remains open. No earlier diagnostic or failed-run time counts. The supervisor
requires both six hours and 300 new canonical blocks, unchanged 30/60-second
freshness budgets, two exact clients, no OOM/restart/routing withdrawal and
20% available memory before full-fleet upgrade. It then requires a separate
matching 24-hour all-six-worker observation. The earliest six-hour boundary is
around 16:00 UTC today; the block requirement can extend that time.

## Fifteen-minute checkpoint

The [checkpoint](quarter-hour-checkpoint.json) at 2026-09-11T10:15:42.293074+00:00 confirms
the same supervisor is active, with 12 new public blocks and 13792 exact
queries, zero mismatches, unchanged routing and verified storage policy.
Maximum public freshness is 14.499 seconds; minimum available
memory is 27.128%. This partial window does not satisfy M1.

## Half-hour checkpoint

The [checkpoint](half-hour-checkpoint.json) at 2026-09-11T10:30:45.705989+00:00 confirms
the same supervisor remains active with 19 new blocks, 27459 exact queries and
14 retries, zero mismatches, unchanged routing and verified persistent storage
policy. Maximum public freshness is 14.499 seconds and minimum available memory
is 27.128%. This remains a partial acceptance window.

## One-hour checkpoint

The [checkpoint](hour-checkpoint.json) at 2026-09-11T11:00:41.995263+00:00 confirms
the same supervisor is active after 3,633 seconds, with 35 new blocks, 54,689
exact queries and 24 retries, zero mismatches and no in-run routing withdrawals.
Maximum public freshness is 15.504 seconds, maximum replica freshness is
15.507 seconds and minimum available memory is 26.734%. The persistent storage
policy and helper identity remain verified; the latest pending-save count is
zero and no cache write failures are recorded. This is a partial acceptance
window, not a completed canary or M1 pass.

## Terminal failure and correction plan

The [complete failed run](failed-canary.tar.gz) ended at
2026-09-11T11:10:27.588331+00:00 after 4,218.794 seconds: block 3479594
arrived publicly after 30.536 seconds, exceeding the unchanged 30-second limit.
The run recorded 63,187 exact responses, 33 retries, zero mismatches and no
in-run routing withdrawals. It does not count toward acceptance.

The [controller log](failure-controller.log) and [reconciler log](failure-reconciler.log)
show the managed worker rejecting candidate `4d727126…` as noncanonical at
11:09:58.131 UTC, but the controller only discarded preparation at 11:10:14.475.
The replacement `0fd25fd8…` completed its publication cycle in 12.385 seconds;
recent-01 activation took 0.100 seconds. This failure does not reproduce the
previous long activation flush. A [preceding near-limit event](near-limit-controller.log)
also involved candidate invalidation followed by retry.

The correction path is to cancel the controller's outstanding preparation wait
when ingestion changes its invalidation epoch, retaining the existing canonical
hash checks and post-activation safety checks. A held-preparation regression
must prove cancellation before the fleet replies, continued availability of
canonical old coverage, and successful replacement publication. Required checks
and a verified deployment must precede a fresh acceptance run.

The controller cancellation change passed the [release regression](cancellation-regression.log)
and [full make check](cancellation-make-check.log). The held reply is not released
until after cancellation returns; replacement publication and later reorg checks
also pass. This is source/test evidence only; the correction is not yet deployed.
