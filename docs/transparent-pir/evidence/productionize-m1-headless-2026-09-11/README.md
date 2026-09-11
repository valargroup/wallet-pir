# Persistent headless worker configuration — 2026-09-11

The [runtime console comparison](../productionize-m1-control-path-failure-2026-09-11/README.md)
motivates the optional `headless_console` fleet setting. The installer stages a
small helper and preflights it without changing console state. On activation,
a privileged worker ExecStartPre runs the helper before the worker starts,
including on reboot. An enabled serial console is required; unknown framebuffer
drivers are refused before writes. Existing helper and worker unit are backed
up for binary rollback. Runtime graphics are not automatically rebound by a
rollback; see the deployment contract.

Read-only observer checks require the expected helper digest, the loaded
pre-start command, boot enablement, serial access and unbound framebuffer state.
The helper digest is included in canary provenance and the full-fleet admission
check. No timing, memory, duration, block or exact-query gate is relaxed.

The [operations suite](m1-headless-tests-final.log) passed 97 tests. New coverage
includes read-only preflight, missing serial/unknown driver refusal before
writes, idempotent unbinding, malformed bindings, boot/load configuration,
changed helper and console rejection, staged-only installation, installed
verification and matching helper provenance for fleet promotion.
[Full `make check`](m1-headless-make-check.log) passed: 590 Rust tests, zero
failures, two ignored, with formatting, lint, documentation and report checks.

[Linux preflight](m1-headless-linux-preflight.log) passed on recent-01. The
subsequent persistence check intentionally failed because the runtime-only
experiment lacked the loaded pre-start hook. This confirms the gate rejects
that incomplete state. [All six workers](fleet-preflight.json) passed read-only
preflight with an enabled serial console and virtio framebuffer. Only recent-01
was unbound from the earlier experiment; the other five were not changed.

The helper SHA-256 is
`1daa5418f1dc9a510e839f032c2aed6b062395bd855d5b6a729db563f4cf44df`.
The [deployment/start capture](deployment-start.json), collected with the
[read-only capture script](capture-start.py) at 01:04:32 UTC, confirms the
recent-01 upgrade passed, including 39 exact verification queries. Worker PID
695393 started after the privileged helper completed successfully at 00:58:39.
The unit is boot-enabled, its loaded pre-start hook is present, and the serial
console remains enabled with the framebuffer console unbound. This verifies
restart installation; no host reboot was performed.

The fresh canary began at **2026-09-11 00:59:10 UTC**, height 3,479,113, under
`transparent-m1-headless-rollout.service` (PID 2658846). Its output is
`/opt/transparent-publisher-build/headless-20260911/rollout`. Frozen operations
source is `f383b01`; worker source remains `043c051`. The capture includes the
operations manifest, artifact and configuration digests. The embedded
`enabled.json` is the earlier pre-launch record (`canary_started: false`);
the start events and live systemd state establish the subsequent launch.

The durable availability counter baseline is six, representing events before
this gate. Any new withdrawal fails this run. Earlier diagnostic probes were
stopped. A separate six-hour local HTTP/Unix probe started at 01:00:27; its
490 samples per path at capture had no errors. Those probes aid diagnosis and
do not substitute for acceptance.

This is **partial deployment evidence**, not a passed canary. Both six hours
and 300 new blocks, followed by the matching six-worker rollout and 24-hour
observation, remain required. Other workers have not been promoted.


The [first-hour checkpoint](hour-1-checkpoint.json), captured with
[capture-checkpoint.py](capture-checkpoint.py) at 01:59:42 UTC, confirms the
same supervisor PID remains active after 3,632 seconds. The partial record has
53,184 exact queries, 35 retries and no mismatches; 41 blocks passed both
visibility checks. Maximum observed public/replica visibility is 22.92 seconds.
Across 119 worker samples, minimum available host memory is 26.07%, with no
restarts or OOM kills. The routing counter remains at its baseline of six.
This checkpoint is a summary of a running observation, not an acceptance result
or a replacement for the final raw logs. The full gates above remain open.


## Final outcome: failed at 02:01:57 UTC

The [complete failed canary archive](failed-canary.tar.gz) supersedes the
running checkpoints: 3,767.108 seconds, 44 accepted new blocks, 54,932 exact
queries, 39 retries, no mismatches. Block 3,479,158 was first observed publicly
at 30.001 seconds, exceeding the unchanged 30-second limit. No fleet promotion
occurred. The routing withdrawal counter remained at six, its baseline.

The [controller/reconciler journal](failure-journal.log) shows three successive
activations: height 3,479,156 at 02:01:27.852 (19.850-second controller freshness),
3,479,157 at 02:01:43.532 (22.510 seconds), and 3,479,158 at 02:01:56.693
(28.613 seconds). The observer rejected the last at 02:01:57.650. Controller
activation and external observation are different measurements; the earlier
activation does not retrospectively prove the public visibility gate passed.
Several staging SSH/rsync operations took multiple seconds while closely spaced
blocks awaited sequential publication.

[Local probes during 02:01:00–02:02:10](failure-local-probes.json) remained fast:
140 Unix samples (maximum 10.94 ms), 139 HTTP samples (maximum 8.32 ms), no
errors. This failure does not reproduce a worker-local readiness stall.
The next investigation must attribute staging and activation delays by host,
separate queueing from preparation, and measure public read timing. Qualify a
correction before a fresh complete gate; no time from this failed run counts.
