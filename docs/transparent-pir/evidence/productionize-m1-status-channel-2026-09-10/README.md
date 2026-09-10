# M1 status channel investigation — 2026-09-10

M1 remains incomplete. The post-reorg canary failed after 1,506.461 s and 18
blocks at 2026-09-10 04:52:03 UTC on public `/v1/shards` HTTP 503. Both direct
status attempts timed out; membership was withdrawn at 04:52:01.915 and restored
at 04:52:07.264. No reorg was recorded. The prior direct-connection fix was
insufficient. No fleet promotion or 24-hour observation occurred.

## Measured separation of costs

A read-only 90-round probe on the real recent-01 compared direct SSH executing a
no-op, direct SSH requesting status through the Unix control socket, and HTTP
readiness, concurrently per round while publication continued. This is diagnosis,
not a loaded-canary replacement. See `samples.ndjson` and `summary.json`.

| Path | Median wall time | Maximum wall time |
|---|---|---|
| HTTP ready | 2.50 ms | 23.94 ms |
| Fresh SSH no-op | 335.96 ms | 1,104.41 ms |
| Fresh SSH plus status | 404.90 ms | 1,096.84 ms |

The maximum measured Unix status request itself was 6.13 ms. All 270 requests
completed. Connection establishment dominates these measurements, but the probe
did not reproduce the full multi-second stall. The failure-window SSH logs show
pre-authentication broken pipes and a roughly four-second gap in session logs;
these do not establish the deeper host-scheduling or network cause.

## Persistent connection comparison and implementation

The verified 60-round comparison (`persistent-verified-samples.ndjson`,
`persistent-summary.json`, `measure-persistent-verified.py`) used a private master
and disabled fresh-login fallback. Persistent Python status had median 83.99 ms,
maximum 206.20 ms; fresh SSH status had median 408.24 ms, maximum 1,330.82 ms.
All requests completed. The first harness failed on socket path length; the
second combined two master options and refused multiplex sessions, silently
falling back to fresh SSH. Neither is evidence of persistent performance.
`measure-persistent.py` preserves that invalid second harness for provenance.

The candidate uses an independently supervised foreground SSH master per worker,
with a private namespace separate from artifact transfer and no client fallback.
Long preparation stays on direct connections. Status budgets and current warm
publication validation are unchanged; mutations are not blindly retried.

The actual compiled control helper across all six workers completed 18/18 warm,
valid status reads: median 15.34 ms, maximum 71.56 ms (`probe.json`). This was a
read-only trial while production remained on direct SSH, not sustained query load.

Lifecycle testing exposed a second issue: an SSH master can retain a cancelled
client's output descriptors, extending a 100 ms timeout to 2.014 s (`lifecycle.json`).
Private temporary output files decouple process reaping from remote channel EOF.
The corrected live test (`lifecycle-file-output.json`) returned in 101.93 ms,
while a concurrent status read took 15.70 ms. Deliberately closing the trial
master recovered in 2.216 s. These deliberate disruptions affected only trial
connections; production still used direct SSH.

All 81 targeted operations tests pass, including missing sessions, no silent
fallback, stale and live predecessor sockets, supervisor cancellation/restart,
output descriptor cancellation and unchanged status/mutation handling.
`failed-postreorg-canary.tar.gz` preserves the full failed acceptance run.

## Acceptance

Operations commit `85d76e8` was deployed at 18:44 UTC. The script SHA-256 is
`33bccaaef1d57692630a1472860d1db7ca89d4ef2d4e47047924ba40d475f232`.
The permanent session unit owns all six connections; `control_sessions: true`
was enabled only after every connection attested warm valid status. Both public
metadata endpoints returned 200 afterward. `enable-sessions.py` records the
one-time switch and rollback procedure; `enable.log` is its actual output.
`make-check.log` records the full successful check (587 Rust tests, two ignored),
and `operations-tests.log` records all 81 targeted operations tests.

A fresh matching loaded canary began at 2026-09-10 **18:45:04 UTC** under
`transparent-m1-sessions-rollout.service` on the existing coordinator, with
worker `f2f351c` and outputs under
`/opt/transparent-publisher-build/sessions-20260910/rollout`.
The supervisor must pass both six hours and 300 new blocks, then the gated
six-worker rollout and 24-hour observation. The start capture establishes only
that the process and query clients were running; it is not a passing result.
Diagnostic probes and earlier failed runs do not satisfy those gates.

The 18:46:34 UTC start capture contains **1,083 exact queries and four retries**.
Both query clients and the supervisor were live. This is approximately 90 seconds
of operation, far short of the acceptance duration. Source/configuration/roster
digests and process identities are in `start-capture.tar.gz`.
