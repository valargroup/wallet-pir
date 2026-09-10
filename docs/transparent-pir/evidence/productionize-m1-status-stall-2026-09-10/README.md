# M1 persistent-session status stall — 2026-09-10

The `85d76e8` operations / `f2f351c` worker acceptance canary failed at
20:21:53.823937 UTC after 5,808.900 seconds and 76 new blocks on HTTP 503.
It completed 82,406 exact queries and recorded 127 retries. Maximum measured
public visibility before failure was 26.090 seconds; minimum sampled host memory
headroom was 25.12%, with no recorded worker restart or OOM. These measurements
do not establish acceptance. The complete terminal run is in
`failed-session-canary.tar.gz`.

Two owned-session status attempts timed out at 20:21:49 and 20:21:51. Membership
was withdrawn at 20:21:51 and restored at 20:21:55. The persistent master service
did not restart. Connection setup improvements did not eliminate the stall.
The logs do not yet distinguish worker lock contention, SSH channel execution,
or host/network scheduling. No fleet promotion occurred.

## Diagnostic execution path

1. Preserve the complete failed run and coordinator/worker logs (included here).
2. Collect concurrent local Unix status, local HTTP readiness, remote HTTP
   readiness, owned-session SSH no-op and actual control status, plus worker
   CPU/memory/I/O pressure. `stall-probe.py` performs this for at most two hours.
3. Run two exact query clients under the existing observer for up to two hours,
   with the original freshness/resource limits. Correlate any stall with the
   independent probe channels before choosing the correction.
4. Reproduce the implicated mechanism, implement and test the correction, then
   restart the full matching acceptance sequence. Diagnostic duration or passing
   short runs cannot substitute for six hours/300 blocks and fleet observation.

At startup, `transparent-m1-stall-probe.service` and
`transparent-m1-stall-load.service` were active on the coordinator. The same
probe unit name runs on recent-01 for local measurements. Output paths:

- Coordinator: `/opt/transparent-publisher-build/sessions-20260910/stall-probe.ndjson`.
- Worker: `/tmp/m1-status-stall-worker.ndjson`.
- Bounded load: `/opt/transparent-publisher-build/sessions-20260910/stall-load`.

The load observer uses `--seconds 7200 --blocks 0` for diagnosis and has no
rollout supervisor. This is explicitly not a replacement acceptance gate.
