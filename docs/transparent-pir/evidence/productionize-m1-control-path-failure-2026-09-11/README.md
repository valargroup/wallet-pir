# Control-path failure — 2026-09-11

The `043c051` canary failed at **00:08:24.568863 UTC**, after 421.021 seconds,
nine new blocks and 5,705 exact queries (five retries, no mismatches). Its
terminal reason is `public routing is unavailable`. The supervisor is terminal
failed; no full-fleet promotion occurred and no replacement gate has started.
The [complete failed run](m1-publication-io-failed.tar.gz) is preserved.

The [coordinator journal](m1-publication-io-failure-journal.txt) records status
transport timeouts at 00:08:22 and 00:08:24, membership removal at 00:08:24,
a subsequent pair of timeouts, then restoration at 00:08:28. The routing audit
counter advanced from baseline 3 to 4 and returned to availability true. This
is the new audit correctly rejecting a withdrawal; it is not a green run whose
outage was missed by periodic sampling. There was no control-session service
journal entry in the incident window.

The [worker-local window](m1-publication-local-failure-window.json), covering
00:08:18–00:08:31, contains 26 Unix-status samples and 26 HTTP-readiness samples,
all successful. Maximum durations were **1.664 ms Unix** and **6.974 ms HTTP**.
This rules out a continuously blocked local worker throughout those sampled
coordinator timeouts. It does not establish every request's behavior between
samples or identify the precise coordinator/SSH failure mechanism.

## Next diagnostic

The live reconciler still performs synchronous durable writes on its async
thread, including a one-second heartbeat. This is a candidate explanation for
coordinator-side timeouts, not a confirmed cause of this incident. A bounded
900-second BPF trace filters fsync calls on reconciler PID 2594424 and records
calls above 10 ms. Unit `transparent-m1-reconciler-fsync.service`, observed
PID 2607970, writes `/run/transparent-reconciler-probe/fsync.ndjson` on the
coordinator. Trace start monotonic_ns is 738949705961807; stderr initially
reported successful attachment of five probes. Source is adjacent.

An independent two-thread probe reads the same forwarded Unix status socket
and direct private HTTP readiness every 0.5 seconds for one hour. Unit
`transparent-m1-control-path-probe.service`, observed PID 2608079, writes
`/run/transparent-control-path-probe/` on the coordinator. Its source is adjacent.
Worker-local probes remain active for correlation. These read-only diagnostics
are not acceptance tests and do not modify the running worker or fleet source.

Compare independent forwarding with direct HTTP and worker-local timings, and
correlate any reconciler-thread fsync delay before selecting the next fix. Do
not weaken health-check timeouts or restart acceptance to seek a lucky run.

At 00:13:58 UTC, a dedicated status-only SSH connection was also active as
`transparent-m1-dedicated-status-tunnel.service` (PID 2608617), with forwarding
through root-only `/run/transparent-reconciler-probe/dedicated.sock`. It uses the
existing deploy key and known-hosts file, strict host checking, no multiplexing,
and a 3,700-second lifetime. `transparent-m1-dedicated-control-probe.service`
(PID 2608713) samples it separately every 0.5 seconds. Compare it with the
existing status connection shared with mutation commands; this is diagnostic
only and no production client has switched to the dedicated tunnel.

A ten-minute two-client diagnostic load is active as
`transparent-m1-control-path-load.service` (PID 2608879), using the installed
candidate's soak-query artifact against the private worker. Logs are under
`/opt/transparent-publisher-build/publication-io-20260910/control-path-load`.
The first observed queries are exact. This restores query load for the transport
comparison without restarting acceptance or enabling promotion. The fsync trace
had no over-10-ms event at this capture; that is not proof that persistence can
never delay the reconciler.

## Readiness follow-up, 00:18 UTC

[Independent control samples](readiness-followup/control-summary.json) captured
shared forwarding at 3.636 seconds and direct HTTP at 2.323 seconds around
00:11:59. A later direct HTTP timeout at 00:13:06 coincides with a
[worker-local HTTP timeout](readiness-followup/worker-window.json). Local Unix
status remained responsive in that later window. Therefore SSH alone cannot
explain all observed delays. This does not undo the distinct 00:08 incident,
whose worker-local samples were fast while coordinator status timed out.

The dedicated tunnel's maximum was 368 ms at capture, but it started after the
largest shared-path incident. Those maxima are not a matched comparison. The
reconciler fsync trace remained active with no over-10-ms event; asynchronous
persistence remains a source concern, not an established incident cause.

Two additional 900-second diagnostics are running: raw HTTP probes separate
connect, send, first byte and total response time on the coordinator and worker
(`transparent-m1-tcp-stages.service`, PIDs 2616194 / 684357, logs in
`/run/tp-tcp-stages` on each host). A 49-Hz CPU profile records sampling gaps over
200 ms on the worker (`transparent-m1-cpu-gaps-v2.service`, PID 684246, logs in
`/run/tp-publication-io-probe/cpu-gaps-v2.*`). V2 resets the comparison across
observed idle transitions; v1 was stopped early to add that exclusion. Neither
profile gaps nor their absence alone establishes a hypervisor or kernel fault.
Bpftrace reported a signed/unsigned arithmetic warning in v2; preserve and
inspect raw timestamps before interpreting any event. The exact scripts are
in [readiness-followup](readiness-followup/).

## CPU sampling gaps, 00:25 UTC

The [completed v2 trace](cpu-gap-followup/m1-cpu-gaps-v2-final.tar.gz) recorded
four non-idle sampling gaps: 4.045 s on CPU 3, 1.720 s on CPU 2, 0.863 s on CPU 1,
and 2.015 s on CPU 3. The first gap's preceding TID 679430 was independently
identified as a `tokio-rt-worker` in process 679049; later gap endpoints were
kernel worker threads. These are sampling gaps, not proof of the particular
kernel/host mechanism or proof that each gap caused an endpoint outage.

V2 was stopped after the stack-enabled replacement was confirmed running.
`transparent-m1-cpu-gaps-v3.service` (PID 685938) records prior/current kernel
stacks at gaps, uses an unsigned timestamp map and retains the idle exclusion.
It runs for 900 seconds from worker monotonic_ns 242441618559158 and writes
`/run/tp-publication-io-probe/cpu-gaps-v3.*`. A second 900-second trace,
`transparent-m1-worker-advice.service` (PID 686140), times worker `madvise` and
`fadvise64` calls above 100 ms to test a memory/cache-reclamation hypothesis.
Its start monotonic_ns is 242526996487300; output is in `worker-advice.*` beside
the CPU trace. Both scripts are preserved in [cpu-gap-followup](cpu-gap-followup/).

The [first ten-minute load](cpu-gap-followup/control-path-load-final.tar.gz)
finished successfully: 8,579 exact queries, six retries, both clients exited
zero. A distinct fifteen-minute diagnostic load started as
`transparent-m1-control-path-load-2.service` (PID 2628176), recording under
`/opt/transparent-publisher-build/publication-io-20260910/control-path-load-2`.
It keeps query traffic present for the new traces. No acceptance supervisor was
restarted, and no deployment setting changed.
