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
