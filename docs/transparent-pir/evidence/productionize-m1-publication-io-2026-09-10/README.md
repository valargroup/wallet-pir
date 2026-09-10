# Publication I/O investigation — 2026-09-10

This is diagnostic evidence, not M1 acceptance. Worker recent-01 remained on
`d8f5217` (binary `fee415d93ad15dbd18612777c9d05b065ed71f5214d1bb86aefe6bb9b3fec3c5`),
with operations `eb116e9`. No acceptance supervisor or rollout was started.

## Live trace

The bounded `transparent-m1-scheduler-trace-v2.service` completed successfully
on recent-01, PID 649049, after 900 seconds. Kernel and thread identity are in
[trace-context.txt](trace-context.txt). Exact trace programs and diagnostic load
driver are adjacent. [summary.json](summary.json) summarizes the immutable
[probe archive](m1-publication-io-probes.tar.gz) and
[query archive](m1-publication-io-load.tar.gz).

The v2 trace captured 13 fsync calls over 100 ms, maximum 602.111 ms. One call
spent **379.788 ms in fsync on TID 649051**, whose comm was `tokio-rt-worker`.
Source inspection identifies synchronous publication persistence inside the
async activation and invalidation handlers. The trace establishes that disk
sync occurred on an executor thread; it does not establish that this call
caused the earlier four-second endpoint timeouts.

V2 recorded no runnable wait above 100 ms for the tracked threads. It seeds
nine initial worker threads and tracks their descendants; existing blocking
pool threads outside that seed may be absent from scheduling coverage. Fsync
coverage filters the process id and includes all its threads. V1 was stopped
after finding that its `prev_state == 0` predicate missed preemption state 256;
v2 uses the low eight state bits. Neither absence of a v2 event nor these short
windows rules out a scheduling or host issue.

The two direct worker query clients ran from 23:36:13 to 23:46:13 UTC, completing
**9,157 exact queries, one retry and zero mismatches**. Both exited zero. This
was one shard, two clients, ten minutes, direct private HTTP from the coordinator;
it is not the Amsterdam qualification or public acceptance workload.

The separate two-hour endpoint probe completed at 23:43:06 UTC. Its final data
includes 14,308 Unix samples (maximum 3.922711 s) and 14,271 HTTP samples, including
four four-second timeouts. The last timeout was at 23:24:54 UTC, before v2
started. Do not correlate those earlier timeouts with the later fsync events as
though they were the same incident.

## Source correction and regression

Activation now owns the mutation guard inside a blocking job, including its
candidate validation, durable write and active swap. Cancelling the control
caller does not release serialization while that job continues. Invalidation
also runs off the executor; it releases serving locks after publishing the
in-memory revocation and before disk persistence, while retaining the publication
gate until persistence finishes. It still acknowledges only after successful
persistence. Revoked requests remain refused during the blocked write.

Two single-thread executor regressions block the real publication temporary-file
open using a FIFO. An independent thread releases it after at most three seconds,
so the old implementation fails instead of hanging the suite. Both regressions
fail against unchanged `8f8b9b9` source, while the existing blocked-collection
regression passes ([baseline log](m1-publication-baseline-tests.log)). All three
pass with the correction ([corrected log](m1-publication-tests.log)). The tests
also check the old active identity during the blocked interval, immediate
revocation refusal, and activation serialization after caller cancellation.
FIFO fsync behavior differs by platform; no assertion relies on it failing.

Full `make check` passed: 590 Rust tests, zero failures, two ignored, with
formatting, Clippy, operations, documentation and report checks. The Linux build
and targeted blocked-write, activation/reorg, memory and disk-cache tests passed.
The [artifact digests](artifact-sha256.json), [Linux tests](linux-tests.log),
[build record](linux-build.txt) and [full local check log](m1-publication-io-make-check.log)
are preserved alongside the exact source-file manifest.

The correction is **not deployed**. Amsterdam qualification must precede
deployment and a fresh matching acceptance run.
The underlying multi-second incident is not yet conclusively explained.
