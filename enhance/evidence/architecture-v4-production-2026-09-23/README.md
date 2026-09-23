# Direct production deployment and validation

The user explicitly authorized direct SSH deployment on existing production
hosts, stopping legacy Enhance services, bypassing CI where possible, and no
new hosts. This supersedes the earlier isolated-host deployment gates; it does
not establish hardware qualification.

The clean candidate `9718a6dcf9801385f69f31bb71f02efd261914f0` was staged and
checksum-verified on the coordinator and both c-4 production workers. Legacy
binaries, configuration and data remain available for rollback. Legacy Enhance
services, autoscaling and APM are disabled. V4 uses worker private port 8091 and
the existing Caddy origin on coordinator port 8080.

The initial journal copy was rejected because production used 29 records per row.
The explicit offline migration preserved all 565,944 records and block entries,
changing packing metadata to 33 records per row. Both records files had SHA-256
`5ab51089db27257ed24175a7e117889456b6ca55f95e40151b5b9b7370863a8f`.
V4 then reconciled live RPC data and published canonical generations on both peers.

Four native load cases passed with 1,870 correct measured answers and no wrong
answers/request errors: public HTTPS, private concurrency 1 and 2, and open-loop
2 QPS. The public test ran from the coordinator through the public HTTPS origin;
a separate external IPv4 health request also succeeded. Oracle records came from
the preserved legacy canonical journal, independent of v4 evaluation but not an
independent chain extractor. Reports retain all latency and warmup counts.

One-replica-offline and coordinator-restart checks added 329 correct measured
answers with zero errors. Independent worker sampling recorded no swap and no
collection errors at the captured points. These short checks are not full-size
hardware qualification.

The active-profile campaign completed 21,657.284 measured seconds and 320
publications. It recorded 225,939 correct background answers, zero background
errors, 13,006 exact boundary/retained probes and 316 expired-session refreshes.
Background query p99 was 391.935 ms. Maximum fixture publication was 92.004 s;
that timer includes synthetic journal changes and publication, not just PIR
preprocessing. See `completed/active/exercise.json` and `publications.jsonl`.

This is not full hardware qualification. Worker 1's sampler was stopped at
06:51:47 UTC; its restart failed because the output directory already existed.
Sampling resumed in a separate directory at 09:50:33 UTC. The missing interval
cannot be reconstructed. Worker 2 recorded the full period. Sampled RSS plus
kernel peaks were 5.873 GiB and 5.296 GiB; recorded samples had no worker swap or
OOM events. `completed/sampling-summary.json` records intervals, raw-file hashes
and collection manifests. Raw samples remain in the recorded host paths.

The campaign finished at 10:30:57 UTC. Automatic restoration failed because the
supervisor attempted to stop a transient unit that systemd had already unloaded.
The supervisor was corrected to accept an absent unit only with MainPID zero;
canonical worker state was restored and the coordinator started at 11:22:22 UTC.
The original restoration error is retained in `completed/restoration-error.json`.
Canonical catch-up and public serving then recovered. All four post-restoration
load cases passed: 1,893 measured exact answers, zero wrong answers or errors.
Their reports and a later replicated canonical health snapshot are in `completed/`.
Synthetic worker state is preserved as `worker.active-completed`; live canonical
state is back at `worker`. Legacy services and automatic provisioning remain off.

Implementation and cleanup were pushed to main at
`436dcc7efda3e09a6734342fd4f55e07bf1d9d95`; this evidence update follows it.
Sealed-role, full-size overload, uninterrupted worker-1 qualification and legacy
rollback rehearsal remain outstanding.

The deployed canonical peer-row repair rehearsal passed. The CLI refused repair
while worker 1 held its live lock. After stopping it, one 8K-row artifact was
moved aside and restored from a checksum-verified worker-2 copy. The durable
journal hash was unchanged. A second repair restored zero units. After restart,
worker 2 was stopped to force exact-answer traffic through the repaired replica:
218 measured answers and 23 warmup answers were correct, with zero errors and
148.223 ms measured p99. Both replicas were restarted; the peer caught up and
generation 50 was published on both replicas. See
`completed/repair-rehearsal.json`, `completed/after-repair-health.json` and
`completed/after-repair-replicated-health.json`. This
proves this canonical missing-row recovery case, not all corruption or full-size
recovery scenarios. The preserved original and peer copy remain on worker 1 at
`/srv/enhance-pir-v4/validation/repair-20260923`.
