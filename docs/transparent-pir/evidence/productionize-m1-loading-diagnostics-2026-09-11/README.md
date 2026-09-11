# M1 publication loading diagnostics

The [failed cancellation canary](../productionize-m1-cancel-prepare-2026-09-11/README.md)
spent 58.918 seconds loading a candidate on recent-01. Source
`8e3de82430c172c2c01af86414af2133fce20f26` adds task-queue, eviction, shard-load and
service-construction timings, plus warnings for Linux cache-advice calls taking
at least 100 ms. It does not change cache policy or integrity checks.

The [deployment bundle](deployment.tar.gz) records hashes of all 321 source files,
the matching source commit, build and deployment scripts, Linux test results and
the guarded recent-01 upgrade result. Worker SHA256 is
`ddd0259af5b0893e60cecb34fd62cacf03b797788ea9387b37a470d392a67f05`.
All 36 Linux worker library tests passed. Local [make check](make-check.log)
passed 591 Rust tests with zero failures and two ignored, along with its other
required checks. The [final Clippy check](final-clippy.log) also passed.
The guarded upgrade completed successfully at approximately 13:25:45 UTC.

A bounded 30-minute diagnostic began at approximately 13:28:12 UTC under
`transparent-m1-loading-diagnostic.service`, PID 3750915, on the coordinator.
It uses two private-query clients, the unchanged 30-second public and 60-second
replica budgets, and output directory
`/opt/transparent-publisher-build/loading-diagnostics-20260911/diagnostic`.
The [status helper](diagnostic-status.py) targets this exact run. It has no M1
acceptance credit and does not trigger fleet rollout.

The [thread sampler](thread-sampler.py) runs for 30 minutes on recent-01 as
`transparent-m1-loading-fd-sampler.service`, PID 857046, writing
`/tmp/m1-loading-fd-samples.ndjson`. It records blocked thread stacks and paths
for read, write, pread, fsync, fdatasync and fadvise syscalls. Early samples show
table reads waiting in the file-page path and cache writes awaiting journal
commits. A cache-advice call took 216 ms; this does not establish the cause of
the earlier 59-second loading stall. The diagnostic completed successfully; terminal results follow below.

At 2026-09-11T13:40:46.007824+00:00, the [checkpoint](checkpoint.json) confirmed the same live PID
after 753.5 seconds, with 10,374 exact responses and zero
mismatches. The [partial thread samples](thread-samples-checkpoint.ndjson) contain
704 samples, 19 with blocked threads. The [worker log](worker-checkpoint.log)
shows subsecond cache-advice delays and ordinary loading around one second;
it does not reproduce or explain the earlier 59-second stall. These files are
intermediate snapshots, not terminal diagnostic evidence.

## Terminal diagnostic result

The observer exited successfully at 2026-09-11 13:58:12 UTC after 1,800.388 seconds.
The [full diagnostic bundle](diagnostic.tar.gz) contains the terminal result,
all 60 worker samples and both query streams: 25,298 exact responses, 19 retries,
zero mismatches, and 26 public/replica blocks. Maximum public visibility was
19.591 seconds and replica visibility 18.626 seconds. Available host RAM never
fell below 26.989%; routing availability remained unchanged from the baseline.

The [complete thread samples](thread-samples.ndjson) and [worker journal](worker.log)
cover this diagnostic. Background cache writes waited on storage after preparation
finished. This does not establish the cause of the previous loading stall.
The 30-minute window did not reproduce that stall and earns no M1 acceptance
credit. The next investigation needs a longer instrumented observation, since the
prior failure occurred after approximately 110 minutes. No fleet rollout was triggered.
