# V4 second local load sweep

This run uses separate coordinator and worker processes on the same 128 GiB
macOS development machine as the load generator. It exercises a small synthetic
bootstrap dataset, concurrent publication, session refresh, and fixed-rate arrivals.
Each measured stage lasts 15 seconds. Release compilation and integration tests
were running on the same host: these numbers are not comparable production
capacity measurements and do not qualify 8 GiB workers.

| Stage | Correct | Incorrect | HTTP errors | All-request p99 ms | Scheduled p99 ms | Unstarted arrivals |
|---|---:|---:|---:|---:|---:|---:|
| load-c1 | 341 | 0 | 0 | 128.83 | 128.83 | 0 |
| load-c2 | 415 | 0 | 0 | 258.56 | 258.56 | 0 |
| load-c4 | 379 | 0 | 400 | 184.19 | 184.19 | 0 |
| load-c8 | 429 | 0 | 1274 | 128.57 | 128.57 | 0 |
| open-loop-0.5 | 205 | 0 | 1 | 145.15 | 146.05 | 0 |
| open-loop-0.75 | 269 | 0 | 41 | 172.67 | 174.46 | 0 |
| open-loop-1.0 | 324 | 0 | 89 | 172.54 | 175.36 | 0 |
| open-loop-1.25 | 338 | 0 | 179 | 159.74 | 163.84 | 0 |

All HTTP errors in this run are explicit 429 admission rejections. The open-loop
rates are fractions of the throughput measured in this run's concurrency-2 stage;
changing host contention means that rate is not a stable capacity estimate.
Even the 50% stage has a rejection, so these stages do **not** satisfy the
zero-error qualification gate. Wrong answers would fail the driver regardless
of its permissive characterization error threshold.

The command files, JSON reports, process logs, and manifest preserve the tested
binary hashes and inputs. Source review continued after the run, including
commit-notification recovery and retention synchronization; this is evidence
for the recorded binaries, not a blanket qualification of subsequent edits.

See the [implementation status](../../docs/architecture_2-implementation.md) for
remaining release gates. No production deployment or hardware soak was performed.
