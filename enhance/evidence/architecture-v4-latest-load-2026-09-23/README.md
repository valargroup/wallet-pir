# Updated candidate load validation — September 23, 2026

Fresh native release-fast binaries ran as separate coordinator/worker/load
processes on one macOS host. These small synthetic fixtures exercise current
code; they are not 8 GiB worker capacity or independent wallet qualification.

## Publication workload

The [workload report](workload/exercise.json) records 87.574 measured seconds,
10 publications, 9,165 correct background answers, zero background errors,
64 exact boundary/retained probes and six expired-generation refreshes.
Background p99 was 50.079 ms; maximum fixture publication was 9,261 ms.
Hardware host identity is null on macOS, and qualification remains unqualified.

## Load sweep

Each step measured 15 seconds after two seconds of warmup. The coordinator kept
publishing during the sweep. Open-loop rates were fractions of the measured
concurrency-two baseline. Correct QPS counts successful exact answers only.

| Step | Correct measured answers | Measured errors | Warmup errors | Correct QPS | Scheduled p99 ms |
|---|---:|---:|---:|---:|---:|
| load-c1 | 988 | 0 | 0 | 65.82 | 21.471 |
| load-c2 | 1588 | 0 | 0 | 105.83 | 42.911 |
| load-c4 | 1814 | 324 | 45 | 120.80 | 43.295 |
| load-c8 | 1850 | 1154 | 158 | 122.92 | 42.207 |
| open-loop-0.5 | 785 | 8 | 163 | 52.39 | 51.935 |
| open-loop-0.75 | 1160 | 30 | 161 | 77.34 | 55.263 |
| open-loop-1.0 | 1501 | 86 | 164 | 100.04 | 61.599 |
| open-loop-1.25 | 1807 | 177 | 154 | 120.37 | 66.495 |

All retry-sweep errors were HTTP 429. Across the eight steps there were 11,493
correct measured answers, no incorrect measured or warmup answers, and no
unstarted arrivals. Every open-loop step had errors, including the lowest offered
rate. No zero-error open-loop acceptance point is established by this sweep.
Higher-concurrency steps used permissive error thresholds for characterization.

## Failure found and correction

The [original sweep](initial-sweep.log) stopped at concurrency eight because a
warmup transport reset aborted the driver before it wrote a measurement report.
Its logs and earlier reports are retained under `sweep/`. The driver now counts
warmup transport and HTTP 429/503 errors explicitly, and checks warmup exact
answers too. Other protocol/client failures remain fatal; incorrect answers in
either phase fail the run. Warmup counters are separate from measured thresholds.

The [complete rerun](retry-sweep.log) passed characterization with the updated
driver; transport reset did not recur in that run, so it does not independently
prove the transport-error branch. [Clippy](clippy.log) passed with warnings denied.
Owned processes and temporary data were cleaned up by the runners.

These are short shared-host measurements with fixture oracles. They do not prove
full-size assignment capacity, resident-memory guard, six-hour qualification,
canonical wallet conformance, deployed recovery, or production readiness.
