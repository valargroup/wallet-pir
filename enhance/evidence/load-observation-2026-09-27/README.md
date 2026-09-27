# Concurrent Enhance and Status query load

Ran on deployed main commit `13ec1fbe875254d45ba2b37c99b7eb55a517f56d`,
2026-09-27 05:28:48–05:33:24 UTC (09:28–09:33 Dubai).
User requested simulated query load across services to observe alert triggers.
Transparent PIR remained excluded. No serving configuration or alert thresholds
changed, and the ongoing shadow observation was not restarted.

| Workload | Duration | Correct / offered | Scheduled-to-completed p99 |
| --- | --- | --- | --- |
| Enhance public HTTPS, 5 QPS | 60 seconds | 300 / 300 | 112.319 ms |
| Enhance public HTTPS, 20 QPS | 180 seconds | 3600 / 3600 | 148.863 ms |
| Status private direct router, 20 QPS | 270 seconds | 5400 / 5400 | 61 ms |

Status ran concurrently with both Enhance stages. All 9,300 measured queries
were correct, with zero errors, incorrect answers or unstarted arrivals.
Enhance additionally reported 16 correct setup/warmup answers outside the
measured counts. Enhance used the independently extracted canonical production
oracle; Status used its independent canonical-mined oracle. This is bounded
load validation, not saturation testing or full workload qualification.

The harness had a 360-second systemd limit, 2 GB memory limit, three-core CPU
quota, and stopped load if the independent external canary failed or monitoring
became unavailable. It exited successfully with all three child probes exiting
zero. No load generator remains running after completion.

Twenty-eight aggregate samples (~10-second intervals) recorded:

- No active or firing alert conditions, no unknown checks, and no changed
  incident IDs relative to the initial sample.
- Healthy APM and external-monitor progress throughout; all observed external
  canary results passed.
- No sampled Status router queue buildup (maximum waiting = 0).
- A subsequent post-load check also showed healthy progress and no active or
  firing conditions. Exact timestamp and full aggregate are in `post-load.json`.

No load-induced new-rule notifications were generated. New rules remain in
shadow; legacy alerts remain enabled. This test did not exercise a firing /
recovery transition or revalidate Slack delivery.

## Coverage limitation

The current evaluator's query latency/error rules cover Enhance init/query
entrypoints. Status has role telemetry and load-test correctness validation,
but no equivalent dedicated query latency/error rules in this evaluator.
Consequently, this test demonstrates Status behavior under load, not complete
Status alert coverage. Adding dedicated Status rules is a follow-up improvement.

The unchanged Enhance query warning threshold is processing p99 > 5 seconds
for 120 seconds, with at least 20 samples. The 5xx critical threshold is > 5%
over five minutes, with at least 10 requests. Client end-to-end latency above
includes a different measurement boundary from the server alert histogram.
Observed performance stayed below the thresholds; no fault injection or
threshold reduction was used to manufacture an alert.

Detailed reports and samples are retained here and on the coordinator under
`/root/load-observation-20260927T0528/`. The independent ongoing 72-hour observer
continues under `pir-observability-evidence-13ec1fb.service`.
