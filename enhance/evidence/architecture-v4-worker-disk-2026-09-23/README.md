# Per-worker disk sizing evidence

Two current full-size HTTP tests passed in 394.20 seconds on macOS: four-worker
consolidation with a failed reservation and canonical retry, plus two-worker
loan/return with retained queries and reorgs. A dedicated temporary directory was
sampled every second for logical and allocated file bytes, separated by worker
and coordinator directories. It was removed by the test harness after completion.

Maximum sampled worker logical size: 15,453,517,399 bytes (14.392 GiB). Loan/return
workers peaked at 5,824,618,378 bytes (5.425 GiB). Coordinator journal/control
storage is excluded from worker sizing. `results.json` binds the test executable
and v4 server source hashes; `samples.jsonl` retains the measurements.

The initial single-worker bootstrap floor is now 32 GiB free, rather than the
64 GiB recommendation for the local multi-worker integration host. This preserves
the production c-4 profile (50 GB root disk). Bootstrap measures the worker data
filesystem, including a separate mount when present. The bootstrap and pair-driver
suite passed 19 tests with one environment-specific skip.

These are sampled integration-test footprints, not worst-case proofs or hardware
qualification. Sampling can miss brief peaks and macOS allocation differs from
Linux. Confirm actual free space after packages/swap and review disk-free trends,
retained artifacts, and reclamation throughout the six-hour native campaigns.
