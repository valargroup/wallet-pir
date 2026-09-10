# Completed bounded runnable-thread probe

The read-only probe ran on recent-01 from 2026-09-10 21:58:13 to 22:58:14 UTC
and exited successfully. Exact source is in ../early-probes/runnable-probe.py.
Systemd recorded 13.947 seconds CPU time and 7.7 MB peak memory. It did not
restart the worker or change acceptance settings. At 22:58:16 the fresh
post-reorg acceptance supervisor was still active, PID 2521008.

The complete finalized output contains 2,274 rows, from 21:58:17.494752 to
22:58:12.559217 UTC. The sampler emits only when it finds a runnable thread;
a missing row is not a failed sample. It reads task state, CPU, scheduling
counters and kernel stack separately, so these are not atomic task snapshots.

There are 73 pairs of observations of the same thread less than 0.8 seconds
apart with identical schedstat strings. Their later recorded CPU counts are
CPU0: 13, CPU1: 25, CPU2: 6, CPU3: 29. These counts describe repeated samples,
not distinct stalls, and do not measure total scheduler delay. They show that
the observed pattern is not isolated to one recorded CPU. Attribution to
application, kernel or virtual CPU behavior remains unproven. Selected
correlated endpoint windows are preserved in adjacent evidence directories.

The longer status/thread/pressure probe continues to its own two-hour limit.
This completed probe is diagnostic evidence and cannot satisfy acceptance.
