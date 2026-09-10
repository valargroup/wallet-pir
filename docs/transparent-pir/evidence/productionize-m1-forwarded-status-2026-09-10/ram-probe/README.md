# Memory-backed worker measurements — 2026-09-10, 21:25 UTC

The first probes timed an asynchronous wrapper and wrote directly to the same
disk as worker snapshots. A delayed event-loop wakeup or log write could inflate
the apparent socket duration. This replacement separates those effects.

`probe.py` runs as `transparent-m1-ram-probe.service` on recent-01 for at most
one hour (systemd ceiling 3,700 seconds). Before startup, `findmnt` confirmed
that `/run` is tmpfs. Its four independent threads write to
`/run/tp-probe-ram/{unix,http,threads,pressure}.ndjson`. Unix and HTTP requests
measure duration through receipt inside the requesting thread; the record also
includes full function duration and the gap between sample starts. Thread and
host-pressure measurements use the same memory-backed output directory.

At 21:25:22.192623 UTC, the first Unix status request reported `ok: true`,
`warm: true`, socket duration 2.349 ms and total function duration 2.606 ms.
The unit was active with PID 635576. After that successful check, the old
worker-local `transparent-m1-stall-probe` and `transparent-m1-thread-probe` units
were stopped. Their files were preserved. The coordinator's diagnostic load
and remote comparisons continue; no serving configuration was changed.

Raw results and correlation are pending. This corrects measurement isolation;
it does not establish the cause of the original canary failure or M1 acceptance.
