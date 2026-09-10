# Early candidate diagnostic probes

Captured 2026-09-10 at approximately 21:57 UTC while the matching canary
supervisor was active. This is not an acceptance result.

The memory-backed probe recorded 1,687 Unix and 1,687 HTTP samples with no
explicit errors. Worst observed socket durations were 1.536 s (Unix) and
1.394 s (HTTP), both starting at 21:55:12 UTC. These are genuine pauses, not
proof of their cause. Both requests completed successfully.

During that window thread 649292 was blocked in `ext4_sync_file`/`fsync`;
thread 649785 was runnable with unchanged scheduler counters across several
samples. CPU1 counters also stopped advancing temporarily while other CPU
counters continued. Correlation does not establish why the runnable thread
made no progress, or which source operation explains the pause.

Worker unit inspection reported unlimited CPU quota and no configured
AllowedCPUs or CPUAffinity. The worker cgroup has no cpu.max file; this is not
a complete audit of ancestor scheduling or hypervisor behavior.

`candidate-2157.tar.gz` contains the raw Unix, HTTP, thread and pressure logs.
It was captured while appenders remained live: tar returned 1 with a
file-changed warning for threads.ndjson. The readable archive is a partial,
non-atomic snapshot; it is not the terminal probe result.

To distinguish runnable-thread kernel activity from blocked I/O, a separate
one-hour, twice-per-second read-only sampler was started under
`transparent-m1-runnable-probe.service`. Its exact source is preserved here.
It records runnable-thread kernel stacks, CPU identity and scheduling counters
in `/run/tp-probe-candidate/runnable.ndjson`. No worker, supervisor, timeout,
CPU setting or acceptance workload was restarted or changed. Its added probe
load is diagnostic and must be considered when interpreting host measurements.
