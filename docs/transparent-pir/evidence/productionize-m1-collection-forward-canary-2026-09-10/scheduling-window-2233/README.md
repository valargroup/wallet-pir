# Candidate scheduling window, 22:33 UTC

Captured at 2026-09-10 22:37 UTC after the canary supervisor was verified active
at 22:36:43, with 48 new blocks and no terminal result. The exact read-only
selector and five probe streams are preserved here. The interval starts
22:33:53 inclusive and ends 22:33:59 exclusive. Reads are sequential and this
is not an atomic snapshot or an acceptance result.

Unix status and HTTP readiness requests starting at 22:33:54.070 and 54.072
took 3.156696 s and 3.155626 s respectively. Both completed successfully.
CPU 3 counters were unchanged across host samples at 54.269, 55.269 and
56.272, while other CPUs advanced. Thread 649785 was reported runnable on
CPU 3 with unchanged scheduling counters and a futex kernel stack in samples
during that interval. Other runnable threads continued accumulating CPU time.

This extends the observed maximum beyond the previous two-second pause.
It does not establish a complete cause, nor show that every pause comes from
the same operation. Probe success means completion before its four-second
timeout, not compliance with the tighter control-plane timing policy. The
acceptance supervisor remained active when checked; no timing threshold,
worker configuration or acceptance workload was changed.
