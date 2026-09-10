# Second candidate scheduling window, 22:27 UTC

Captured at 2026-09-10 22:28 UTC while canary supervisor PID 2395279 was
verified active with no terminal canary result. The exact read-only selector
and JSON records from five probes are preserved here. The interval starts
22:27:16 inclusive and ends 22:27:21 exclusive; files were read sequentially
and the capture is not atomic. This is diagnostic evidence, not acceptance.

Unix and HTTP requests beginning at 22:27:17.227 and 17.229 took 2.029503 s
and 2.027882 s respectively, both successful. Two threads, 649050 and 649298,
were recorded runnable on CPU 1 with unchanged scheduling counters across
four half-second samples. Their sampled kernel stacks were in futex waits.
CPU 1 counters were unchanged between 17.920 and 18.920 while other CPUs
continued advancing. Thread 649054 was blocked in writeback. State and stack
reads are separate observations, not an atomic task snapshot.

This repeats the association among a local endpoint pause, stalled scheduling
counters and disk writeback seen in the earlier window. It does not establish
whether kernel scheduling, virtual CPU behavior, application synchronization
or their interaction is responsible. No timeout or fleet setting was changed;
the existing acceptance supervisor remained active after the pause.
