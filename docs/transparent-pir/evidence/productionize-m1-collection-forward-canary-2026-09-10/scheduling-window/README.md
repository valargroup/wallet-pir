# Candidate scheduling window, 22:09 UTC

Read-only capture on 2026-09-10 while acceptance supervisor PID 2395279
remained active. These files preserve raw records selected by UTC from
22:09:27 inclusive to 22:09:32 exclusive. They are a diagnostic interval,
not an acceptance result or an atomic snapshot.

Unix status took 1.003238 s and HTTP readiness took 1.002419 s for requests
starting at 22:09:28.815 UTC. Both completed successfully. Thread 649050 was
reported runnable with unchanged scheduling counters from 28.555 through
29.805; the runnable sampler recorded CPU 0 and an epoll kernel stack in two
samples. CPU 0 counters were unchanged between the 28.485 and 29.485 host
samples. Other CPUs advanced. Thread 649785 waited for dirty-page/writeback
work during this window. None of this establishes the host or kernel cause,
or proves the worker's source locks caused the delay.

The archive was read while probes appended; tar returned 1 with file-changed
warnings for threads.ndjson and http.ndjson. The selected historical interval
is earlier than that capture. Full local capture: /tmp/m1-candidate-probe-2210.tar.gz.
Its SHA-256 is recorded in capture.json. The ongoing remote raw files remain
under /run/tp-probe-candidate on recent-01. No configuration or acceptance
threshold was changed in response to this observation.
