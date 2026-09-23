# Load warmup regression checks — September 23, 2026

The actual load-driver warmup accounting now uses a testable shared routine.
Two deterministic tests passed, covering accumulation of transport/429/503
failures alongside correct and incorrect answers, rejection of incorrect answers
in either phase, and propagation of authorization, server, malformed-response,
generation and coverage errors. Clippy passed for all load-driver targets.

The transport test constructs the real reqwest error variant using an invalid
URL without network access. It tests accounting of that error variant, not TCP
reset/reconnect behavior. The previous live sweep observed the original reset;
its successful rerun did not reproduce it. This distinction remains unproven.

See [tests](tests.log), [Clippy](clippy.log), and the
[prior live load results](../architecture-v4-latest-load-2026-09-23/README.md).
No additional live load campaign, deployment or qualification occurred here.
