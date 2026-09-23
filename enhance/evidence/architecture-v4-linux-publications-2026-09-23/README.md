# Linux publication-under-load campaign

This campaign uses the unchanged x86-64-v3 binaries from the
[Linux build](../architecture-v4-linux-build-2026-09-23/README.md), launched through
QEMU 10.2.3 on the local ARM64 Docker host. It requires at least two completed
publications between load-campaign start and finish, with 90-second concurrency
steps at 1 and 2. Exact synthetic answers are checked by the load driver.

The campaign passed: generation 1 advanced to generation 4, completing
3 publications during the campaign. The two 90-second steps returned 1,336
correct measured answers (524 at concurrency 1 and 812 at concurrency 2), with
zero incorrect answers, request errors, or unstarted arrivals. Warmup returned
33 correct answers and no errors. Both step-end health snapshots had no blocked
reason or pending commit/abort notifications. Raw reports are in `campaign/`.

This run checks exact synthetic answers during completed publications. It does
not separately force replica failure, retention expiry, or full-size assignments.
The wrappers, emulator and actual ELF identities are recorded in the build report;
smoke-manifest binary hashes identify wrappers, not ELF bytes. This is functional
evidence under emulation, not native throughput, latency or memory qualification.
