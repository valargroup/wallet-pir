# Bounded query memory wait — 2026-09-09

Source `f2f351c` makes verified runtimes available before optional snapshot
persistence completes and gives already-admitted queries a cancellable memory
wait bounded by 250 ms and their remaining request deadline. The shared memory
ceiling and query concurrency limit are unchanged.

The focused two-slot run on the existing Amsterdam generator completed in
8.808 s, with 77 exact queries, zero retries and a 1.109 s maximum completion gap.
Three qualification repetitions use the same frozen 14-shard assignment, four
worker CPUs, separate external client CPUs and chunked-split backend. Results
are in `comparison.json`; run `python3 summarize.py` to reproduce the screening
decision. The generator is not equivalent to the older live worker hardware.

Initial and final persistence drains must both complete; memory sampling
continues through the final drain. Client records are drained before reporting.
No retry or completion-gap threshold has been relaxed from the recorded reference.

Validation: `make check` passed 587 Rust tests with zero failures and two ignored
manual benchmarks. All 71 transparent operations tests passed. Linux memory
admission and snapshot integration tests passed. `source.tar.gz` matches all
manifest source hashes; its base source label predates this source commit, but
its file hashes exactly match the committed implementation.

Test executable SHA-256:
`3344bf683fc21771234d47405021dd4d03a7e73f9bccff867fb9c59052ed6592`.
Default automatic-backend worker SHA-256:
`c6f169a3bdbd0cc5f110309ac70ee089b9336b507f46493b8fe80aaa51861bea`.
The live target selects chunked-split automatically because it lacks AVX-512F.

M1 remains open until a matching loaded canary completes six hours and 300 new
blocks, followed by the gated fleet rollout and 24-hour observation.


## Completed generator qualification

| Repetition | Visibility | Exact queries | Retries | Maximum completion gap |
|---|---|---|---|---|
| 1 | 9.721 s | 92 | 0 | 1.236 s |
| 2 | 8.856 s | 63 | 2 | 1.172 s |
| 3 | 8.933 s | 78 | 0 | 1.190 s |

All three combined screens passed, including persistence, memory, exactness,
retry fraction and completion gaps. Modeled host headroom was at least 27.0%;
there were no high/max/OOM events. This qualifies the candidate for a fresh
live canary, not for fleet promotion by itself.

## Live canary handoff

Recent-01 restored 28/28 runtimes in 10.296 s and passed 43 exact maintenance
queries with the default worker binary above. Public service reopened before
the canary began at **2026-09-09 20:56:02 UTC**. The coordinator unit
`transparent-m1-querywait-rollout.service` was active in `canary_observation` at
the capture. Its live directory is
`/opt/transparent-publisher-build/querywait-20260909/rollout`.
`canary-start/` is a dated start capture, not final acceptance. The other five
worker binaries remain unchanged until the matching gate passes.
