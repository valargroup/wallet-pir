# Failure-path regression follow-up

Both review gaps are covered at the source revision in [manifest.json](manifest.json).
The two focused CPU tests and two explicitly invoked P4000 tests passed. The full
CUDA-feature library suite passed 70 tests, with three opt-in GPU tests ignored;
the two new ignored tests were run explicitly in the focused GPU pass. The earlier
encrypted hardware test is recorded in the parent evidence directory.

Preparation fails after the second new candidate kernel has allocated/uploaded its
database. The first candidate unit is already persisted/prepared. Fresh and cached
paths both verify candidate kernel cleanup, unchanged published runtime inventory
and answers, successful retry, and final reference release. Cached failure and
retry must never request canonical rows. GPU variants retain a real CUDA-backed
published evaluation while the candidate fails.

The worker HTTP test injects an evaluation error from a real-kernel wrapper and
writes a deliberately invalid partial output first. The response must be sanitized
HTTP 503. The same single-permit worker then rejects malformed length and modulus
coefficients as HTTP 400 without calling the backend, and successfully serves a
valid request after clearing the fault. Durable worker state must remain unchanged.

All fault wiring is test-only, with scoped thread-local construction and per-kernel
owned fault state. Production backend configuration and behavior are unchanged.
These tests do not force physical GPU exhaustion or recover a poisoned CUDA context.
Raw logs, commands and binary hashes are retained here; see [checksums](SHA256SUMS).
