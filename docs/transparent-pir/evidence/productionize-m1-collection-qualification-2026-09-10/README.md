# M1 collection correction qualification — 2026-09-10

Worker source `d8f5217` matches all 233 input hashes in the preceding
[collection-lock evidence](../productionize-m1-collection-lock-2026-09-10/README.md).
The compatible Linux build and all three Amsterdam qualification repetitions
completed successfully. This is candidate qualification, not M1 acceptance.

| Repetition | Publication visibility | Exact queries | Retries | Maximum completion gap |
|---|---|---|---|---|
| 1 | 8.198 s | 59 | 0 | 1.237 s |
| 2 | 8.355 s | 62 | 1 | 1.259 s |
| 3 | 9.140 s | 72 | 2 | 1.271 s |

All three pass the unchanged combined screen, including the 14-second worker
budget, memory, persistence, external-client isolation, exactness, baseline
retry fraction and baseline completion-gap limits. Minimum modeled host memory
headroom was 26.08%. Run `python3 summarize.py` to rebuild `comparison.json`
from the raw qualification records and the original baseline evidence.
The Amsterdam generator is not equivalent to the older live worker hardware.

`linux-build.tar.gz` preserves build metadata and Linux test logs: the blocked
collection regression, seven memory tests and two disk-cache tests passed.
The initial driver accidentally copied a normal binary over the library test
runner and stopped on an unexpected CLI argument. `build-resume.py` selects
Cargo's library-test metadata explicitly and completed the remaining checks and
worker build; the original failed driver/log is preserved, not counted as a
worker assertion failure. Compatibility flags remain `target-cpu=x86-64-v3`
and `target-feature=+pclmulqdq`. The generator uses `portable-kernel` to match
the live worker's non-AVX-512 execution path.

Default worker SHA-256:
`fee415d93ad15dbd18612777c9d05b065ed71f5214d1bb86aefe6bb9b3fec3c5`.
All artifact hashes are in `artifact-sha256.json`; coordinator artifacts are at
`/opt/transparent-publisher-build/collection-20260910/artifacts`.
Generator results are at `/opt/transparent-collection-20260910/qualification`.

## Live diagnosis remains open

The original failure followed activation without a new collection recorded in
that window. The reproduced collection defect therefore does not yet establish
the cause of that failure. The old `f2f351c` worker remains deployed; no new
acceptance canary or fleet promotion has begun.

`thread-probe.py` now runs as `transparent-m1-thread-probe.service` on recent-01
for at most 90 minutes, alongside the existing bounded status/load diagnosis.
It records thread state, kernel wait locations, scheduler counters and kernel
stacks for disk waits every 250 ms, without changing serving state. Output is
`/tmp/m1-thread-probe.ndjson`. At 20:58:55 UTC it had 420 samples of 14 threads,
with maximum sample work 14.8 ms and nine disk-wait samples. Captured waits were
filesystem journal commit and writeback during fsync. No multi-second status
failure has yet been correlated with these thread samples. Raw capture and
causal interpretation remain pending; this observation is not a root-cause claim.

M1 still requires a matching six-hour AND 300-block loaded canary, the fleet
rollout and 24-hour all-worker observation.
