# M1 isolated burst comparison — Amsterdam, 2026-09-09

Operator: Codex acting for Roman. **The isolated comparison passed; M1 fleet
acceptance remains open.** Two build slots are a candidate for the full-residency
qualification. No live worker configuration or acceptance threshold was changed.

## Experiment and identity

The existing `transparent-pir-loadgen-01` ran four fresh processes from 07:37:06
to 07:38:59 UTC, ordered 1, 2, 2, 1 build slots. The host is a 16-vCPU Intel Xeon
Platinum 8280 with about 32 GiB RAM; the test unit was restricted to CPUs 0–3,
7 GiB memory and zero swap. This is not the same hardware/residency environment
as a fully loaded recent worker.

Each process builds an initial synthetic shard plus two successive revisions at
real `recent-8k` geometry. Two HTTP clients alternate directory/page queries,
selecting occupied rows and verifying decoded bytes against hashed plaintext.
Both clients complete warmup before the burst. Two candidates arrive one second
apart; the worker processes them sequentially, including queue delay in the
second candidate's visibility time. Both final-revision tables must decode
exactly before shutdown. All state is temporary and HTTP binds to loopback.

`build.json`, `linux-build.log`, `implementation.patch` and the runner's
`results/manifest.json` identify the source, compiler flags and executable.
All compiled Rust source hashes and the runner hash match the local implementation;
the executable hash matches the transferred Linux build. The raw runner manifest
also lists two uncompiled AppleDouble metadata files from the macOS tar overlay
(`._query.rs`, `._burst.rs`); those are not Rust modules or build inputs.

## Results

| Run | Build slots | Slowest worker visibility | Sampled peak process RSS | Exact queries | Query p95 |
|---|---:|---:|---:|---:|---:|
| Repeat 1 | 1 | 25.165 s | 1.553 GiB | 205 | 0.294 s |
| Repeat 1 | 2 | 10.272 s | 1.453 GiB | 104 | 0.114 s |
| Repeat 2 | 2 | 13.990 s | 1.691 GiB | 97 | 0.455 s |
| Repeat 2 | 1 | 20.852 s | 1.429 GiB | 186 | 0.125 s |

All four runs passed the isolated worker-stage 30-second check and exact-query
checks, with both clients overlapping the burst. **592 queries decoded exactly.**
Runs end when the burst and final-table verification finish, so raw query counts
are not a throughput comparison. Query p95 covers the query request/response,
not map/setup downloads or complete wallet synchronization.

With one slot, first-candidate preparation took about 9.11 s and the second
candidate waited about 8.12 s before preparing for another 12.73–17.05 s. With two
slots, first preparation took 5.60–5.74 s; second-candidate queueing was 4.60–4.75 s
and preparation 5.52–9.39 s. Activation itself took about 2–3 ms in this local
worker. The reduction is in warm preparation, not a simulated faster router.

The systemd journal records successful completion and approximately 1.8 GiB
peak unit memory, with no swap. Per-run RSS comes from 100 ms samples, begins
after initial prewarm, and includes the two in-process clients. Query p95 varies
between repetitions; this is not evidence of a general query-latency improvement.

## Limits and disposition

Artifacts are constructed before timing. This experiment excludes node ingestion,
publication construction, SSH/rsync, routing, archive quorum, managed-reconciler
contention and the other thirteen resident recent shards. All four worker-stage
runs passing does not contradict the previous live 30.842-second failure: the
live path includes those additional costs. Synthetic hashes do not attest to
mainnet canonicality. Available host memory on a 32 GiB generator cannot establish
20% headroom on an 8 GiB worker.

The measured reduction justifies qualifying **two build slots with the full
recent assignment resident**, two sustained clients and the production memory
limits. Only if that meets the freshness/headroom requirements should it be
promoted to the canary and a fresh six-hour/300-block gate started. The fleet
rollout and 24-hour observation remain gated by [deployment](../../deployment.md).
No production concurrency change or new canary soak was started by this run.

## Reproduction and artifacts

Run `make transparent-burst` locally; see [testing](../../testing.md#isolated-publication-burst-comparison)
for flags and the prebuilt executable option used on Amsterdam. The command
prints its output directory and preserves per-run logs and JSON even on failures.
Start with [summary.json](results/summary.json). Detailed run JSON includes every
query, candidate timing, readiness response and memory sample. Host inventory and
supervisor output are preserved beside this note. `checksums.json` covers the
saved artifacts; the evidence contains no wallet secrets or user wallet state.

## Validation

`make check` passed: operations tests, report tests, documentation links, formatting,
workspace Clippy with warnings denied, and the release workspace suite. The manual
burst is intentionally ignored in routine tests and was exercised separately in
all four Amsterdam processes. Runner regression tests verify that a failed budget
or crashed process preserves a summary, continues other configurations and cannot
overwrite previous evidence. See `validation.json` and `make-check.log.gz`.
