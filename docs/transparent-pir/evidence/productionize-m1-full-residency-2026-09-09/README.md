# M1 full recent residency — Amsterdam, 2026-09-09

Operator: Codex acting for Roman. This evidence qualifies an isolated recent
worker configuration; it cannot pass the live canary or whole-fleet gates.

## Workload and controls

The frozen fixture contains three consecutive real publications through heights
3,477,140, 3,477,141 and 3,477,142. Each assigns shards 160–173 to recent-01.
Global map/manifest/filter metadata is retained; only assigned table bytes are
copied. Unchanged files preserve hard links across publications. Fixture files
live outside the live publication tree and are never activated on a real worker.
`fixture-canonical.json` records the served-map hashes; the raw on-disk map hashes
are recorded separately. Assignments use the same 5% cache-reservation headroom
parameter as the existing fleet adapter; that is distinct from the host-memory
gate. `capture-fixture.py` records the capture procedure and fixed source paths.

The existing Amsterdam generator runs each trial in a fresh Linux cgroup:

- CPU affinity and allowed CPUs both 0–3.
- MemoryHigh 5.5 GiB, MemoryMax 7 GiB, swap disabled.
- RAM runtime cache 5 GiB; fresh runtime disk cache 10 GiB; four restore slots.
- All 28 current runtimes must be warm before each accepted activation.
- Two clients continuously query occupied directory/page rows of the changing
  tail and verify decoded bytes against hashed plaintext. Both final-revision
  tables must decode exactly before shutdown.
- Two candidate arrivals are scheduled one second apart; processing is serial,
  and the second candidate's visibility includes its queueing delay.

Cold startup is outside the publication timer but inside the kernel memory peak.
New runtime-cache writes, metadata loading, queries and revision overlap are
included. A test process has a fresh cache and activation file in its own temporary
directory; only its disposable cache can be collected. The source fixture remains
unchanged. The process contains the worker and both query clients, so memory and
CPU measurements include client overhead.

## Outcome: do not promote two build slots

The final run began at 08:08:11 UTC and finished at 08:17:48 UTC. All four
processes completed with full warm readiness and exact answers. All passed the
modeled memory check. The supervisor exited **1**, correctly, because one of the
two-slot runs exceeded even the isolated worker's 30-second visibility budget.

| Run | Slots | Cold prewarm | Worst worker visibility | Kernel peak | Modeled headroom | Exact queries |
|---|---:|---:|---:|---:|---:|---:|
| Repeat 1 | 1 | 134.22 s | 20.721 s | 5.501 GiB | 21.86% | 211 |
| Repeat 1 | 2 | 86.82 s | 29.973 s | 5.501 GiB | 21.86% | 207 |
| Repeat 2 | 2 | 88.17 s | **30.175 s — fail** | 5.501 GiB | 21.86% | 191 |
| Repeat 2 | 1 | 131.97 s | 26.451 s | 5.501 GiB | 21.86% | 255 |

**864 queries decoded exactly.** There were no OOMs, OOM kills, swap use or
hard-limit hits. Soft-limit events were observed in every run; the cgroup was
reclaiming memory around MemoryHigh. Memory staying near that threshold is
measured under active resource controls, not evidence of unlimited headroom.

Two slots shortened cold prewarm but made the first update after prewarm take
23.37–23.56 s; its successor then waited and took another 7.41–7.80 s to prepare.
One slot's per-candidate preparation ranged from 10.84 to 16.47 s. Activation
itself was small. Thus the earlier single-shard speedup does not qualify a
production concurrency change once full residency and disk caching are included.
Even the passing two-slot result leaves essentially no time for fleet staging
and routing, which are excluded from this experiment.

## Interpretation and next work

Keep the deployed one-slot setting. M1 remains incomplete; no canary configuration
change, replacement six-hour soak, or fleet promotion was started.

A source-based explanation to investigate is work-memory admission. The worker
reserves about 512 MiB per recent cold build and admits outstanding work only
below 90% of its 7 GiB cgroup limit (6.3 GiB). At a charge near 5.5 GiB, two such
reservations would require about 6.5 GiB and cannot both be admitted. A configured
second slot therefore does not guarantee two admitted builds at full residency.
This is an inference from the policy and observed memory, not a trace proving
which waits caused the measured delay. Do not weaken that protection on this
basis.

The next investigation should separate the query clients from the worker process
and record per-runtime restore/build timing and work-memory admission refusals.
This test includes client memory and CPU inside the worker's resource envelope,
so it does not establish how much of the regression would remain on the actual
fleet. Use that improved attribution to reduce update-time memory/work or schedule
preparation within the existing limits; then repeat full-residency qualification
before any canary promotion. The one-slot passes here also do not replace the
failed live end-to-end acceptance run.

## Memory interpretation

The final comparison reserves **768 MiB** outside the process on a modeled
8 GiB host and requires at least 20% headroom using the kernel high-water mark.
It also checks the actual cgroup CPU/memory/swap settings and absence of OOMs.
`memory.events.high` counts soft-limit pressure; it is reported, not interpreted
as an OOM. Sampled RSS is not substituted for kernel peak memory.

The contemporaneous live recent-01 snapshot reported 8,326,942,720 bytes total,
2,060,681,216 bytes available and 5,904,187,392 bytes charged to its worker cgroup.
Relative to nominal 8 GiB, unavailable memory outside that worker amounts to
625,065,984 bytes; the 768 MiB reserve adds margin. This is a single snapshot,
not a measured maximum. A modeled pass remains conditional on live-host headroom
in the actual loaded canary. The generator itself has about 32 GiB physical RAM.

## Earlier attempts

`earlier-attempts.tar.gz` preserves the preliminary runs:

1. The first fixture used the raw file SHA as the served-map SHA. The loader
   correctly rejected it before prewarm. The input `fixture.json` is retained;
   `fixture-canonical.json` corrects only that metadata from the assignments.
2. The next attempt completed the one-slot baseline with exact queries and
   21.004 s worst worker visibility, but its verifier expected a restricted
   cgroup CPU set while the runner had set process CPU affinity only. It was
   stopped and preserved. The final runner explicitly sets both constraints;
   it also uses the larger non-worker reserve described above.

Neither attempt supplies samples to the final qualification. No acceptance
threshold was relaxed and no live worker or router setting was changed.

## Reproduction

See [testing](../../testing.md#full-recent-residency) for the fixture schema and
Makefile options. The final command is recorded by the supervisor journal and
its source/binary identity by `build.json` and the run manifest. Raw public table
bytes remain in the isolated Amsterdam fixture at
`/opt/transparent-full-fixture-20260909`; this directory stores compact metadata
and measurements instead of committing the 1.1 GiB dataset. The fixture metadata
and implementation patch are preserved here.

## Validation

`make check` passed (576 Rust tests, one intentionally ignored manual benchmark,
workspace Clippy, formatting, operations/report tests and documentation checks).
The three runner tests pass, including refusal to qualify missing kernel counters,
incorrect limits, OOMs, excessive peak memory or non-isolated observations.
The default synthetic benchmark also passed locally in both slot configurations.
Compiled source and runner hashes match the final run manifest; the executable
hash matches the Linux build and the fixture hash matches the captured metadata.
Raw evidence and checksums preserve failures as well as successful samples.
