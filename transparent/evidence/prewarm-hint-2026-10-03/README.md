# Recent-worker tail prewarm: batched exact hint, 2026-10-03

This is a local release-fast benchmark on the development hub. It is not a
production, fleet or freshness qualification, and nothing was deployed.
Production attempt 14 is still the failed freshness observation: serial cycles
of 23–25 s and burst block visibility of 43–45 s, against the unchanged 30 s
target. In that attempt, both recent workers prewarmed for 10.13–11.34 s.

This record continues [the tail publication profile](../publication-freshness-2026-10-03/README.md).
That work cut native publication but projected 31–34 s burst visibility for
full tails. Most of what remained was the recent workers' two sequential native
tail builds. [manifest.json](manifest.json) records sources, binaries, commands
and inputs. [host.json](host.json) describes the host: the shared 8-vCPU hub,
with AVX2, without AVX-512, and with other tenants active.

## What changed

Each tail runtime build does three things:
- encodes the table;
- computes the public hint `H = A * D`, which is exact and reduced modulo `q`;
- builds two-mask preprocessing from that hint.

The hint went through `pir_native::hint`. That function makes one call into
Reinspiring's lifted products per column. Each call uses a scalar,
constant-time NTT over 42-bit primes, which exists to protect secret operands.
Nothing in a hint is secret: the masks come from published seeds, and the
plaintext is the server's own table.

`transparent_native::batched_hint::hint` computes the same integers. It uses
three NTT primes below 2^30. Each transform covers a tile of 32 columns, so
every butterfly applies one twiddle across contiguous lanes, which the compiler
vectorises. The full signed sum is rebuilt by CRT and only then reduced modulo
`q`. That is exact whenever the primes' product (about 2^90) exceeds twice the
sum's bound. The bound is computed from the masks actually given, the same way
the reference computes it. Recent-8k needs about 2^83. Any shape beyond the
bound is handed to the reference.

`TableRuntime::build` now calls it. Nothing else changes: parameters, seeds,
preprocessing, published masks, wire format, runtime snapshot format, build
threads, build slots and memory reservations.

## Result

Each binary built a tail's directory runtime and then its pages runtime in the
two-thread build pool. That matches a 4-vCPU worker with one build slot
prewarming a tail revision. The baseline and final binaries alternated twice
per fill, three rounds each. Inputs were the retained 24%, 66% and 97% tails.
Details are in [runs](runs/) and [summary.json](summary.json).

| Tail fill | Baseline, median (range) | Final, median (range) | Change |
|---|---|---|---|
| 24% | 4.19 s (3.89–4.67) | 2.91 s (2.75–3.54) | −30% |
| 66% | 4.79 s (4.41–5.55) | 3.02 s (2.85–4.02) | −37% |
| 97% | 4.81 s (4.50–5.40) | 3.05 s (2.74–3.16) | −37% |

Hint alone, median of three in-process alternations:

| Fill / table | Blocks | Reference | Batched |
|---|---|---|---|
| 24% directory / pages | 4 / 1 | 1.05 s / 0.42 s | 0.16 s / 0.08 s |
| 66% directory / pages | 4 / 3 | 1.17 s / 0.87 s | 0.17 s / 0.13 s |
| 97% directory / pages | 4 / 4 | 1.07 s / 1.06 s | 0.16 s / 0.15 s |

Peak resident memory per process was 422–431 MiB for both binaries. Scratch is
1.25 MiB per build thread, and the output hint is the same 32 MiB as the
reference's. Build threads, slots and reservations are unchanged, so this is
less CPU time under the same limits, not a concurrency change.

## Equivalence

- **Real tails.** At every fill, every batched hint equals the reference hint.
  The published-mask digests are identical across both binaries and all runs.
- **Deployed geometry.** `the_batched_hint_runtime_is_the_reference_runtime`
  runs recent-8k with a full directory and a partly filled pages table. It
  checks that the runtime publishes the masks of one prepared from the
  reference hint over every block. Its encrypted query answers must be
  byte-identical, and they must decode to the selected row.
- **Existing tests.** These now exercise the batched path:
  `masks_rebuilt_from_row_bytes_equal_the_served_masks`, the
  trailing-zero-block test, deployed-geometry disk restore and query, and the
  HTTP `round_trip` and `revisions_and_cache` suites.
- **Unit tests** in `batched_hint`:
  - an inverse round trip for each prime;
  - CRT at ±half capacity;
  - masks at both centered extremes with all-`0xffff`, random and zero columns,
    against independent schoolbook negacyclic products;
  - random masks against the reference;
  - shape, length and non-canonical-mask refusals;
  - the capacity edge at 512 blocks.
- **Mutations** ([mutations.json](mutations.json)). Dropping the third prime
  fails three tests. Loosening the capacity check fails one.

## Projection and limits

The following is arithmetic, not a measurement. Suppose production's prewarm
of 10.13–11.34 s scales like the local two-table build: ×0.63 at 66–97% fill,
×0.70 at 24%. Prewarm would then take about 6.4–7.9 s, a saving of about
3.1–4.2 s. The earlier record projected 17–19 s cycles after its publication
fix. Subtracting this saving gives about 13–16 s. At attempt 14's burst ratio
of about 1.8 cycles, block visibility would be about 23–29 s. The top of that
range leaves little margin below 30 s. Only a deployed measurement can decide
freshness.

Limits:
- **One shared host.** Production workers prewarm about twice as slowly, and
  their CPU features may differ. Deploy builds target x86-64-v3, which includes
  AVX2, the same vector width the hub used.
- **Synthetic inputs.** The tails are derived from the retained mainnet day,
  not production's tail.
- **Preprocessing.** Two-mask preprocessing now takes most of each build:
  1.1–1.3 s of about 1.5 s. It is in the pinned external Reinspiring crate and
  was not changed. Prewarm still runs the two tables one after the other at
  build slot 1.
- **Not exercised.** Query contention during prewarm, txid display tables and
  archive geometries were not run here. The hint is display-agnostic. Archive
  shapes stay within capacity, and the capacity test checks them.
- **Paths.** Absolute input paths in `runs/` were rewritten to `<a1-bench>`
  before they were retained.

## Rejected

- **More build threads or slots, or overlapping the two tables.** Each is a
  capacity or pipeline change. Each would need its own contention, memory and
  cancellation evidence. A pure compute reduction made them unnecessary for
  this step.
- **Reusing hints across revisions.** This was already ruled out: script tags
  are salted by the endpoint hash, so no nonzero row survives.

## Reproduce

Build `hint_bench` at the final source, and again with
[base-runtime.patch](base-runtime.patch) applied. Copy the two binaries to
`bin/hint_bench.final` and `bin/hint_bench.base` beside
[prewarm-compare.sh](prewarm-compare.sh). Then run it against the earlier
benchmark's working directory and run `python3 summarize.py`.
