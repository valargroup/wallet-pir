# Schema-9 worker capacity at a 7 GiB soft limit

Date: 2026-09-22. Application source: `05337af410dd0f49214dc36fc4f053d3b126fb6c`
(schema 9, PR #86). The experiment uses an isolated source snapshot, not the
older main working tree. Schema-8 benchmark artifacts were removed from this
investigation at the user's request; no schema-8 measurements qualify this report.

## Current decision and evidence status

The [architecture specification](../../docs/archive/architecture-v6.md#five-generation-placement-model-and-memory-estimates)
is authoritative for the selected policy: **five retained published generations,
including current, plus one candidate; five total shards with one active frontier,
or six sealed shards per replica**. Maximum mutable-unit size remains 8K.

| Configuration | Evidence type | Resident + kernel peak | Headroom below 7 GiB |
|---|---|---:|---:|
| Five total, one active, eight published generations + candidate | Measured below | 6.702 GiB | 305 MiB; fails 512 MiB guard |
| Five total, one active, five published generations + candidate | Derived estimate | ~6.15 GiB | ~873 MiB |
| Six sealed | Derived estimate | ~5.21 GiB | ~1,833 MiB |

Five-generation retention removes three extra 8K revisions from the measured
active workload, saving `3 * 192 = 576 MiB`. Its database allocation becomes
`5 * 768 + 5 * 192 + 768 = 5,568 MiB` (5.4375 GiB), including the modeled
transition allowance. Six sealed databases occupy 4.5 GiB. Adding a rounded
0.71 GiB preparation/process/kernel allowance gives the projections above.
The unchanged 512 MiB guard fits both estimates, but neither is a new measurement.
The architecture owns the full formulas, session-expiry policy, and acceptance
requirements; this report owns the historical observations and their provenance.

## Measured results: eight-generation retention

Under the workload and 512 MiB resident guard defined below, the successful
schema-9 placement candidates are **seven sealed 32K shards**, or **four total
32K shards with one active frontier** (normally three sealed plus one active),
per worker replica. These counts are not additive. Each replica holds the full
assignment; a two-replica group does not double the capacity.

| Configuration | Peak process RSS, GiB | Conservative process + kernel, GiB | Headroom to 7 GiB, MiB | Resident guard |
|---|---:|---:|---:|---|
| Four total, one active, with 768 MiB transition allowance | 5.822 | 5.950 | 1,074.9 | Pass |
| Seven sealed | 5.816 | 5.945 | 1,080.6 | Pass |
| Five total, one active, with 768 MiB transition allowance | 6.573 | 6.702 | 305.4 | Fail |
| Eight sealed | 6.566 | 6.695 | 312.6 | Fail |

Both candidates reserve 5.25 GiB of distinct live query databases. The active
case completed twelve publications and 470 concurrent exact intermediate
evaluations, in addition to explicit old/candidate checks. The sealed case
completed fifty exact evaluations. Neither encountered a hard-limit or OOM event.
The four-shard frontier preparation p95 was 6.60 seconds (including synthetic
input generation and durable preparation), not an end-to-end query latency.

All four runs completed with zero hard-limit, OOM, or OOM-kill events. Five total
with one active completed twelve publications and 385 concurrent exact checks;
its preparation p95 was 7.15 seconds. Eight sealed completed fifty exact checks.
Completion does not override the guard failures in the table.

All cases reached the soft limit through artifact page cache: total charged
peaks were approximately 7.001 GiB. MemoryHigh events were 12,499 / 13,453 /
20,873 / 22,390 in table order. These are memory-controller event counters,
not counts of failed requests. Reclaim occurred; these are not zero-pressure
runs and sustained query latency was not qualified.

## Why plaintext size and worker memory differ

The user's calculation is correct:

```text
4 * 32,768 rows * 33 records * 737 bytes / 2^30 = 2.968872 GiB
```

That is the current plaintext payload, not all memory needed to publish and serve
it. Schema 9 uses 16-bit plaintext coefficients and six PIR instances. Each row
is padded from 24,321 plaintext bytes to 12,288 `u16` coefficients (24,576 bytes).
Four current encoded databases therefore occupy exactly 3 GiB, only 31.875 MiB
more than the plaintext calculation.

The active-frontier stress case additionally holds these databases:

| Allocation | GiB |
|---|---:|
| Four current 32K query databases | 3.00 |
| Eight extra versions of the changing 8K mutable unit | 1.50 |
| Four extra 8K runtimes modeling transition overlap | 0.75 |
| **Database subtotal before preparation/process overhead** | **5.25** |

The current database already includes one frontier version. At maximum overlap
there are eight published versions plus a ninth candidate, so eight additional
8K units are counted. This does not duplicate all four shards. The four transition
units are an explicit 768 MiB allocation stress allowance, not measured steady-state
data and not a proved bound on the proposed loan lifecycle.

Real request buffering, CRS preparation, serialization, allocator state, and query
and publication buffers add to process RSS. Cgroup-charged kernel memory and
disk-backed database/CRS page cache must also be considered. A peak from this
active stress case is not the footprint of four sealed shards.

For the measured four-shard schema-9 run, preparation/process overhead above the
5.25 GiB database subtotal was approximately 0.572 GiB, producing **5.822 GiB
peak RSS**. Adding the conservative kernel allowance gives **5.950 GiB**. This
is the measured answer to the apparent discrepancy with the 2.969 GiB plaintext
calculation; the plaintext calculation itself is not wrong.

## Method and decision rule

- Fresh cgroup per case: `MemoryHigh=7G`, `MemoryMax=7680M`, `MemorySwapMax=0`,
  four-CPU quota, CPUs 0–3, four Rayon threads. Cases run sequentially.
- Existing idle Linux benchmark host `wallet-pir-ci-01`, approximately 16 GiB
  physical RAM. No production worker runs there. The runner checks for CI jobs
  and stops its own test if one appears.
- Rust 1.91.0, release, `RUSTFLAGS=-C target-cpu=x86-64-v3`. iPIR/inspiring/kernel
  revision `225972648cc2982abfac66ba5b7a3930b223051a`, with the standalone lockfile
  preserved under `source/`.
- Each shard equivalent is 32K rows, composed of four 8K mutable units. Startup
  asserts 33-record rows, p=65,536, query precision 46, six instances, and 12,288
  columns. The profile is `simplepir-p16-q46-v1`.
- Production worker, preprocessing, artifact, and wire modules are imported
  directly from the committed schema-9 source. The harness supplies a minimal
  table catalog and row-digest adapter and deterministic synthetic record bytes.
- Preparation goes through the actual in-process HTTP handler, including request
  body copies, digest checks, real CRS construction, and durable artifacts. It
  sends one preparation request at a time. Socket/TLS buffering is not included.
- Active cases make twelve publications, exceeding the eight-generation retention
  window. They repeatedly evaluate oldest/newest retained generations in two
  concurrent tasks and stream publications during preparation. Four extra runtimes
  are prepared and held at the last full-retention overlap, then evicted on commit.
- Sealed cases cold-build all runtimes, then perform fifty exact evaluations in
  two tasks while streaming a publication.
- Queries are one-hot intermediate checks comparing all 12,288 coefficients,
  not fresh encrypted wallet round trips. Existing routing scans the whole worker
  assignment, not one independently selected architecture-2 shard.

A capacity candidate must complete successfully, pass its exact checks, have no
hard-limit/OOM events, and leave at least **512 MiB of resident headroom below
7 GiB**. The conservative resident estimate is:

```text
max(process-lifetime peak RSS, sampled peak anonymous memory)
+ sampled peak cgroup kernel memory
```

These peaks need not coincide, so the estimate intentionally overcounts. Cgroup
samples are taken every 0.5 seconds, supplemented by lifetime RSS, memory-peak and
event counters. The 512 MiB guard is a planning policy, not a failure-probability
certificate. Reclaimable file cache is reported separately; meeting the resident
guard is not the same as keeping all charged memory below MemoryHigh.

## Safety limits

This experiment can establish bounded memory-capacity candidates, **not certify
production safety**. The proposed independent-shard routing and fixed loan/return
lifecycle are not implemented by this harness. Its 768 MiB transition allowance
does not bound arbitrary reorgs, migrations, multiple boundary crossings, or the
successor's full growth. Admission must count projected distinct runtimes and
place the successor on another group when its complete reservation will not fit.
Do not spend the transition allowance twice.

An actual 8 GiB-host qualification is still required. A 7.5 GiB worker hard cap
on this 16 GiB test host does not prove that only 512 MiB outside the cgroup is
enough for an 8 GiB host's kernel and services. The outside-host allowance is
distinct from the 512 MiB guard inside the worker's soft limit.

The real service must bound queued preparation bodies before buffering them:
its execution semaphore is acquired after body collection. A one-preparation
execution limit alone does not enforce the memory envelope tested here. Sustained
query latency under cache reclaim, the six-hour/300-publication service soak,
restart/reload, failover, slow clients, reorgs, migrations, and cryptographic
correctness certification for the final query dimensions remain separate work.

## Evidence and reproduction

This directory is the single retained bundle for this investigation. Raw captures
are unchanged; paths inside captured commands refer to the original Linux run,
not the current checkout. Historical investigations elsewhere in the evidence
catalog remain separate and are not schema-9 capacity evidence.

- `REPORT.md` separates current derived estimates from measured results and scope.
- `provenance.json` pins application/dependency revisions, profile, and source hashes.
- `SHA256SUMS` protects the raw captures, source snapshot, exact case configuration,
  provenance, and derived analysis. Markdown and working analysis/runner scripts
  are excluded so editorial corrections do not change capture identity.
- `source/` contains the complete standalone harness, locked dependencies and four
  imported schema-9 source modules, with their original relative paths preserved.
- `linux-cases.json` defines the exact cases; adjust binary/scratch paths for a new
  isolated host directory.
- `remote-run.py` creates and stops only its own benchmark services; it refuses
  an active production worker or CI job. Results must use a new directory.
- `linux-results/` records exact commands, binary SHA-256, host and cgroup limits,
  worker events, 0.5-second samples, stderr, and `/usr/bin/time -v` reports.
- `linux-analysis.json` is generated with `analyze-linux.py linux-results --soft-gib 7`.
  The analyzer checks that the requested threshold matches the recorded cgroup.

Build `source/enhance/evidence/row-layout-benchmark-2026-09-22/Cargo.toml` with
`cargo build --release --locked --bin enhance-worker-capacity` and the flags above.
The path name is retained for relative imports; this crate contains only the
schema-9 worker binary. No schema-8 row-layout executable is included.

From this directory, verify the retained bundle and reproduce the derived summary
without rerunning the expensive benchmark:

```sh
shasum -a 256 -c SHA256SUMS
python3 analyze-linux.py linux-results --soft-gib 7 | diff -u linux-analysis.json -
```

The captured harness deliberately remains at eight-generation retention to
reproduce the measured run. A five-generation experiment must have a new run
identity and source/configuration; do not relabel or overwrite these captures.
