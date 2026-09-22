# Streaming Enhance artifacts — September 22, 2026

The candidate separates publication CRS from retained query databases and streams
artifact persistence, restoration, and worker hint responses. Protocol schema 8
and artifact format 7 are unchanged. The baseline is commit
`93fa9da7ba274e4d7f6f14e4b1b514b16bdca8be`; the candidate is the working-tree
implementation captured in `candidate.patch` and the source hashes in
[manifest.json](manifest.json).

## Results

One sequential local comparison on an Apple M4 Max with 128 GiB RAM, macOS 26.2,
using the release profile with fat LTO. Each scenario used the same deterministic
schema-8 shard: 8,192 rows, 29 records per row, and six CRS blocks. The benchmark
ran after workspace tests completed, on a shared developer machine.

| Scenario | Baseline peak RSS | Candidate peak RSS | Baseline operation | Candidate operation |
|---|---:|---:|---:|---:|
| Preparation with no cached artifact | 946.8 MiB | 562.6 MiB | 1.534 s | 1.529 s |
| Cached artifact loading | 967.8 MiB | 198.9 MiB | 0.857 s | 1.065 s |
| Hint fetching, including preceding load in memory peak | 967.8 MiB | 199.6 MiB | 0.382 s | 0.398 s |
| Eight retained frontier revisions | 3,644.5 MiB | 1,909.1 MiB | 1.520 s mean preparation | 1.550 s mean preparation |

After eight preparations, sampled RSS was 3,644.4 MiB for the baseline and
1,909.1 MiB for the candidate, a measured reduction of approximately 1,735 MiB
(47.6%). Live CRS coefficient ownership falls by exactly 192 MiB per revision,
or 1,536 MiB for eight revisions; the larger observed RSS reduction also reflects
allocator retention and temporary allocations. RSS is not a live-allocation
counter. Cached loading was slower in this run; one repetition does not establish
latency distributions or production performance.

Database, CRS, and metadata files matched byte for byte between the baseline and
candidate after both initial preparation and eight revisions. The encoded hint
was 201,375,776 bytes in both implementations, with equal SHA-256 digests.
[compatibility.json](compatibility.json) records all artifact hashes;
[results.json](results.json) records exact byte counts and times.

Disk retention is the deliberate tradeoff. The candidate held eight distinct CRS
inodes totaling 1,611,006,208 logical bytes. Including the current database and
metadata, live artifact lengths totaled 1,812,333,214 bytes (about 1.688 GiB),
versus 402,702,782 bytes (about 384 MiB) for the baseline. Seven candidate CRS
files had been replaced at their pathname but remained pinned by open handles.
The captures in `retained-candidate.open-files.txt` identify their distinct
inodes. These are logical file lengths, not physical APFS allocation measurements;
closing the last handle permits reclamation.

## Validation

`make check` passed: formatting, Clippy with warnings denied, operations/tooling
and documentation checks, and workspace release tests. The Rust summaries report
612 passed and two existing ignored tests. The compressed output is
[make-check.log.gz](make-check.log.gz).

New tests cover legacy artifact compatibility, exact MPH1 bytes, streaming
truncation/corruption/size errors, failed persistence, independent pinned readers,
replacement and eviction during reads, slow-consumer backpressure, cancellation,
and chunked HTTP decoding. Existing integration tests passed for full schema-8
sharded-versus-monolithic equality, embedded and remote publication, retained
queries, worker migration, and aborted topology changes.

## Measurement limits

- Runtime-level synthetic measurements; no live chain, journal, anchors, wallet
  workload, or fleet qualification. Input construction and row hashing outside
  `ShardRuntime` are not included in operation timings.
- “Preparation” means an artifact miss; the OS file cache was not purged.
  Cached-load and hint cases reused the first preparation's files.
- Hint data was drained locally into a SHA-256 sink. The candidate exercised the
  bounded streaming producer; the baseline encoded a full vector. HTTP socket
  transport and coordinator cache memory are excluded from this benchmark.
- Query correctness comes from the separate integration suite; these measurements
  contain no online PIR queries or wallet syncs.
- One repetition per case, with eight sequential revisions in the retention case.
  No measured benchmark run failed or timed out. No production deployment occurred.

## Reproduction and provenance

On macOS, from this candidate checkout, run:

```sh
python3 enhance/evidence/streaming-artifacts-2026-09-22/reproduce.py /tmp/new-streaming-artifact-run
```

Use a fresh output path. The script extracts the original baseline modules from
Git, compiles the baseline and current implementation in one harness using the
same dependencies, and runs each scenario in a fresh process. It writes artifacts
under that output directory; they are disposable after inspection. The measured
run used `measure.py` with a previously built identical harness. Exact commands,
raw JSON lines, `/usr/bin/time -l` outputs, open-file captures, lockfile, and build
output are retained alongside this note. `manifest.json` records the binary and
candidate source hashes. MiB and GiB use powers of two.

Verify the retained files with:

```sh
cd enhance/evidence/streaming-artifacts-2026-09-22
shasum -a 256 -c SHA256SUMS
```
