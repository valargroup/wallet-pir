# Tiered txid display proof of concept: local stage, 2026-10-05

Local release-fast evidence for the time-tiered, hash-bucketed txid display
publication (source on `main`, see [txid display](../../docs/txid-display.md#tiered-display-publication-proof-of-concept)).
Everything ran on roman-dev-2 against **synthetic** journals. Nothing is deployed;
no production host was changed. The production stage, its change sheet and the
live measurements of each criterion remain open in
[remaining work](../../docs/remaining-work.md#tiered-txid-display-proof-of-concept-2026-10-05).

## What ran

| Run | Directory | What it shows |
|---|---|---|
| End-to-end run 1 | [local/e2e-run1](local/e2e-run1) | First integrated run at the pre-rebase handoff source (before `87181611`; exact revision not recorded), with the old synthetic size mix and a per-class fixture. **Failure retained:** 142 lookups failed with 503 "waiting places" because the per-class fixture's heavy page mix overloaded one worker. |
| End-to-end run 2 | [local/e2e-run2](local/e2e-run2) | Same harness at the current source with the recent-era mix and natural fixtures. **Harness gap retained:** the fixture was sampled once, so after the first window drop the recent tier had no samples and 1,961 slots were missed (80% of the last 245 s). |
| End-to-end run 3 | [local/e2e-run3](local/e2e-run3) | The accepted local run: load re-samples its fixture from the newest candidate every 60 s. |
| Recent bench | [local/bench](local/bench) | `bench-recent` (publisher side) and a recent-01-shaped worker under `systemd-run` limits, 10k–70k recent records. |
| Bucket ablation | [local/ablation](local/ablation) | One 240k-record chain published at N=1 (T=20k, T=40k) and N=4 (10k per bucket), each censused and metered. |

Each end-to-end run: a synthetic journal of 12,000 blocks at about 8 txids per
block, bootstrap through height 6,200, two `transparent-txid-server` workers
(archive owner, recent replica) behind [proxy.py](harness/proxy.py), and the
controller replaying 25 blocks every 2 s to the journal end while `txid-rate`
looks up 10 txids per second (80% recent, 20% archive, every 25th cold). Window
is 2 archives, `archive_target` 20,000, `recent_floor` 10,000, N=1, `txid-2k`.
Harnesses: [local-e2e.sh](harness/local-e2e.sh), [recent-bench.sh](harness/recent-bench.sh),
[ablation.sh](harness/ablation.sh), summaries by [analyze.py](harness/analyze.py).

## Results per criterion (local, synthetic)

| Criterion | Target | Local result | Production |
|---|---|---|---|
| Anonymity | ≥10,000 real txids per queried (shard, bucket) | Minimum (shard, bucket) 16,003 (run 3; the recent shard at the journal end); every sealed archive ≥20,000. Page-count classes are below 10k as expected: smallest 1 txid, 36 classes holding 5,623 of 56,013 txids. | Not measured |
| Recent rebuild | ≤20 s block to serving; p50 and max | Recent-01 limits (1 CPU, 1 build thread, 1.5G): per-block freshness p50 7.1–7.6 s, max 8.4–11.5 s for 10k–60k records; **70k (two directory segments) p50 19.7 s, max 22.1 s**. CPUQuota 200%, 2 threads: p50 4.2–4.9 s, max ≤6.0 s up to 60k, 9.8 s at 70k. Publisher build, verify, digest and write ≤0.9 s at every size. | Not measured |
| Archives immutable | Digests unchanged across rebuilds and seals | Run 3: 4 seals, 2 window drops, 0 violations over 112 observed maps; `verify` re-bootstrap reproduced all 4 sealed digests. Runs 2 and 3 produced identical seal digests. | Not measured |
| Latency | p99 ≤500 ms at the 20 QPS reference | Loopback, 9.0 lookups/s achieved (18.9 queries/s): per-lookup p50 54 ms, p99 277 ms (warm p99 242 ms, cold p99 360 ms); per-query HTTP p50 11 ms, p99 69 ms; 4,931/4,931 exact, 0 errors. Not comparable to the remote 20 QPS reference. | Not measured |
| Bandwidth | <300 KB per lookup, all requests for one txid | Metered bytes equal computed bytes for every sample. Warm: inline 92.6 KB, 1 page 138.9 KB, 3 pages 231.5 KB, 4 pages 277.8 KB, 5 pages 324.1 KB. Cold: inline 119.7 KB, 1 page 186.5 KB, 3 pages 279.1 KB, 4 pages 325.4 KB. Records ≤3 pages are 99.90% of the synthetic mix. Plain HTTP; TLS framing not included. | Not measured |
| Growth | No operator action across seal boundaries | 4 seals and 2 drops under accelerated replay with no intervention; 0 controller errors. | Not measured |

Smaller observations:
- 105 `lag` alerts in run 3 are expected under replay: 25 blocks per 2 s outpace
  a 5–7 s cycle, so cycles batch blocks. They are not live freshness.
- 328 residual missed slots in run 3 happen when a seal moves a segment's recent
  samples into an archive before the next re-sample; no lookup failed.
- The 26–52 stale-map retries per run are the client's single 409 retry after
  a recent revision moved; all succeeded.

## Benches and choosing the seal parameters

[bench-summary.json](local/bench/bench-summary.json) (first cycle per size,
which follows a cold activation, excluded):

| Recent records | Directory segments | Prepare p50 (1 CPU) | Freshness max (1 CPU) | Prepare p50 (2 CPU) | Memory peak (1 CPU) |
|---|---|---|---|---|---|
| 10,000 | 1 | 6.6 s | 8.4 s | 3.9 s | 402 MiB |
| 30,000 | 1 | 6.6 s | 9.8 s | 4.0 s | 404 MiB |
| 50,000 | 1 | 6.8 s | 11.5 s | 4.1 s | 413 MiB |
| 60,000 | 1 | 6.9 s | 10.9 s | 4.1 s | 406 MiB |
| 70,000 | 2 | 10.3 s | 22.1 s | 6.2 s | 487 MiB |

Worker prepare (the native two-table setup) is the whole cost, and it is flat in
record count until the directory needs a second 2,048-row segment, at about 60k
synthetic records (86% fill). The plan's rule is the largest `archive_target`
whose recent peak (about T plus the 10k floor) stays in one segment. These runs
support **T = 40,000 with `max_recent_records` 55,000**, keeping margin for a real
size mix that may pack less densely. That choice goes on the production change
sheet; the deployment default in source stays 20,000 until it is approved.

## Lever ablation

[ablation-summary.json](local/ablation/ablation-summary.json) and
[formula.json](local/ablation/formula.json):

| Configuration | Sealed shards | Table segments per shard | Runtime per txid* | Smallest (shard, bucket) | Smallest 1-page class | Warm inline / 4 pages |
|---|---|---|---|---|---|---|
| N=1, T=20k | 11 | 2 | 7.5 KB | 20,000 | 1,936 | 92.6 / 277.8 KB |
| N=1, T=40k | 5 | 2 | 3.8 KB | 40,002 | 3,919 | 92.6 / 277.8 KB |
| N=4, 10k per bucket | 5 | 5 | 9.3 KB | 9,398 (recent) | 912 | 92.6 / 277.8 KB |

\*Derived from the plan's 75.5 MB prepared runtime per 2,048-row segment; not
measured here.

Ranking for bandwidth, from the formula over the same transcript:
1. **Geometry.** The history-attached `archive-wide` tables cost 468.8 KB warm for
   an inline lookup and 2.21 MB at 4 pages; `txid-2k` costs 92.5 KB and 277.5 KB.
2. **Page count.** Each overflow page adds one 46.3 KB query.
3. **Cold metadata.** Map, manifest and two setups add 27–58 KB.
4. **Buckets.** No effect while one segment holds a shard: identical per-query
   bytes, more tables and smaller classes.

## Deviations

- N=1 instead of the brief's N=4 starting point (Roman, 2026-10-05). N stays a
  manifest parameter, exercised by tests and this ablation.
- Display metadata (map, manifests) is served by the recent worker, not the
  coordinator authority, so the map matches what is activated.
- All seals here happened under accelerated replay of synthetic blocks.

## Next optimizations (from the local data)

1. **Batched prewarm hint for display geometries.** Prepare is the entire
   rebuild cost. The history recent geometries' column-batched hint cut a
   two-table build by 30–37% ([evidence](../prewarm-hint-2026-10-03/README.md));
   `txid-2k` still uses the reference product.
2. **One directory query instead of two**, through a public choice hint: removes
   46.3 KB (half the inline transcript) from every lookup.
3. **Larger archives (T = 40k) with N = 1**: half the memory per txid, twice the
   smallest class, same bandwidth and rebuild time. Page-count classes stay
   below 10k without padding or a page cap.

## Provenance

- Source: `1f89cac0` (rebased onto `c0f4eee5`) for runs 2–3, benches and ablation;
  the harness re-sampling change came after the binaries and touches only shell.
  Run 1 used the pre-rebase handoff source (root session, 2026-10-05; revision
  not recorded), and its outputs are copied unchanged from the handoff directory.
- Build: `release-fast`, rustc 1.97.1, `-C target-cpu=native`;
  [binary digests](local/binaries.sha256).
- Host: roman-dev-2, DigitalOcean premium Intel, 8 vCPU, 31 GiB, Linux 6.8.0.
  Shared with other work; not production hardware.
- Machine-readable metadata: [manifest.json](manifest.json); checksums:
  [SHA256SUMS](SHA256SUMS).
