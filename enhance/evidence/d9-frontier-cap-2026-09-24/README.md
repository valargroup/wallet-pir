# D9 frontier unit cap comparison — 2026-09-24

**Decision: keep the current 8K cap for now; D9 needs another measurement after hint assembly is improved.** The isolated run found a useful memory saving, but current full-hint assembly made each 2K frontier publication about 5 seconds slower and the per-query evaluation median about 53% slower. Neither configuration has been qualified for QPS or admission on a real 8 GiB worker. Do not amend `architecture.md` from this result.

This run used schema-11 records, p16/q48, one 32K query domain with its last record absent, and the production unit builder, hint sum, packing builder, encrypted evaluation, and response packer. The benchmark example partitions the domain into either four 8K or sixteen 2K units. It bypasses only the production geometry validator so that the unimplemented D9 partition can be measured. Twelve exact encrypted answers passed per configuration in the query run; two more passed per configuration in the retention run. Each retention run kept six distinct versions of its frontier unit resident, building five new versions after the initial domain. All figures below are local Apple M4 Max measurements on a 128 GiB macOS host, not capacity measurements on Linux.

| Metric | 8K cap | 2K cap | Interpretation |
|---|---:|---:|---|
| Frontier revision build, median of 5 | 508 ms | 140 ms | 2K saves 368 ms |
| Frontier revision persistence, median of 5 | 1,234 ms | 775 ms | 2K saves 459 ms |
| Full-domain hint assembly per revision, median of 5 | 1,937 ms | 7,811 ms | 2K costs 5,874 ms more; all 16 units are read and summed |
| Packing rebuild per revision, median of 5 | 663 ms | 648 ms | Approximately unchanged |
| Sum of those median publication phases | 4,342 ms | 9,374 ms | 2K costs about 5.0 s more on this host |
| Encrypted evaluation per query, median of 12 | 7.25 ms | 11.11 ms | 2K costs 53% more; no load or QPS claim |
| Response packing per query, median of 12 | 4.91 ms | 5.24 ms | Similar; this is distinct from packing rebuild |
| Six frontier versions, process peak RSS | 6.535 GiB | 5.662 GiB | 2K saves 0.873 GiB in this isolated process |
| Six frontier versions, final sampled RSS | 6.535 GiB | 5.662 GiB | `ps` sample after fifth new revision; rounded |

The observed peak RSS difference exceeds the planning difference of `6 × (192 − 48) = 864 MiB` by about 30 MiB. It is roughly one 768 MiB sealed database image, **not** a qualified additional sealed-shard admission: a real worker also has retained snapshots, packing moved onto it under D6, transition/tail reservations, query buffers, host services, and a resident guard. The current [capacity model](../../docs/architecture.md) assigns 768 MiB of database bytes to a sealed shard and includes additional overhead. The full benchmark process includes synthetic setup, artifacts, hints, packing, and temporary build allocations; its RSS is not the worker's cgroup footprint. The host had 128 GiB RAM and no 8 GiB cgroup. The earlier [native c-4 preprocessing run](../architecture-v4-preprocessing-2026-09-23/README.md) measured 2K and 8K unit construction on an 8 GiB worker, but did not compare the two complete full-size layouts, retention RSS, or concurrent traffic.

The limiting publication phase is `Evaluation::hint`: it assembles the full hint from every unit on every frontier change. With 16 units, this local run spent approximately four times as long in that phase. An incremental cached sum of sealed-unit hints, or another measured reduction in repeated block reads, could reverse the publication result. That optimization needs exact-answer and revision-binding tests. The 12 isolated sequential queries show an evaluation penalty but cannot establish whether sustained QPS or p99 latency changes materially under batching and real traffic.

Next decision gate: run both partitions on an isolated, actual 8 GiB Linux worker with the intended packing location, six retained revisions, concurrent publications and queries, and cgroup `memory.peak`; report successful QPS, p95/p99 latency, failures, and admission margin. Repeat after incremental hint assembly if implemented. D9 should be adopted only when that run demonstrates sufficient memory headroom without violating the publication and query latency budgets.

## Raw inputs and provenance

[`commands.sh`](commands.sh) records the exact local build and four run commands. [`manifest.json`](manifest.json) records host, compiler, source base, and binary digest; each JSONL file records the binary digest and inputs. [`8k.jsonl`](8k.jsonl), [`2k.jsonl`](2k.jsonl), [`8k-retained.jsonl`](8k-retained.jsonl), and [`2k-retained.jsonl`](2k-retained.jsonl) contain per-unit, per-revision, and per-query timings. Corresponding `.time` files contain maximum RSS from `/usr/bin/time -l`. The benchmark source is [`frontier_cap.rs`](../../services/enhance-pir-server/examples/frontier_cap.rs). Disposable artifact directories remained under `/tmp/wallet-pir-d9-*` and are excluded from the PR.
