# Enhance evidence

## Current architecture-2 capacity evidence

The [schema-9 worker-capacity report](schema9-worker-capacity-2026-09-22/REPORT.md)
is the consolidated entry point for the 7 GiB worker budget. Its raw tests use
**eight published generations plus a candidate**. The current
[architecture-2 specification](../docs/architecture_2.md#five-generation-placement-model-and-memory-estimates)
selects **five published generations plus a candidate**, with five total shards
and one active frontier or six sealed shards. The five-generation figures are
derived estimates, not completed benchmark or production qualification results.

## Run catalog

| Record | Supports | Limit |
|---|---|---|
| [September 22 schema-9 worker capacity](schema9-worker-capacity-2026-09-22/REPORT.md) | Four isolated Linux capacity cases at a 7 GiB soft limit, raw memory/correctness evidence, pinned source, and five-generation planning estimates | Measured retention is eight generations; five-generation lifecycle and physical 8 GiB-host safety remain unqualified |
| [September 22 streaming review fixes](streaming-artifacts-review-fixes-2026-09-22/README.md) | Corrupt CRS recovery, preparation-slot regressions, and five before/after cached loads | Local warm-cache sample; no demonstrated latency improvement or production qualification |
| [September 22 streaming artifacts](streaming-artifacts-2026-09-22/README.md) | Local schema-8 before/after worker memory, persistence compatibility, streaming tests, and retained CRS disk usage | One synthetic run on Apple M4 Max; no fleet qualification or HTTP transport timing |
| [September 20 live PIR baseline](../../evidence/live-pir-2026-09-20/REPORT.md) | Fresh production inventory, public exact-answer probes, payload sizes, timing, and bounded load observations for Enhance and Transparent PIR | Schema 7 observation only; short WAN-bound runs do not establish saturation capacity |
| [September 20 wider-row encoding study](../../evidence/enhance-width-study-2026-09-20/README.md) | Pinned-encoder arithmetic and serialization sweep that selected candidates for empirical testing | No server timing, memory qualification, or deployment |
| [September 20 layout benchmark](layout-benchmark-2026-09-20/REPORT.md) | Isolated four-vCPU AMD measurements of nine-, 29- and 58-record rows: serialized bytes, server time, and 1,528 exactly decoded queries | Synthetic records; qualifies no production layout, and no retained window, publication, concurrency or 16-shard behaviour |
| [September 20 SimplePIR baseline](simplepir-baseline-2026-09-20/REPORT.md) | Same-machine, single-thread comparison of reference SimplePIR with the 29-record Enhance layout | Synthetic records and different protocol setup models; not a production replacement proposal |
| [September 20 schema-8 derivation](schema8-derivation-2026-09-20/README.md) | The live schema-7 `init` reading, and schema-8 sizes and worker residency derived from the pinned encoder | Derived, not measured; no schema-8 code has run on the fleet |
| [September 20 schema-8 rollout qualification](schema8-rollout-2026-09-20/README.md) | Checksummed candidate artifacts, prepared-journal validation, a completed three-shard retained-window run, and the truncated four-shard attempt | Isolated AMD fixture, not a production deployment or the six-hour full-capacity qualification |
| [September 14 public baseline](public-baseline-2026-09-14/README.md) | Fresh schema-7 public observations and 1/2/4/8-client query measurements with raw summaries and metadata | Short single runs; server hardware/revision and resource usage not observed |
| [September 13 c-4 preflight](preflight-2026-09-13/README.md) | Reported isolated preparation, memory and exact-answer measurements at a named revision | Raw bundle not committed; full qualification remains open |
| [Historical performance report](reported-performance/README.md) | Earlier README figures and traffic calculation | Undated, no raw query log or complete run identity |
| [Undated local summary](undated-local-summary/README.md) | Preserved JSON with stage timings and query counts | Test date, revision and hardware unknown; differs from the historical prose report |

[Performance](../docs/performance.md) explains the measurements;
[status](../docs/status.md) owns dated deployment claims, and
[remaining work](../docs/remaining-work.md) owns unresolved acceptance evidence.
Record new runs with the [shared evidence requirements](../../evidence/README.md).
Retained raw files are immutable; corrections belong in new notes or runs.
