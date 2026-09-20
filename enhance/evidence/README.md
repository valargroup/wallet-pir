# Enhance evidence

| Record | Supports | Limit |
|---|---|---|
| [September 20 layout benchmark](layout-benchmark-2026-09-20/REPORT.md) | Isolated four-vCPU AMD measurements of nine-, 29- and 58-record rows: serialized bytes, server time, and 1,528 exactly decoded queries | Synthetic records; qualifies no production layout, and no retained window, publication, concurrency or 16-shard behaviour |
| [September 20 schema-8 derivation](schema8-derivation-2026-09-20/README.md) | The live schema-7 `init` reading, and schema-8 sizes and worker residency derived from the pinned encoder | Derived, not measured; no schema-8 code has run on the fleet |
| [September 14 public baseline](public-baseline-2026-09-14/README.md) | Fresh schema-7 public observations and 1/2/4/8-client query measurements with raw summaries and metadata | Short single runs; server hardware/revision and resource usage not observed |
| [September 13 c-4 preflight](preflight-2026-09-13/README.md) | Reported isolated preparation, memory and exact-answer measurements at a named revision | Raw bundle not committed; full qualification remains open |
| [Historical performance report](reported-performance/README.md) | Earlier README figures and traffic calculation | Undated, no raw query log or complete run identity |
| [Undated local summary](undated-local-summary/README.md) | Preserved JSON with stage timings and query counts | Test date, revision and hardware unknown; differs from the historical prose report |

[Performance](../docs/performance.md) explains the measurements;
[status](../docs/status.md) owns dated deployment claims, and
[remaining work](../docs/remaining-work.md) owns unresolved acceptance evidence.
Record new runs with the [shared evidence requirements](../../evidence/README.md).
Retained raw files are immutable; corrections belong in new notes or runs.
