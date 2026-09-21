# Enhance PIR: communication can fall by 54–57%, with deployment work

Fresh isolated benchmark, September 20, 2026. Database: 466,986 × 737-byte records = 328.225 MiB raw. Four AMD vCPUs; synthetic records; actual production PIR runtime and serialization.

| Records per row | Query upload | Response | Recurring total | Public init envelope | Server computation p50 |
|---:|---:|---:|---:|---:|---:|
| 9, current layout | 420.0 KiB | 10.0 KiB | 430.0 KiB | 39.5 KiB | 30.8 ms |
| 29 | 166.0 KiB | 30.0 KiB | 196.0 KiB | 113.3 KiB | 42.1 ms |
| 58 | 125.0 KiB | 60.0 KiB | 185.0 KiB | 225.1 KiB | 63.5 ms |

- Including setup, 29 records/row is cheaper through 10 queries sharing one setup; 58 wins at 11+.
- 58 records/row with current physical shards exceeded the worker's 7-GiB memory limit in the full retention test: 7.08 GiB.
- Reducing physical shards from 8,192 to 4,096 rows kept the same 185-KiB query total and measured 5.83 GiB in that retention test. Server p50 was 72.8 ms. Full-service publication/concurrency still needs memory qualification.
- Existing clients, stored geometry and autoscaling require migration. The 2,048-row-shard option also exceeds the coordinator's current internal hint-response limit.
- The currently byte-efficient layouts have only 1.75% record-count headroom before a logical-row doubling. At one million records, a separately measured width-63 layout used 231.0 KiB/query and 82.4 ms server p50.
- All 1,528 randomized queries across the benchmark decoded exactly; 1,480 exclude warmup and contribute timing results.

Suggested presentation wording: **“We measured a 54–57% reduction in recurring client traffic by widening Enhance rows. Short sessions favor 29 records per row; longer sessions favor 58. Deployment requires a layout migration and memory qualification, and wider rows increase server packing cost.”**

These are application-body bytes and isolated server computation times, not WAN latency, production Intel timing or maximum throughput. The optimization concerns client traffic; internal generation-publication traffic is a separate cost. Empirical decoding success is not a cryptographic security proof. See REPORT.md and summary.json for exact measurements, scope and raw evidence.
