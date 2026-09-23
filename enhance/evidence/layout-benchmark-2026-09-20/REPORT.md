# Enhance PIR communication/layout benchmark — 2026-09-20

**Result:** widening the row substantially reduces client communication with the existing cryptographic implementation. There are deployment blockers, and the lowest recurring byte count is not the lowest total cost for every session. No production layout was changed.

For the refreshed live count of **466,986 records**, 737 bytes each (344,168,682 raw bytes, 328.225 MiB), the measured candidates are:

| Records/row | Shard rows | Upload KiB | Download KiB | Sum KiB | Init KiB | Server p50 / p95 ms |
|---:|---:|---:|---:|---:|---:|---:|
| 9 | 8192 | 420.008 | 10.016 | 430.023 | 39.525 | 30.81 / 34.84 |
| 29 | 8192 | 166.008 | 30.016 | 196.023 | 113.273 | 42.09 / 47.71 |
| 58 | 8192 | 125.008 | 60.016 | 185.023 | 225.085 | 63.48 / 70.78 |
| 58 | 4096 | 125.008 | 60.016 | 185.023 | 225.270 | 72.79 / 80.03 |
| 58 | 2048 | 125.008 | 60.016 | 185.023 | 225.639 | 66.25 / 72.13 |

**29 records/row is the first candidate for short sessions.** It cuts recurring bytes by 54.4% relative to nine. **58 records/row minimizes recurring communication in the searched layout family**, reducing it by 57.0%, but has higher setup and packing costs. The 2,048-row-shard candidate is blocked by the actual coordinator hint limit even though its in-process crypto benchmark passed.

## What was measured

- Fresh runs on `wallet-pir-ci-01`, an isolated 8-vCPU / 16-GiB DO-Premium-AMD host, restricted to four CPUs / four Rayon threads, 400% CPU quota, 10-GiB memory limit, no swap, Nice=10. Main manifest starts 05:44:40 UTC; supplemental manifest starts 05:52:26 UTC. No production queries were used to time alternative layouts.
- Repository commit `48c910c72f7349e69fe5d6d8631caad90e834221`; exact pinned `ipir-sp` / `inspiring` revision `accc424e879d8da425fa620aad80f0f2c4e0defd`. The harness imports unchanged production shard construction, evaluation, aggregation, and wire code. Original source copies are under `source/`.
- Deterministic synthetic 737-byte records at the actual live record count, also 450,163 records to match the earlier study and 1,000,000 records for growth. Data is a PIR byte-correctness fixture, not a canonical semantically validated record database.
- Real randomized query/key generation, serialization and parsing; evaluation of all populated shards; response packing/serialization; full decoded-row comparison. Targets cover first/last rows, polynomial and shard boundaries, and distributed interior rows.
- **1,480 timed queries plus 48 warmups; all 1,528 decoded exactly.** Current-count cases each have 100 timed queries; million-record cases have 60. All 22 remote cases exited zero, including six retention cases. Worst observed decryption error was 3.74% of the decoding threshold (26.8× margin). This is empirical correctness evidence, not a new security proof or a bound on rare failures.
- Server time includes parse, sequential shard evaluation/combination and response packing. It excludes HTTP, network/RPC, queues, journal writes and generation publication. This AMD host differs from production Intel hardware; use comparisons within this run, not these values as production latency or maximum throughput.
- Upload/download are actual serialized application-body bytes, including query/response envelope headers. Init sizes serialize real generated public parameters in the current JSON/base64 envelope, using a captured live template with adjusted geometry and fixed-length hash placeholders. This measures envelope size; it is not a signed deployable manifest. HTTP/TLS bytes are excluded. KiB = 1,024 bytes; GiB = 2^30 bytes.

## Include setup when minimizing total communication

For Q queries sharing one generation's public setup:

`total client bytes = serialized init bytes + Q × (query upload bytes + response bytes)`

The full measured-envelope totals at the refreshed count are:

| Records/row | One query + setup KiB | 10 queries + setup KiB | 11 queries + setup KiB | 100 queries + setup KiB |
|---:|---:|---:|---:|---:|
| 9 | 469.549 | 4339.760 | 4769.783 | 43041.869 |
| 29 | 309.297 | 2073.508 | 2269.531 | 19715.617 |
| 58 | 410.108 | 2075.319 | 2260.343 | 18727.429 |

Thus 29 wins through ten queries per setup; 58 wins at eleven and above. The 4,096-shard variant has essentially the same crossover. Repeated setup downloads after generation changes reset amortization. The sweep in `optimal-layout-sweep-dense.jsonl` checks every integer width from 5 through 512 for the pinned parameter selection, power-of-two logical rows, and shard minima 8,192 / 4,096 / 2,048. Its objective includes base64 public setup but excludes the small JSON metadata; the table above includes that metadata. Ties select the densest width for growth headroom. “Minimum” here refers to this existing layout/parameter family, not all possible PIR protocols.

## Blockers and tradeoffs

1. **Client compatibility and persisted geometry.** The current client explicitly rejects layouts other than nine records and 8,192 rows per shard ([client.rs](../../crates/enhance-pir/src/client.rs), geometry validation around line 92). Record addressing and expected cryptographic parameters also use constants. The journal validates persisted records-per-row and refuses a changed layout ([store.rs](../../services/enhance-pir-server/src/store.rs), around line 100). Cached shard artifacts bind dimensions. A versioned client/server transition, rebuilt artifacts and a separate migrated data directory are required; changing only the server constant will not work.

2. **Worker retention memory.** Production was checked over SSH: MemoryHigh is 6 GiB and MemoryMax is 7 GiB. Workers retain eight generations, with space for the new candidate during activation ([worker.rs](../../services/enhance-pir-server/src/worker.rs), lines 26 and 254–339). We built all current shards and eight distinct additional frontier revisions, retaining them together, with real production runtime construction:

| Records/row | Shard rows | Populated shards | Extra frontier revisions | Measured process peak GiB | Assessment |
|---:|---:|---:|---:|---:|---|
| 58 | 8,192 | 1 | 8 | 7.079 | Exceeds production 7-GiB hard limit |
| 58 | 4,096 | 2 | 8 | 5.831 | Below hard limit; close to 6-GiB soft limit |
| 58 | 2,048 | 4 | 8 | 5.741 | Below hard limit, but hint-size blocker below |

The first case uses 450,163 records; the smaller-shard cases use 466,986. Both counts have identical populated-shard geometry for these layouts; allocated rows are fully sized. Peaks are `/usr/bin/time -v` RSS. In-process VmHWM readings differ by less than 0.001 GiB. This is the worker-runtime construction/retention workload, not the full production process: HTTP hint serialization, concurrent traffic, caches and publication overlap require additional headroom. Therefore 4,096 rows is a promising configuration, not yet qualified for the current worker size. Other retention runs kept fewer revisions and must not be cited as a full eight-generation qualification for those widths.

3. **A concrete 2,048-shard publication failure.** `worker_hint_limit()` in [coordinator.rs](../../services/enhance-pir-server/src/coordinator.rs), around line 1475, allows four times the largest plaintext shard size. For width 58 and 2,048 rows this is **350,175,232 bytes**. The real production encoder emits **402,751,544 bytes** for its 12 CRS blocks. The response exceeds the limit and publication would be rejected. `hint-limit-proof.json` comes from actually allocating and serializing those blocks and asserting the bounds. At 4,096 rows the limit is 700,350,464 bytes, so that particular blocker is absent. A parameter-derived limit is needed if using 2,048-row shards.

4. **Autoscaling assumes the old layout.** [enhance-autoscale.py](../../../docs/cleanup-2026-09-23.md), line 22, fixes `GROUP_POSITIONS = 16 * 73728`, encoding 16 × 8,192 × 9. Capacity thresholds and group assignment need updating with any new geometry.

5. **Packing CPU rises with row width.** At the refreshed count, median scan time falls from 20.43 to 14.71 ms for width 58, but median packing rises from 9.10 to 48.40 ms. Overall median server computation rises from 30.81 to 63.48 ms; width 29 is 42.09 ms. The 4,096-shard width-58 run is 72.79 ms. Separate sequential cases have some run-to-run variation; the original baseline repeat moved from 30.99 to 31.61 ms. None of these runs establish maximum QPS.

6. **Growth headroom is small at the present optimum.** Both width 29 / 16,384 logical rows and width 58 / 8,192 rows hold 475,136 records: only 1.75% above the refreshed count. At record 475,137 the next logical-row doubling increases recurring bytes to approximately 282.023 KiB for fixed width 29, and 226.023 KiB for fixed width 58 (parameter arithmetic, not a timed run at that boundary). Wider alternatives may buy headroom but increase response/setup cost; continuously changing records-per-row requires repacking existing shards and cannot be treated as free.

7. **The remaining fixed upload has a security-sensitive escape hatch.** Fresh packing keys account for 86,016 bytes (84 KiB) per query in these configurations. Width 58 reduces the query vector to 41 KiB, hence the 125-KiB upload floor at this count. The pinned library labels its bounded key-reuse path “Experimental bounded evaluation-key reuse. Not a production security claim.” Reusing keys is separate protocol/security work, not an ordinary cache optimization. The measured widths used fresh keys throughout.

8. **Fleet traffic is a separate objective.** These totals optimize wallet-to-service communication. A width-58 shard's actual internal CRS hint is about 384.094 MiB, versus approximately 64.016 MiB at width nine (same wire-format arithmetic, six times as many blocks). Publication traffic, replicas, generation frequency and coordinator-worker query traffic also matter if “total” means every byte across the fleet. This run does not claim to minimize that larger objective. The large internal hint also explains why production memory headroom must include serialization and publication, not only retained runtimes.

The checked 64-MiB query limit, 16-MiB client response limit and 1-MiB init limit do not block these client envelopes. All queries still evaluate the full populated database; publicly selecting the requested shard was not used to reduce communication.

## Database growth: directly measured at one million records

Same hardware, same binary, 737-byte synthetic records, 60 measured queries per case. Raw data is 737,000,000 bytes (702.858 MiB).

| Records/row | Shard rows | Upload KiB | Download KiB | Sum KiB | Init KiB | Server p50 / p95 ms |
|---:|---:|---:|---:|---:|---:|---:|
| 9 | 8192 | 772.008 | 10.016 | 782.023 | 40.828 | 52.12 / 59.55 |
| 29 | 8192 | 420.008 | 30.016 | 450.023 | 113.830 | 64.09 / 69.63 |
| 63 | 8192 | 166.008 | 65.016 | 231.023 | 243.943 | 82.42 / 87.60 |

At one million records, width 63 is the recurring-byte winner from the parameter sweep and was actually timed. This illustrates why a width chosen for today's count should not be advertised as a permanent optimum. Width 34 is a setup-inclusive short-session candidate from arithmetic only; it was not timed.

For a fixed width k, logical rows are `next_power_of_two(max(ceil(N/k), shard_rows))`. Serialized query bytes grow with logical rows and query-bit precision, in steps at capacity boundaries. At fixed width, response and public-setup sizes stay constant until the cryptographic parameter selection changes. With width optimized as N grows, query bytes, response bytes, setup and packing work trade against each other. Server scan work generally grows with allocated populated shards, while packing grows with instance count; use the measured points rather than scaling one latency linearly across all configurations.

## Recommended next implementation

For short sessions, qualify width 29 first; it gets most of the recurring-byte reduction with materially less setup and packing than width 58. For sessions consistently exceeding ten queries per setup, qualify width 58 with 4,096-row physical shards and sufficient worker memory. Both require the client/store/artifact/autoscale migration above. Run full-service generation publication with eight retained generations and concurrent queries before choosing production memory limits. Do not reduce the retention window solely to make a layout fit without checking the client's session guarantees.

No fundamental decode failure was observed in the tested layouts. Deployment is blocked by current geometry contracts and, for specific candidates, resource and hint-size limits. Production rollout and experimental key reuse were not performed.

## Reproduction and evidence

- `src/main.rs`, `Cargo.toml`, `Cargo.lock`: exact benchmark harness and pinned dependency graph.
- `run.py`, `supplemental.py`: exact cases, arguments, environment and collision guard for CI work.
- `results/manifest.json`, `supplemental/manifest.json`: commands, UTC timestamps and exits. Main binary SHA-256: `e50498a389bcc01bb55409e90fc0a6280b1f5ee64232899c941e9385ad46043b`.
- `results/*.jsonl`, `supplemental/*.jsonl`: per-query timing, correctness, noise and memory evidence; matching `.time` files have process peaks; `.stderr` preserves errors. `results/hardware.txt` captures host configuration.
- `remote-final-status.txt`: systemd completion and binary hash. Both isolated jobs finished successfully and are inactive.
- `remote-build/`: build logs (including initial offline failure) and captured init template. Local intermediate build failures are also preserved.
- `live-health-at-start.json`, `live-worker-budget.txt`, `live-coordinator-budget.txt`: fresh read-only production observations.
- `hint-limit-proof.json`, `src/bin/hint-limit.rs`: actual encoder size/limit test.
- `src/bin/sweep.rs`, `optimal-layout-sweep-dense.jsonl`: parameter-arithmetic enumeration; the earlier non-dense tie-break output is preserved separately.
- `analyze.py`, `summary.json`: validated aggregates. Percentiles use nearest-rank p95; p50 is median. Warmups excluded from timing aggregates.

Build with `RUSTFLAGS='-C target-cpu=x86-64-v3' cargo build --release --locked --manifest-path enhance/evidence/layout-benchmark-2026-09-20/Cargo.toml --bin enhance-layout-benchmark`. Run on an isolated compatible host with `RAYON_NUM_THREADS=4 BENCH_INIT_TEMPLATE=<path-to-init-template.json> <binary> <records-per-row> <record-count> <query-count> <extra-retained-revisions> <shard-rows>`. For example, `29 466986 100 0 8192`. Use the resource restrictions and sequential case execution recorded above. Local ARM smoke/proof runs are not included in the server timing table.
