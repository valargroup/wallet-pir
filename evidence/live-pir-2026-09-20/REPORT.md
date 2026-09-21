# Fresh empirical PIR measurements — September 20, 2026 (Dubai)

Measurements started September 19 UTC, which is September 20 in Dubai. All headline figures below come from fresh live queries, current node metadata, and file inspection; historical benchmark figures were not reused. See the exact UTC times in `run-manifest*.json`, `load-manifest.json`, and `inventory.json`.

## Presentation table: cost of one encrypted query

Warm, single-client queries over the public HTTPS retrieval origins. Upload/download are actual HTTP body lengths; headers, TLS framing, initialization, and filters are separate. KiB = 1,024 bytes; MB/GB are decimal. Timing percentiles describe successful queries; failures are retained below.

| Query | Table searched | Upload KiB | Download KiB | Combined KiB | Server p50 / p95, ms | Successful samples |
|---|---|---:|---:|---:|---:|---:|
| Enhance | Whole logical DB: 65,536 rows; 434.70 MB padded | 420.01 | 10.02 | 430.02 | 40.86 / 43.00 | 90 |
| Transparent recent directory | 8,192 × 3,584 B = 28 MiB | 125.01 | 5.02 | 130.02 | 11.81 / 34.20 | 86 |
| Transparent recent pages | 8,192 × 3,584 B = 28 MiB | 125.01 | 5.02 | 130.02 | 11.24 / 27.95 | 88 |
| Transparent archive directory | 32,768 × 3,584 B = 112 MiB | 252.01 | 5.02 | 257.02 | 7.92 / 9.28 | 75 |
| Transparent archive pages | 65,536 × 3,584 B = 224 MiB | 420.01 | 5.02 | 425.02 | 12.58 / 13.57 | 87 |

**The server timing scopes differ:** Enhance is coordinator processing after the full upload has arrived, including queueing, coordinator work, worker RPC, and response packing. Transparent is actual all-segment evaluation inside the worker, excluding upload, runtime acquisition, and queueing. These are deliberately not labeled as identical CPU microbenchmarks. They are wall-clock stage durations, not CPU-seconds. Enhance worker RPC timings and Transparent whole-handler timings are included below.

## Current database inventory

- **Enhance:** 450,163 records × 737 bytes = **331,770,131 bytes (331.77 MB)** in the live record file at the inventory snapshot. Seven populated 8,192-row shards; nine records per row. Advertised logical geometry is 65,536 rows × 6,633 plaintext bytes = **434,700,288 bytes (434.70 MB)**, capacity 589,824 records. Encoded and prepared in-memory representations differ from these plaintext sizes. The chain continued advancing during the run; each Enhance sample set records its own generation and record count.
- **Transparent:** **353,378,453 events**, 175 shards (160 archive, 15 recent), covering height 0 through **3,489,198** in the captured map. The active padded directory/page files total **57,252,249,600 bytes (57.25 GB / 53.32 GiB)**. Public filter files add **63,428,567 bytes (63.43 MB)**. This is one active logical dataset, excluding replicas, superseded revisions, runtime caches, journals, and other disk overhead. Per-query work accesses the selected shard's table, not all 57.25 GB.

The sizes were collected by inspecting every active shard manifest and file stat, and the Enhance record file, over SSH. Counts and full manifests are in `inventory.json`; row-content checks use hashes of the actual stored plaintext.

## Timing details and observed network path

| Query | Prepare p50 ms | HTTP p50 / p95 ms | Decode p50 ms | End-to-end p50 / p95 ms |
|---|---:|---:|---:|---:|
| Enhance | 29.44 | 1068.02 / 4381.18 | 0.88 | 1100.29 / 4414.67 |
| Transparent recent directory | 8.43 | 490.21 / 1198.88 | 0.78 | 500.14 / 1213.91 |
| Transparent recent pages | 8.53 | 523.85 / 1304.12 | 0.78 | 534.19 / 1317.54 |
| Transparent archive directory | 17.51 | 648.78 / 1575.71 | 0.77 | 673.97 / 1601.37 |
| Transparent archive pages | 29.71 | 1013.10 / 2247.37 | 0.77 | 1045.10 / 2278.63 |

Enhance worker RPC p50 / p95: **34.75 / 36.16 ms**. It includes private-network RPC and response decoding, so it is an upper scope around worker computation.

| Transparent query | Whole-handler p50 / p95 ms | Evaluation p50 / p95 ms | Queue p50 ms |
|---|---:|---:|---:|
| Transparent recent directory | 278.12 / 748.77 | 11.81 / 34.20 | 0.003 |
| Transparent recent pages | 352.40 / 806.18 | 11.24 / 27.95 | 0.002 |
| Transparent archive directory | 514.85 / 1242.10 | 7.92 / 9.28 | 0.002 |
| Transparent archive pages | 791.21 / 1794.24 | 12.58 / 13.57 | 0.002 |

Whole-handler time starts on handler entry and includes upload receipt, admission/runtime acquisition, and evaluation, ending at response construction. It excludes sending the response. Independently calculated percentiles must not be subtracted to derive another stage. End-to-end in the exact-answer probe is prepare + HTTP + decode for each request, excluding diagnostic metric scrapes. Initialization and setup are excluded from query latency. Metric scrapes introduce spacing between queries; these samples measure latency at low load, not saturation throughput.

Client: Apple M4 Max, 16 logical CPUs, 128 GiB RAM, macOS 26.2. Public traffic originates on the operator Mac; its access network/route was not independently characterized. All server nodes report DigitalOcean `ams3` through instance metadata. These WAN observations must not be advertised as universal mobile latency.

## Hardware and measurement integrity

- Enhance: two 4-vCPU, nominal 8-GB workers, Intel Xeon Platinum 8280. Coordinator: 8 vCPUs, nominal 64 GB, Xeon Gold 6548N.
- Transparent recent: four 4-vCPU, nominal 8-GB workers, CPU reported as `DO-Regular`.
- Transparent archive: two 8-vCPU, nominal 64-GB workers. The measured archive shard 0 is on archive-01, Xeon Platinum 8358; archive-02 reports Xeon Platinum 8168. Archive-02 was inventoried but is not represented by the query timing table.
- Probe uses the checked-out Rust client libraries in release mode and fresh cryptographic randomness for every request. It queries rows 0–29 repeatedly, with three warmups per block, targeting 30 measured attempts in each of three repetitions. These are fixed table rows, not a representative wallet workload. All successful answers are compared byte-for-byte via SHA-256 to the corresponding stored rows. No private wallet material is used.
- Enhance requests exercise the whole published database. Transparent targets one sealed recent shard (160) and one sealed archive shard (0), so results do not establish every shard's performance or changing-tail behavior.
- Server durations come from actual Prometheus sum/count counter differences around individual queries. A stage sample is admitted only when its count delta is exactly one and its duration is nonnegative. All raw before/after counters are retained. No histogram percentile interpolation is used: p95 is the nearest-rank sample percentile; p50 is the sample median.
- Client, server binaries, source identities, commands, hardware, database bindings, failed attempts, setup sizes, and per-query observations are retained. Server binaries were not redeployed or restarted.

## Failures and public discovery limitation

- Enhance: 90 successful measured queries; 0 recorded query errors (see raw records for warmup classification). 90 singly attributable server timing samples. Every successful measured answer matched stored plaintext.
- Transparent recent directory: 86 successful measured queries; 4 recorded query errors (see raw records for warmup classification). 86 singly attributable server timing samples. Every successful measured answer matched stored plaintext.
- Transparent recent pages: 88 successful measured queries; 2 recorded query errors (see raw records for warmup classification). 88 singly attributable server timing samples. Every successful measured answer matched stored plaintext.
- Transparent archive directory: 75 successful measured queries; 1 recorded query errors (see raw records for warmup classification). 75 singly attributable server timing samples. Every successful measured answer matched stored plaintext.
- Transparent archive pages: 87 successful measured queries; 3 recorded query errors (see raw records for warmup classification). 87 singly attributable server timing samples. Every successful measured answer matched stored plaintext.

The first archive-directory run stopped on a broken-pipe error after 15 measured successes; its raw output and stderr were preserved. A second probe revision records later request failures and continues to the next independent query without retrying that failed query. Unattempted requests after the original early stop are not counted as successes or attempted failures. Warmup failures are distinguishable in the JSONL. Across all runs, three recent-query errors were explicit cache-capacity refusals; seven other errors were HTTP transport failures. There were 426 successful measured queries and 10 failures (436 attempted); another 14 planned requests were unattempted after the initial early stop. All 45 warmup queries succeeded and are excluded from those figures. These failures are material even though every completed answer was correct. This is a small bounded experiment, not an availability SLO or capacity qualification.

**Live public discovery is currently inconsistent:** `/v1/shards/init` returned HTTP 404 on both public origins. The public filter map returned only three shards beginning at height 3,428,143 and ending at 3,473,474, whereas live private workers serve 175 shards through the current chain height. The query benchmark therefore obtains verified current geometry and revision metadata from private workers/SSH and sends the encrypted setup/query requests through the public Transparent origin. This proves current retrieval behavior with that bootstrap; it does not prove a fresh wallet can discover and recover through today's public APIs. See `public-discovery-check.json`, `transparent-map.json`, and `transparent-worker-map.json`. No routing repair was performed during measurement.

## Bandwidth and setup accounting

The table shows recurring per-query payload. Additional observed setup costs:

- Enhance: initialization/setup response body **40473 bytes** for the tested session/table.
- Transparent recent directory: initialization/setup response body **19435 bytes** for the tested session/table.
- Transparent recent pages: initialization/setup response body **19431 bytes** for the tested session/table.
- Transparent archive directory: initialization/setup response body **19436 bytes** for the tested session/table.
- Transparent archive pages: initialization/setup response body **19432 bytes** for the tested session/table.

Transparent generally sends **two directory queries per filter-matched script/shard**, followed by as many page queries as its history requires; inline events may eliminate page queries. Thus a single-query row is not a whole address recovery. Filters, manifests/maps, setup, multiple scripts, false positives, retries, and history pages must be added for a wallet budget.

For a chosen query rate Q, payload bandwidth is `Q × (upload_bytes + download_bytes) × 8`. At **10 successful queries/s**, arithmetic (not a measured capacity claim) gives:

| Query | Client upload MB/s | Client download MB/s | Combined Mbit/s |
|---|---:|---:|---:|
| Enhance | 4.301 | 0.103 | 35.228 |
| Transparent recent directory | 1.280 | 0.051 | 10.652 |
| Transparent recent pages | 1.280 | 0.051 | 10.652 |
| Transparent archive directory | 2.581 | 0.051 | 21.055 |
| Transparent archive pages | 4.301 | 0.051 | 34.818 |

## How costs change with database size

### Enhance: whole-database queries with power-of-two upload steps

At the current fixed record layout, padded rows are the next power of two above `ceil(records / 9)`, minimum 8,192. The current encoder's request body is `86,024 + ceil(rows × query_bits / 8)` bytes. The query coefficient width itself can increase with geometry. The two-instance response remains 10,256 bytes while row width/cryptographic parameters are unchanged.

| Record count scenario | Raw record DB MB | Logical rows | Upload bytes / KiB | Response bytes |
|---|---:|---:|---:|---:|
| Current inventory: 450,163 | 331.77 | 65,536 | 430,088 / 420.01 | 10,256 |
| 2× records: 900,326 | 663.54 | 131,072 | 790,536 / 772.01 | 10,256 |
| 4× records: 1,800,652 | 1,327.08 | 262,144 | 1,495,048 / 1,460.01 | 10,256 |

**Only the first row was measured on the live database. Future rows are encoder/layout projections**, checked against the current parameter-generator geometry sweep, not deployments or future latency measurements. There is upload headroom until 589,824 records; crossing that capacity raises the logical row count and request size. Populating more physical shards increases server evaluation work even before the next upload-size jump.

Server scan work grows approximately with the populated physical shards/encoded bytes, plus fixed cryptographic and coordination costs. The 2× scenario uses 13 shards versus today's 7, so scan work is approximately 13/7 under the existing full-shard layout, not necessarily exactly twice. Every query evaluates every populated Enhance worker group; adding groups can parallelize wall time while aggregate compute and storage still increase. Therefore an exact future server-time multiplier cannot be established from this live baseline. No synthetic larger database was installed on production.

### Transparent: fixed-size shard queries; wallet work depends on matches

Adding shards without changing their geometry preserves each query's upload, response size, and work over that table. Total disk/cache capacity grows with the dataset; keeping those runtimes warm requires proportional capacity. Insufficient memory, cold runtime builds, and queueing can increase latency even when the per-query geometry stays fixed.

For a full-history wallet scan, filter/map coverage grows with historical data; at equal mix and occupancy, twice today's dataset would have roughly twice the filter bytes (about 126.9 MB before caching/encoding effects). That is a projection. A wallet's private query bill depends on matching script–shard pairs and history pages, not all global shards. An inactive wallet can avoid new private queries; an active wallet spanning more shards pays for additional queries. With the same query mix, twice as many needed queries means twice the private traffic and aggregate server work. Recent-only sync need not traverse all archive growth.

Changing the shard geometry is a different operation. The current archive directory and page tables provide a same-worker empirical comparison:

- Plaintext table doubles: **112 → 224 MiB**, rows **32,768 → 65,536**.
- Upload: **258,056 → 430,088 bytes**, **1.667×**.
- Response: **5,136 → 5,136 bytes**, unchanged.
- Measured evaluation p50: **7.92 → 12.58 ms**, **1.589×** on the same archive worker.

This comparison illustrates fixed per-query overhead plus table-size-dependent work; it is not a claim that every database doubling costs that exact multiplier. The recent workers use different CPUs and share publication activity, so their 8,192-row times must not be combined with archive measurements to infer a single hardware scaling curve.

## Reproducibility

`summary.json` is generated by `summarize.py`; `inventory.py` and `get-hardware.py` capture current node inputs; `probe/` contains the standalone measurement client and lockfile. `src-v1.rs` preserves the original abort-on-error implementation, while `probe/src/main.rs` records later query errors and continues. The first and continuation manifests identify the corresponding binary hashes. Raw JSONL, stderr, metadata, and their checksums are retained in this directory. Run scripts are capture records: do not rerun them over retained evidence; copy to a new dated output directory first.

Fresh continuous Enhance load, concurrency 1, 60 measured seconds after 10-second warmup: **43 successes, 0 errors, 0.72 requests/s**, end-to-end p50/p95/p99 **1289.2/2115.6/2603.0 ms**. At the observed payload size this is **0.308 MB/s upload**, **0.007 MB/s download**, **2.52 Mbit/s combined** (derived traffic rate). This closed-loop run is constrained by the client/network path and does not establish maximum server capacity. The tool counts drained completions against the configured measurement duration. See `load-c1-summary.json`.

Fresh continuous Enhance load, concurrency 8, 60 measured seconds after 10-second warmup: **294 successes, 0 errors, 4.90 requests/s**, end-to-end p50/p95/p99 **1548.3/2449.4/3530.8 ms**. At the observed payload size this is **2.107 MB/s upload**, **0.050 MB/s download**, **17.26 Mbit/s combined** (derived traffic rate). This closed-loop run is constrained by the client/network path and does not establish maximum server capacity. The tool counts drained completions against the configured measurement duration. See `load-c8-summary.json`.
