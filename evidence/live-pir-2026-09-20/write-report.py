import json,pathlib,datetime
p=pathlib.Path(__file__).resolve().parent;s=json.loads((p/'summary.json').read_text());inv=json.loads((p/'inventory.json').read_text())
rows=[('Enhance','enhance','Whole logical DB: 65,536 rows; 434.70 MB padded','post_body_server_ms'),('Transparent recent directory','transparent-160-directory','8,192 × 3,584 B = 28 MiB','evaluation_ms'),('Transparent recent pages','transparent-160-pages','8,192 × 3,584 B = 28 MiB','evaluation_ms'),('Transparent archive directory','transparent-0-directory','32,768 × 3,584 B = 112 MiB','evaluation_ms'),('Transparent archive pages','transparent-0-pages','65,536 × 3,584 B = 224 MiB','evaluation_ms')]
text='''# Fresh empirical PIR measurements — September 20, 2026 (Dubai)

Measurements started September 19 UTC, which is September 20 in Dubai. All headline figures below come from fresh live queries, current node metadata, and file inspection; historical benchmark figures were not reused. See the exact UTC times in `run-manifest*.json`, `load-manifest.json`, and `inventory.json`.

## Presentation table: cost of one encrypted query

Warm, single-client queries over the public HTTPS retrieval origins. Upload/download are actual HTTP body lengths; headers, TLS framing, initialization, and filters are separate. KiB = 1,024 bytes; MB/GB are decimal. Timing percentiles describe successful queries; failures are retained below.

| Query | Table searched | Upload KiB | Download KiB | Combined KiB | Server p50 / p95, ms | Successful samples |
|---|---|---:|---:|---:|---:|---:|
'''
for title,key,size,metric in rows:
 r=s[key];up=r['upload_bytes'][0];down=r['download_bytes'][0];t=r[metric]
 text+=f"| {title} | {size} | {up/1024:.2f} | {down/1024:.2f} | {(up+down)/1024:.2f} | {t['p50']:.2f} / {t['p95']:.2f} | {r['successful_measured_queries']} |\n"
text+='''
**The server timing scopes differ:** Enhance is coordinator processing after the full upload has arrived, including queueing, coordinator work, worker RPC, and response packing. Transparent is actual all-segment evaluation inside the worker, excluding upload, runtime acquisition, and queueing. These are deliberately not labeled as identical CPU microbenchmarks. They are wall-clock stage durations, not CPU-seconds. Enhance worker RPC timings and Transparent whole-handler timings are included below.

## Current database inventory

- **Enhance:** 450,163 records × 737 bytes = **331,770,131 bytes (331.77 MB)** in the live record file at the inventory snapshot. Seven populated 8,192-row shards; nine records per row. Advertised logical geometry is 65,536 rows × 6,633 plaintext bytes = **434,700,288 bytes (434.70 MB)**, capacity 589,824 records. Encoded and prepared in-memory representations differ from these plaintext sizes. The chain continued advancing during the run; each Enhance sample set records its own generation and record count.
- **Transparent:** **353,378,453 events**, 175 shards (160 archive, 15 recent), covering height 0 through **3,489,198** in the captured map. The active padded directory/page files total **57,252,249,600 bytes (57.25 GB / 53.32 GiB)**. Public filter files add **63,428,567 bytes (63.43 MB)**. This is one active logical dataset, excluding replicas, superseded revisions, runtime caches, journals, and other disk overhead. Per-query work accesses the selected shard's table, not all 57.25 GB.

The sizes were collected by inspecting every active shard manifest and file stat, and the Enhance record file, over SSH. Counts and full manifests are in `inventory.json`; row-content checks use hashes of the actual stored plaintext.

## Timing details and observed network path

| Query | Prepare p50 ms | HTTP p50 / p95 ms | Decode p50 ms | End-to-end p50 / p95 ms |
|---|---:|---:|---:|---:|
'''
for title,key,_,_ in rows:
 r=s[key];text+=f"| {title} | {r['prepare_ms']['p50']:.2f} | {r['http_ms']['p50']:.2f} / {r['http_ms']['p95']:.2f} | {r['decode_ms']['p50']:.2f} | {r['total_ms']['p50']:.2f} / {r['total_ms']['p95']:.2f} |\n"
e=s['enhance'];text+=f"\nEnhance worker RPC p50 / p95: **{e['worker_rpc_ms']['p50']:.2f} / {e['worker_rpc_ms']['p95']:.2f} ms**. It includes private-network RPC and response decoding, so it is an upper scope around worker computation.\n\n"
text+='| Transparent query | Whole-handler p50 / p95 ms | Evaluation p50 / p95 ms | Queue p50 ms |\n|---|---:|---:|---:|\n'
for title,key,_,_ in rows[1:]:
 r=s[key];a=r['handler_ms'];b=r['evaluation_ms'];c=r['queue_ms'];text+=f"| {title} | {a['p50']:.2f} / {a['p95']:.2f} | {b['p50']:.2f} / {b['p95']:.2f} | {c['p50']:.3f} |\n"
text+='''
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

'''
for title,key,_,metric in rows:
 r=s[key];text+=f"- {title}: {r['successful_measured_queries']} successful measured queries; {len(r['errors'])} recorded query errors (see raw records for warmup classification). {r[metric]['n']} singly attributable server timing samples. Every successful measured answer matched stored plaintext.\n"
text+='''
The first archive-directory run stopped on a broken-pipe error after 15 measured successes; its raw output and stderr were preserved. A second probe revision records later request failures and continues to the next independent query without retrying that failed query. Unattempted requests after the original early stop are not counted as successes or attempted failures. Warmup failures are distinguishable in the JSONL. Across all runs, three recent-query errors were explicit cache-capacity refusals; seven other errors were HTTP transport failures. There were 426 successful measured queries and 10 failures (436 attempted); another 14 planned requests were unattempted after the initial early stop. All 45 warmup queries succeeded and are excluded from those figures. These failures are material even though every completed answer was correct. This is a small bounded experiment, not an availability SLO or capacity qualification.

**Live public discovery is currently inconsistent:** `/v1/shards/init` returned HTTP 404 on both public origins. The public filter map returned only three shards beginning at height 3,428,143 and ending at 3,473,474, whereas live private workers serve 175 shards through the current chain height. The query benchmark therefore obtains verified current geometry and revision metadata from private workers/SSH and sends the encrypted setup/query requests through the public Transparent origin. This proves current retrieval behavior with that bootstrap; it does not prove a fresh wallet can discover and recover through today's public APIs. See `public-discovery-check.json`, `transparent-map.json`, and `transparent-worker-map.json`. No routing repair was performed during measurement.

## Bandwidth and setup accounting

The table shows recurring per-query payload. Additional observed setup costs:

'''
for title,key,_,_ in rows:text+=f"- {title}: initialization/setup response body **{', '.join(map(str,sorted(set(s[key]['setup_bytes']))))} bytes** for the tested session/table.\n"
text+='''
Transparent generally sends **two directory queries per filter-matched script/shard**, followed by as many page queries as its history requires; inline events may eliminate page queries. Thus a single-query row is not a whole address recovery. Filters, manifests/maps, setup, multiple scripts, false positives, retries, and history pages must be added for a wallet budget.

For a chosen query rate Q, payload bandwidth is `Q × (upload_bytes + download_bytes) × 8`. At **10 successful queries/s**, arithmetic (not a measured capacity claim) gives:

| Query | Client upload MB/s | Client download MB/s | Combined Mbit/s |
|---|---:|---:|---:|
'''
for title,key,_,_ in rows:
 up=s[key]['upload_bytes'][0];down=s[key]['download_bytes'][0];text+=f"| {title} | {up*10/1e6:.3f} | {down*10/1e6:.3f} | {(up+down)*80/1e6:.3f} |\n"
text+='''
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

'''
a=s['transparent-0-directory']['evaluation_ms']['p50'];b=s['transparent-0-pages']['evaluation_ms']['p50']
text+=f"- Plaintext table doubles: **112 → 224 MiB**, rows **32,768 → 65,536**.\n- Upload: **258,056 → 430,088 bytes**, **{430088/258056:.3f}×**.\n- Response: **5,136 → 5,136 bytes**, unchanged.\n- Measured evaluation p50: **{a:.2f} → {b:.2f} ms**, **{b/a:.3f}×** on the same archive worker.\n"
text+='''
This comparison illustrates fixed per-query overhead plus table-size-dependent work; it is not a claim that every database doubling costs that exact multiplier. The recent workers use different CPUs and share publication activity, so their 8,192-row times must not be combined with archive measurements to infer a single hardware scaling curve.

## Reproducibility

`summary.json` is generated by `summarize.py`; `inventory.py` and `get-hardware.py` capture current node inputs; `probe/` contains the standalone measurement client and lockfile. `src-v1.rs` preserves the original abort-on-error implementation, while `probe/src/main.rs` records later query errors and continues. The first and continuation manifests identify the corresponding binary hashes. Raw JSONL, stderr, metadata, and their checksums are retained in this directory. Run scripts are capture records: do not rerun them over retained evidence; copy to a new dated output directory first.
'''
# Continuous-load data is deliberately separate from instrumented low-load queries.
for c in [1,8]:
 f=p/f'load-c{c}-summary.json'
 if f.exists():
  v=json.loads(f.read_text());t=next(x for x in v['stages'] if x['name']=='end-to-end');q=v['requests_per_second'];text+=f"\nFresh continuous Enhance load, concurrency {c}, 60 measured seconds after 10-second warmup: **{v['succeeded']} successes, {v['errors']} errors, {q:.2f} requests/s**, end-to-end p50/p95/p99 **{t['p50_ms']:.1f}/{t['p95_ms']:.1f}/{t['p99_ms']:.1f} ms**. At the observed payload size this is **{q*430088/1e6:.3f} MB/s upload**, **{q*10256/1e6:.3f} MB/s download**, **{q*440344*8/1e6:.2f} Mbit/s combined** (derived traffic rate). This closed-loop run is constrained by the client/network path and does not establish maximum server capacity. The tool counts drained completions against the configured measurement duration. See `load-c{c}-summary.json`.\n"
lf=p/'load-manifest.json'
if lf.exists():
 for step in json.loads(lf.read_text())['steps']:
  if step['exit_code'] != 0:
   text+=f"\nContinuous-load step ended with exit code {step['exit_code']}; consult its log for the failed threshold or error. It is not a passing capacity/SLO result.\n"
(p/'REPORT.md').write_text(text)
print(p/'REPORT.md')
