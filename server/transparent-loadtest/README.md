# Transparent recovery simulation

`transparent-loadtest` supports reproducible mixed-wallet recovery scenarios as
well as the existing stepped-concurrency load test. Both use the reference
wallet HTTP adapters and verify recovered events against a pinned journal
sample. Scenario mode supervises separate wallet processes so a recovery deadline
actually stops client work.

## Run a scenario

From the repository root, the Make targets are the quickest way to run:

```sh
make transparent-sim             # 20-wallet wave
make transparent-sim-sustained   # 20 slots maintained for 10 minutes
make transparent-sim-open        # Open the newest saved report in your browser
make transparent-sim-help        # All options and examples
```

Each invocation creates a fresh temporary report directory and prints its path.
The HTML report path is printed again when available, including after an
unsuccessful run. Make fails when the runner reports an unsuccessful run.

`make transparent-sim-open` opens the newest available report by modification
time, checking automatic temporary runs and the most recent report remembered by
this checkout. Make remembers custom `SIM_OUT` locations too, including reports
from unsuccessful runs. To open a particular run:

```sh
make transparent-sim-open SIM_OUT=/tmp/my-wave
```

Opening reports requires Python 3 and uses `open` on macOS or `xdg-open` on Linux.
The latest-report pointer lives in Git's metadata directory, outside tracked files.

```sh
make transparent-sim \
  SIM_URL=http://localhost:8093 \
  SIM_METRICS="local=http://localhost:8093/metrics" \
  SIM_OUT=/tmp/my-wave
```

`SIM_METRICS` accepts multiple space-separated `NAME=URL` entries. Use
`SIM_FILTER_URL` for a separate filter origin, `SIM_RUN_ID` for a report label,
and `SIM_SCENARIO` for custom wallet profiles or durations. URLs otherwise come
from the selected scenario. An explicit `SIM_OUT` must be a new directory; its
parent is created automatically. `transparent-sim-wave` is an explicit wave alias.

The equivalent direct CLI invocation is:

```sh
cargo run --locked --release -p transparent-loadtest -- \
  --scenario server/transparent-loadtest/scenarios/mixed-20-wave.json \
  --out-dir /tmp/transparent-wave-001
```

Use `mixed-20-sustained.json` for a ten-minute sustained run. Both scenarios use
20 slots: 2 unused, 4 small-active, 2 each of one-day/seven-day/thirty-day catch-up,
3 six-month restores, 2 old restores, 2 multi-script wallets and 1 reused-tail
wallet. These are explicit synthetic workload weights, not a population model.

Override origins and add every measured worker's full private metrics URL:

```sh
cargo run --locked --release -p transparent-loadtest -- \
  --scenario server/transparent-loadtest/scenarios/mixed-20-wave.json \
  --shard-url http://127.0.0.1:8093 \
  --filter-url http://127.0.0.1:8093 \
  --metrics-target worker-1=http://127.0.0.1:8093/metrics \
  --out-dir /tmp/transparent-wave-002
```

`/metrics` is an operator endpoint; use worker addresses reachable from the load
host. It is not exposed through the public edge. No metrics URL means no server
APM; unavailable scrapes remain gaps. The runner never deploys, restarts,
clears a service cache. Wallet preparation can warm server caches.

The output directory must not exist; its parent must exist. Open `report.html`
locally after completion. The report is self-contained and works offline.
`report.json` is checkpointed every five seconds. `wallets.ndjson`,
`requests.ndjson`, and `metrics.ndjson` preserve incremental observations;
`manifest.json`, `scenario.json`, `map.json`, and discovery request logs preserve
provenance. `--run-id` overrides the generated run identifier. Source commit and
build profile are embedded in the executable at build time. Worker stderr is under `logs/`. Unsuccessful SQLite databases remain
under `stores/`; exact stores are removed only after verification and close.

The existing **Load-test transparent shards** workflow accepts `scenario_path`
and `metrics_targets` (a JSON mapping of worker names to full `/metrics` URLs).
The scenario controls storage, duration, profiles and query budget; legacy
step/store/sample inputs apply only when `scenario_path` is blank. Origin inputs
still override scenario origins. Artifacts upload even when the scenario fails.
The coordinator is the load host and may share CPU and memory bandwidth with
ingest; record that and target SKU/region/background traffic in scenario notes.

## Scenario contract

A `transparent-scenario-v1` JSON file contains:

| Field | Meaning / default |
|---|---|
| `name`, `mode` | Run label; `wave` or `sustained` |
| `sample` | Journal sample path relative to the scenario file |
| `shard_url`, `filter_url` | Retrieval origin; filters default to retrieval |
| `profiles` | Class name → positive concurrent slot count; 1–512 total |
| `preparation_concurrency` | Total preparation workers across all profiles, default 2; idle slots immediately take the next eligible wallet |
| `preparation_cache` | `reuse` (default), `refresh`, or `off` |
| `preparation_cache_dir` | Optional persistent cache directory override |
| `measured_http_attempts` | 1–3 attempts per HTTP call; custom scenarios default to 1, supplied scenarios use 3 |
| `seed` | Deterministic class shuffle, default 1 |
| `duration_seconds` | Sustained admission window, default 600 |
| `recovery_deadline_seconds` | Measured recovery hard deadline from dispatch, default 600 |
| `preparation_deadline_seconds` | Full-history preparation deadline per wallet, default 3600; independent of the measured deadline |
| `request_timeout_seconds` | Individual HTTP timeout, default 60 |
| `store` | `sqlite` (default) or `memory`, independent per recovery with prior ledger events seeded for windowed profiles |
| `max_queries` | Optional per-sync private-query budget; default unlimited |
| `metrics_targets` | Worker name → full metrics URL; default empty |
| `max_p99_exact_seconds` | Optional per-profile exact-recovery latency objective |
| `notes` | Hardware, network, background traffic and cache-state context |

A wave releases prepared workers through a common barrier and runs each slot
once. Actual start offsets show skew. A sustained run replaces completed wallets
within the same profile, with no think time; this preserves slot composition,
not equal completed-wallet frequencies. Wallets are selected without concurrent
reuse within a profile, cycling deterministically after exhausting its sample.
Each recovery opens new adapters and a new store; child processes persist between
recoveries. Process startup and admission skew are separate from sync duration.

After the sustained window, existing recoveries drain until their own deadlines.
The report separates exact completions during load and drain. Whole-run exact
throughput divides all exact completions by execution time including drain;
load-window throughput counts only exact completions within that window.

The sample must supply an anchor hash. The default wave and sustained scenarios
set `allow_advancing_publication: true`: recovery stays pinned to the sample's
historical height/hash, while the service may publish newer blocks. Genesis and
range must match, the publication must cover the target, and any named endpoint
at the target must agree with its hash. The sample provides the benchmark's
accepted target; this is not independent live chain verification. Events above
that target never enter the digest comparison. Prepared ledger seeds are bound
to the same genesis and target.

Custom scenarios default to strict publication matching. With
`allow_advancing_publication: false`, the served tip must equal the sample target,
and map changes during preparation or measurement invalidate the run.
Reports record `publication_stable` separately from `publication_compatible`,
plus the sampled target and served tip. Failed postflight remains unsuccessful
in both modes. Wallet revision/refusal checks remain enabled.

Every started job has a terminal outcome: `exact`, `mismatched`, `incomplete`,
`failed`, `timed_out` or `cancelled`. Only complete syncs matching both the
journal-derived event count and SHA-256 digest are exact. A query budget does not
turn partial progress into success. A nonzero exit follows any unsuccessful
recovery, incompatible publication (or any drift in strict mode), interruption, supervisor error, or configured
latency objective violation. Missing APM does not fail exact recovery, but the
report labels the missing evidence. SIGINT/SIGTERM finalize a report; SIGKILL
may leave an unfinished checkpoint and append-only evidence.

## Measurements

HTTP observers record each attempt's stage, status, elapsed time and payload
bytes, including error response bodies and retries. They record no query body,
key or wallet script. Upload is submitted payload size, not measured socket
transmission; bytes from partially read failed responses are not counted. Initial
per-wallet map/init requests are included, with supervisor preflight/postflight
requests in separate files. No HTTP headers or TLS totals are inferred.

Exact-only p50/p95/p99 are reported separately from unsuccessful attempt durations.
Fewer than 100 exact samples per profile are marked sparse. A twenty-user wave
is a useful concurrency/correctness observation, not enough evidence for tail
latency or fleet capacity.

Server metric scopes:

- `transparent_shard_query_seconds{outcome="success|error|cancelled"}`: query
  handler entry through response construction, including body receive; excludes
  response transmission. It includes early returns and dropped handlers.
- `transparent_shard_queue_wait_seconds`: evaluation semaphore wait, including
  expired and cancelled waits; excludes body receipt and runtime acquisition.
- `transparent_shard_evaluation_seconds`: all table segments inside the blocking
  evaluation task, excluding runtime acquisition/build and executor queueing.
- `transparent_shard_process_cpu_seconds_total` and
  `transparent_shard_process_start_time_seconds`: cumulative process CPU and
  restart identity. CPU rate is cores consumed, not percent of the host.

The report also plots existing cache/build, queue/refusal, warm-state and process
memory metrics. Queue depth includes waiting **and running** requests. Cache
reserved bytes are not RSS. Each worker is scraped every second with an initial
and final sample; gaps, counter resets and publication/process identity changes
break derived time series. Histogram quantiles use interval bucket deltas and
are never averaged across workers. All traffic to a worker is included.

Hard termination stops the wallet process, but an already admitted server
evaluation may finish afterward. Client CPU samples can undercount short-lived
processes between scrapes. Shared-host and background traffic limitations remain
visible in the report.

## Validation

```sh
cargo test --release -p transparent-loadtest
node --test server/transparent-loadtest/tests/apm.test.cjs
make check
```

The integration suite executes the real binary, child-process protocol, HTTP PIR
and SQLite against deterministic local fixtures. It covers a twenty-user wave,
sustained replacement, mismatches, refused filters, hard deadlines, publication
failure, process crashes, interruption, and a receive-before-window/spend-inside-window
regression for both stores. Payload aggregate tests include repeated and unsuccessful
recoveries. APM arithmetic tests cover missing
scrapes, resets, escaped labels and histogram intervals. Set
`TRANSPARENT_SIMULATION_QA_DIR` to an artifact directory when running the Rust
suite to retain a real local-PIR report for visual review.

Fully persistent sessions that retain caches between recoveries, mobile/network
emulation, intentional chain
churn and operational dashboard integration are separate work. See the
[accepted-anchor regression suite](../../docs/transparent-pir/testing.md) for
persistent wallet correctness and the [evidence rules](../../docs/transparent-pir/evidence/README.md)
for capacity claims.

### Wallet initialization and summaries

Before measured load begins, wallets whose requested window starts after the
publication start undergo a separate full recovery through the sampled anchor.
Preparation must complete with no unresolved spends and match the sample's
independent expected count and digest for the requested window. Only events
strictly before that window are saved as the initial ledger. The measured recovery
uses an independent store with these prior events and cold filter/setup caches;
no measured-window events or coverage are imported. Seed import time is included
in the measured recovery duration. This models prior ledger state, not a fully
warm persistent wallet cache.

Preparation uses a finite queue of killable workers and its own
`preparation_deadline_seconds` (default 3600 per wallet). As soon as a wallet
finishes, the next eligible wallet starts, respecting the global worker count
and each profile's limit. A wave prepares its selected wallets; sustained runs
prepare their replacement pool. Measurement starts only after every required
wallet is ready. A terminal preparation failure stops admission, drains active
workers, and preserves successful seeds for the next run.

Validated preparation is cached by default under
`$XDG_CACHE_HOME/transparent-loadtest/preparation`, or
`$HOME/.cache/transparent-loadtest/preparation`. Keys include the workload sample,
wallet scripts/window, accepted historical target, chain and service identity,
seed format and an implementation fingerprint computed from Rust sources,
manifests, build scripts and dependency pins. Strict mode also binds the map
digest. Advancing mode retains the historical anchor compatibility checks.
Only complete, exact recoveries with no unresolved spends are stored. Entries
are checksummed and atomically published; invalid entries are reported as misses
and rebuilt. A 5 GiB LRU budget bounds persistent storage. Oversized entries
remain in the run directory but are not persisted. Cache errors do not turn
otherwise correct recovery into failure.

Each run copies accepted seeds into its own `seeds/` directory. Measured stores
remain independent, with cold filter/setup caches and no imported coverage or
measured-window events. Cache hits contribute no historical preparation traffic
to the current run. Cached source-run provenance remains visible separately.
Changing relevant source bytes invalidates the cache even before a commit;
changing only documentation does not. Legacy report seeds are not auto-imported.

```sh
make transparent-sim-wave                         # reuse preparation by default
make transparent-sim-wave SIM_PREP_CACHE=refresh  # fresh preparation; replace successful entries
make transparent-sim-wave SIM_PREP_CACHE=off      # no persistent cache reads or writes
make transparent-sim-wave SIM_PREP_CONCURRENCY=2 SIM_HTTP_ATTEMPTS=3
```

`SIM_PREP_CACHE_DIR=PATH` selects another cache. The same options are available
as `--preparation-cache`, `--preparation-cache-dir`,
`--preparation-concurrency`, and `--measured-http-attempts` CLI overrides.
Preparation reports retain the legacy `preparation/batch-N/` artifact naming,
but each uncached entry is one independently scheduled wallet. The root report
shows required/cached/queued/active/completed/failed totals, phase elapsed time,
active wallet stages and time since progress. Earlier batch reports still open.

The recovery table includes event correctness, unresolved spends, seeded event
count, HTTP requests/failures, upload/download totals and cumulative HTTP time.
`users[].http_totals` exposes these request aggregates in JSON. The per-wallet
aggregate table and `summary.wallets` group repeated recoveries by sample index,
with outcome counts, exact/unsuccessful latency distributions and HTTP totals.
HTTP time sums request durations and should not be interpreted as wall-clock time.

Preparation adapters retain their bounded transient/overload retry policy.
Measured adapters retry HTTP 408/502/504, connection/request timeouts,
request-send failures on established connections, and interrupted response bodies when
`measured_http_attempts` exceeds one, waiting one second before the second
attempt and two before the third. Typed overloads remain in the wallet's
existing refusal policy, without an extra measured HTTP retry layer.
All attempts and backoff remain inside the original recovery deadline. Ordinary
client defaults remain one HTTP attempt. Server upload deadlines are unchanged.

Request logs include process-local `request_id` and `attempt` (identify a call
with wallet run ID plus request ID). Reports retain every response status and
attempt's submitted/received payload bytes, including failed attempts.
Transport failures also include `transport_error` with the underlying cause chain
in request logs, even when a subsequent retry succeeds. Terminal wallet errors
retain their cause chain in the report. Request construction errors and protocol
validation errors are not retried by this HTTP policy.
`retry_attempts`, `retried_requests`, `http_status_counts`, and
`exact_after_retry` make retries visible alongside ordinary success rates.
End-to-end latency includes backoff; HTTP stage time sums attempts only.
Only a complete recovery matching the original event count and digest is exact,
even after retries. Exhausted retries remain unsuccessful.

The main HTML/JSON report now includes preparation batch summaries, failed-wallet
errors, upload/download totals and relative links to each detailed batch report.
It is created before preparation starts. Batch HTML checkpoints update while work
runs; reload the report to see updates. If preparation fails, measured charts are
hidden and the report explicitly says the measured wave never started.
`make transparent-sim-open` also recognizes older empty failure reports and opens
their saved preparation results without changing historical report files.

The wallet retry path also resumes pending pages before repeating directory
retrieval. A page-setup overload after the directory commit previously created
duplicate pending entries: one copy could finish while the original remained,
producing `PendingLimit` despite matching event counts. A real HTTP regression
test exercises this sequence without preparation-level retries.
