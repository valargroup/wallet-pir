# Warm fleet, first pass, 2026-09-08 (run a)

The first load pass against the activated fleet, from the coordinator inside
the VPC through the router's internal plain-HTTP listener, with every worker
warm under the assignment at 6% headroom. `report.json` is the harness
report, `harness.log` its console output, `time.txt` the client process's
resource accounting, `sample-sha256.txt` the workload sample's digest.

| Field | Value |
|---|---|
| Run | [Load-test transparent shards, run 34204504192](https://github.com/valargroup/enhance-pir/actions/runs/34204504192); harness commit `4ba9019` |
| Fleet | 4 recent replicas `s-4vcpu-8gb` (14 shards, 4.4 GB resident each), 2 archive owners `m-8vcpu-64gb` (80 shards, 46.9 GB resident each), router `s-2vcpu-4gb`; workers at `16631a1`, router Caddyfile from assignment `baf1e664…`; ams3 |
| Client | `enhance-pir-coordinator-01`, 8 shared vCPU, plain HTTP over the VPC; 264% CPU, 382 MB peak |
| Workload | steps 8, 32, 128 wallets; 3 min per step; 30 completions per class; 1,500 queries per sync; the 1,080-wallet sample |

## Result (step 1, 8 concurrent wallets, 198 s)

847 syncs started, 652 completed, **652 exact**, 194 failed. 3.29 completed
syncs per second. The step stopped the run on its error rate (0.23).

| Class | n | Exact | Failed | p50 | p95 | p99 | Events | Private bytes per sync |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| catch-up-1d | 93 | 93 | 0 | 0.41 s | 0.66 s | 0.76 s | 437 | 1.03 MB |
| catch-up-7d | 94 | 94 | 0 | 0.30 s | 0.53 s | 0.75 s | 802 | 0.81 MB |
| catch-up-30d | 94 | 94 | 0 | 0.33 s | 0.97 s | 1.47 s | 2,226 | 1.14 MB |
| restore-6m | 94 | 94 | 0 | 0.87 s | 1.84 s | 2.75 s | 1,869 | 3.19 MB |
| multi-script (40 scripts) | 95 | 94 | 0 | 6.8 s | 9.9 s | 76 s | 79,444 | 20.1 MB |
| unused (4 scripts) | 94 | 94 | 0 | 2.95 s | 4.0 s | 4.8 s | 0 | 63.2 MB of filters, no private work |
| small-active | 94 | 83 | 11 | 0.20 s | 0.37 s | 0.62 s | 179 | 0.93 MB |
| restore-old | 94 | 0 | 94 | | | | 0 | |
| reused-tail | 95 | 6 | 89 | | | | 31,563 | |

Every completed sync was exact: the store's events digested to the journal's
expectation for that wallet. Bytes are the wallet's own accounting (filters,
setup, query uploads and responses); the harness does not measure TLS or
connection bytes, and this listener carried no TLS.

## The failures

All 194 failures are one client-side message, raised before any archive
query: "the map uses geometry archive-wide but the service declares no
parameters for it". The wallet fetches the init document once, through the
router, which sends it to the recent pool; a replica assigned only the recent
tier declared parameters for `recent-8k` alone, and the wallet correctly
refused a map naming a geometry the document did not declare. The eleven
`small-active` failures are wallets whose script's first event lies below the
cutoff. Both archive owners answered zero queries during the run.

The fix is set-wide declaration: a worker declares parameters for every
geometry the map names, whatever it is assigned (`shardset::geometries`),
tested on a subset worker in `tests/assignment.rs`. The next pass measures
the archive classes.

## Reading the recent-tier figures

These are warm, resident, uncontended numbers at 8 wallets, from a client on
the same VPC: an upper bound on what a wallet over the internet sees, not a
product SLO. The 76 s p99 for the 40-script class is one wallet with 56,703
events paging through the recent tier. Later steps at 32 and 128 wallets did
not run in this pass.
