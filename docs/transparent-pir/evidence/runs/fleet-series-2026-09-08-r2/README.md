# Fleet series, repetition 2 (load generator), 2026-09-08

The second Gate 6 repetition, run from `transparent-pir-loadgen-01` (a `c-16`
dedicated-CPU droplet in the VPC, Xeon 8280) with the harness binary built by
the coordinator's runner at `dd76da2` and the committed workload sample.
`report.json`, `harness.log` and `time.txt` are the harness's own files.

Fleet as in [run c](../fleet-warm-2026-09-08-c/README.md); through the
router's internal listener; 10 min per step, 100 completions per class,
6,000 queries per sync; per-worker `/metrics` scraped at every step boundary
and kept inside `report.json`.

## Summary

| Step | Duration | Syncs | Covered | Exact | Failed | Covered/s |
|---|---:|---:|---:|---:|---:|---:|
| 8 wallets | 1,226 s | 439 | 434 | 434 | 0 | 0.35 |
| 32 wallets | 1,401 s | 1,111 | 1,096 | 1,096 | 4 | 0.78 |
| 128 wallets | 1,668 s | 1,881 | 1,763 | 1,763 | 105 | 1.06 |

The run stopped at 128 on the 5% error rule. Every covered sync in every
step was exact. Client: 862% CPU over 1 h 12 min, 7.5 GB peak.

## Latency by class (p50 / p95 / p99, seconds)

| Class | 8 wallets | 32 wallets | 128 wallets |
|---|---|---|---|
| catch-up-1d | 0.44 / 0.78 / 0.87 | 0.88 / 1.55 / 1.70 | 2.8 / 4.9 / 5.6 |
| catch-up-7d | 0.30 / 0.59 / 0.78 | 0.66 / 1.08 / 1.74 | 1.9 / 3.4 / 4.5 |
| catch-up-30d | 0.32 / 0.61 / 0.99 | 0.68 / 1.74 / 3.22 | 2.0 / 5.3 / 9.5 |
| restore-6m | 0.85 / 1.20 / 2.22 | 1.9 / 3.5 / 4.5 | 5.6 / 11.1 / 16.6 |
| restore-old | 3.9 / 24.9 / 29.8 | 8.8 / 36.9 / 61.5 | 23.0 / 34.1 / 129 |
| multi-script (40 scripts) | 6.9 / 10.6 / 12.7 | 14.9 / 22.0 / 34.4 | 49.4 / 75.9 / 119 |
| small-active | 0.23 / 0.56 / 1.57 | 0.46 / 1.44 / 3.26 | 1.3 / 4.2 / 10.3 |
| unused (filters only) | 3.0 / 4.0 / 4.1 | 6.3 / 8.1 / 9.2 | 17.9 / 25.0 / 26.0 |
| reused-tail | 15.8 / 819 / 828 | 35.3 / 1,095 / 1,188 | 112 / 1,421 / 1,630 |

## The 128-wallet failures, and what they measured

Of the 105 failures, 104 were HTTP 503 on archive-01's shards (0, 1, 16, 24
and others), and the per-worker scrape at the step's end shows archive-01
with zero queries served and 27.6 GB resident: it had just been restarted.
Its journal shows two clean stop/start cycles during this run, at 11:18Z and
11:43Z, each followed by a 15- and 8-minute warm-up, from fleet deploys
dispatched by another session against single workers (runs 34219443444 and
34221468055; `recent-01` likewise at 11:07Z). During a warm-up the worker
answers 503 without a delay, which the wallet correctly treats as not
retryable, and the harness counts as failed. So the 128-wallet error rate is
an availability event under an operator restart, not saturation; the same
step's other classes, all served by workers that were not restarted, failed
nothing. The step's 129 s p99 for old restores and the archive-01 counters
are from a worker that was cold for two of its ten minutes.

The remaining failure, "shard 167 holds a script twice", was a wallet-side
bug: a script's two candidate directory rows are two salted hashes that can
coincide (one script in 8,192 at `recent-8k`); the wallet fetched the same
row twice, saw its entry twice, and refused. Fixed to keep both fetches, so
the query count stays independent of the script, and to treat a second
sighting from the same row as the same entry.

## What the fleet did

No worker refused a query in any step: queue, body-budget and deadline
rejections stayed at zero. Archive-02, holding shards 80–159 and the reused
scripts, served 409,296 queries over the run against archive-01's 21,506
before its restart and about 55,000 per replica; its queue depth peaked at 6
and its load at about 8 on 8 vCPUs during the 128-wallet step. The demand
skew between the two owners, seven to one, is the capacity finding of this
series: the archive split is even by resident bytes and uneven by demand.
Owners held 49.7 GB resident and replicas 4.6–4.9 GB throughout.

The load generator itself reached load 28 on 16 cores at 128 wallets; the
client-side PIR work is not negligible and a mobile wallet's own cost is
still unmeasured here.
