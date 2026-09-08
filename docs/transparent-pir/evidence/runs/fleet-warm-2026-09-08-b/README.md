# Warm fleet, second pass, 2026-09-08 (run b)

The same pass as [run a](../fleet-warm-2026-09-08-a/README.md) after the
fleet was redeployed at `8802cd0`, where every worker declares parameters for
every geometry the map names. Both archive owners answered queries for the
first time. Files as in run a, plus per-worker `/metrics` scrapes inside
`report.json` (`metrics_before`, `metrics_after` per step).

| Field | Value |
|---|---|
| Run | [Load-test transparent shards, run 34209862968](https://github.com/valargroup/enhance-pir/actions/runs/34209862968); harness `bca5c33` |
| Fleet | workers at `8802cd0` (owners with two build slots for this activation only; no faster, see below), router Caddyfile from assignment `baf1e664…` |
| Workload | steps 8 (32, 128 not reached), 3 min, 30 completions per class, 1,500 queries per sync |

## Result (8 concurrent wallets, 300 s)

266 syncs, 205 covered, 205 exact, 0 failed, 61 stopped short. 0.68 covered
syncs per second, bounded by the step's tail: one reused-tail wallet ran
159 s.

| Class | n | Covered | Exact | p50 | p95 | p99 | Events | Private bytes per sync |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| catch-up-1d | 29 | 18 | 18 | 0.37 s | 0.63 s | 0.73 s | 141 | 1.13 MB |
| catch-up-7d | 29 | 22 | 22 | 0.25 s | 0.45 s | 0.47 s | 161 | 0.82 MB |
| catch-up-30d | 29 | 23 | 23 | 0.27 s | 0.43 s | 0.76 s | 211 | 1.02 MB |
| restore-6m | 30 | 30 | 30 | 0.71 s | 1.12 s | 1.39 s | 426 | 3.06 MB |
| **restore-old** | 29 | 29 | 29 | **3.2 s** | 13.0 s | 22.8 s | 2,845 | 72.5 MB |
| multi-script | 31 | 0 | 0 | 5.8 s | 8.2 s | 8.9 s | 7,103 | 17.4 MB |
| reused-tail | 30 | 24 | 24 | 14.6 s | 158.8 s | 159.0 s | 428,695 | 194 MB |
| small-active | 29 | 29 | 29 | 0.18 s | 0.42 s | 1.49 s | 66 | 1.94 MB |
| unused | 30 | 30 | 30 | 2.38 s | 3.07 s | 3.11 s | 0 | 63.2 MB filters |

Old-birthday restores now complete, exact, in seconds against resident
archive owners: the number the cold pilot could not produce.

The 61 syncs that stopped short are not failures of the service. The
wallet's sync (since `8b5a5c5`) reports a sync incomplete, and withholds its
anchor, when the ledger holds a spend whose receive it never saw. A synthetic
wallet here starts with an empty store at a height inside its history, so
every catch-up or cutoff-based wallet that spent an older output stops on
exactly that rule. [Run c](../fleet-warm-2026-09-08-c/README.md) records the
reasons per class and the harness thereafter counts such a sync as covered.

## Memory after the step

| Worker | Process RSS | cgroup current | Limit |
|---|---:|---:|---:|
| archive owners (80 shards, 45.0 GiB reserved) | 49.7 GB | 51.7 GB | 56 GiB |
| recent replicas (14 shards, 3.5 GiB reserved) | 4.6–4.9 GB | 4.6–4.9 GB | 7 GiB |

Two build slots on the owners made no difference to the warm-up (4.5 s per
runtime either way, about 12 minutes per owner on the faster host, 20 on the
slower); the roster was set back to one slot after this activation.
