# Warm fleet, third pass, 2026-09-08 (run c)

[Run b](../fleet-warm-2026-09-08-b/README.md) with the harness recording why
each sync stopped short and a 6,000-query budget per sync. Same fleet, same
files.

| Field | Value |
|---|---|
| Run | [Load-test transparent shards, run 34210859218](https://github.com/valargroup/enhance-pir/actions/runs/34210859218); harness `0b5bbd1` |
| Workload | steps 8 (32, 128 not reached), 3 min, 30 completions per class, 6,000 queries per sync |

## Result (8 concurrent wallets, 678 s)

219 syncs, 170 covered, 170 exact, 0 failed, 49 stopped short: 46 for
`unresolved-spends` (every stop in the catch-up and multi-script classes) and
3 reused-tail wallets at the query budget. The step ran 11 minutes because
three reused-tail syncs ran 545–568 s each; that class's heaviest scripts are
the tail of every step and are reported as such.

| Class | n | Covered | Exact | p50 | p95 | p99 | Events | Private bytes per sync | Stopped short |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| catch-up-1d | 24 | 15 | 15 | 0.40 s | 0.73 s | 1.00 s | 109 | 1.12 MB | 9 unresolved-spends |
| catch-up-7d | 24 | 17 | 17 | 0.29 s | 0.50 s | 0.52 s | 137 | 0.81 MB | 7 unresolved-spends |
| catch-up-30d | 24 | 19 | 19 | 0.32 s | 0.54 s | 0.90 s | 192 | 1.04 MB | 5 unresolved-spends |
| restore-6m | 24 | 24 | 24 | 0.84 s | 1.26 s | 1.55 s | 356 | 3.08 MB | |
| restore-old | 24 | 24 | 24 | 3.8 s | 15.0 s | 23.4 s | 2,613 | 73.1 MB | |
| multi-script | 25 | 0 | 0 | 6.9 s | 9.1 s | 9.6 s | 5,613 | 17.4 MB | 25 unresolved-spends |
| reused-tail | 25 | 22 | 22 | 19.2 s | 545 s | 568 s | 973,647 | 490 MB | 3 query-budget |
| small-active | 24 | 24 | 24 | 0.20 s | 0.58 s | 1.21 s | 54 | 2.10 MB | |
| unused | 25 | 25 | 25 | 2.90 s | 3.65 s | 4.29 s | 0 | 63.2 MB filters | |

From harness `dd76da2` onward a sync stopped only for unresolved spends is
counted as covered, with its events still checked against the journal and
the count reported per class; the series runs use that rule. Memory on every
worker was unchanged from run b.
