# Fleet series, repetition 3 (load generator), 2026-09-08

As [repetition 2](../fleet-series-2026-09-08-r2/README.md): same host,
binary, sample, fleet and workload. Ran 11:57–12:51Z.

| Step | Duration | Syncs | Covered | Exact | Failed | Covered/s |
|---|---:|---:|---:|---:|---:|---:|
| 8 wallets | 1,233 s | 438 | 433 | 433 | 0 | 0.35 |
| 32 wallets | 1,431 s | 1,108 | 1,097 | 1,097 | 1 | 0.77 |
| 128 wallets | 554 s | 1,822 | 1,704 | 1,704 | 118 | 3.08 |

Every covered sync exact in every step. Client: 810% CPU over 54 min.

## Latency by class (p50 / p95 / p99, seconds)

| Class | 8 wallets | 32 wallets | 128 wallets |
|---|---|---|---|
| catch-up-1d | 0.43 / 0.72 / 0.75 | 0.84 / 1.32 / 1.61 | 2.9 / 5.2 / 6.8 |
| catch-up-7d | 0.31 / 0.46 / 0.83 | 0.61 / 1.04 / 1.21 | 2.1 / 3.7 / 5.6 |
| catch-up-30d | 0.31 / 0.63 / 0.94 | 0.66 / 1.75 / 3.03 | 2.2 / 6.0 / 9.4 |
| restore-6m | 0.84 / 1.26 / 1.74 | 1.8 / 3.6 / 4.6 | 5.8 / 12.6 / 15.8 |
| restore-old | 4.0 / 25.3 / 30.7 | 8.1 / 37.9 / 63.0 | 24.1 / 73.1 / 137 |
| multi-script (40 scripts) | 7.0 / 11.3 / 12.1 | 14.5 / 21.6 / 37.2 | 49.5 / 75.2 / 121 |
| small-active | 0.21 / 0.66 / 1.64 | 0.42 / 1.20 / 3.23 | 1.3 / 6.1 / 10.2 |
| unused (filters only) | 2.8 / 3.6 / 4.0 | 6.0 / 7.2 / 7.9 | 18.2 / 45.1 / 46.3 |
| reused-tail | 16.6 / 822 / 829 | 35.9 / 1,116 / 1,224 | 112 / 404 / 445 |

At 32 wallets the medians agree with repetition 2 within 5% on every class.

## Bytes per sync at 32 wallets (wallet accounting, no TLS)

| Class | Filters | Directory queries | Page queries | Setup | Total |
|---|---:|---:|---:|---:|---:|
| catch-up-1d | 0.13 MB | 0.72 MB | 0.13 MB | 0.05 MB | 1.0 MB |
| catch-up-7d | 0.13 MB | 0.57 MB | 0.08 MB | 0.03 MB | 0.8 MB |
| catch-up-30d | 0.34 MB | 0.63 MB | 0.12 MB | 0.05 MB | 1.1 MB |
| small-active | 1.4 MB | 0.37 MB | 0.01 MB | 0.03 MB | 1.9 MB |
| restore-6m | 1.3 MB | 1.6 MB | 0.16 MB | 0.12 MB | 3.1 MB |
| multi-script | 1.3 MB | 14.1 MB | 3.9 MB | 0.4 MB | 19.7 MB |
| restore-old | 63.2 MB | 6.1 MB | 3.5 MB | 0.3 MB | 73 MB |
| unused | 63.2 MB | 0 | 0 | 0 | 63.2 MB |
| reused-tail | 18.2 MB | 14.3 MB | 303 MB | 1.1 MB | 336 MB |

Filters dominate any wallet that starts from genesis: 60 MB of public
bytes before the first private query. Directory queries dominate the rest.

## The 128-wallet failures

All 118 failures were HTTP 503 on archive-01's shards (0, 1, 16, …) and, at
the step's end, archive-02's; both owners were stopped and restarted by
hand during the step (archive-01 at 12:33Z, archive-02 at 12:47Z; clean
`systemctl` stops, no workflow run in the window) from another session's
work on the fleet, and each answered 503 for the 8–15 minutes of its
warm-up. Archive-02's `/metrics` was unreachable at the step boundary for
the same reason. Nothing else failed: no queue, budget or deadline
rejection on any worker. As in repetition 2, the 128-wallet error rate is
an operator restart, not saturation; a clean 128-wallet step is still owed.
