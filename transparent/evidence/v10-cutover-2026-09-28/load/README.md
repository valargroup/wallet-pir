# Bounded public production load, 2026-09-28

The production client binary is from `8e69ea75`, SHA-256
`368b57a56908e258e8f1aec43c792b6cefc5a92d4640adeb2c01f2b5a5ad503a`.
The Amsterdam client host was `roman-ipir-bench-8vcpu`, 8 vCPU / 32 GiB,
`g-8vcpu-32gb-intel`, over public HTTPS. The frozen version-2 sample has 1,080
synthetic public-script groupings, SHA-256
`d9ce4f84249f9559ba0adaee0bae88f13b3f97f86aa79d92119ae04141e5f0a1`,
pinned to height 3,473,686. It is not a wallet-population sample.

The [one-client smoke](smoke.json) covered eight ordinary classes and completed
124/124 exact syncs, zero failed, in 89.405 s. The [four-client run](load-4.json)
covered all nine classes: **47 attempts, 46 completed and exact, zero failed,
one `query-budget` incomplete**, over **710.182 s**. The run dispatched for up
to 300 s and then waited for in-flight work; the 600 s flag is a per-HTTP-request
deadline, not a whole-sync timeout. Each sync had a 6,000-private-query limit.
The previous v9 baseline had 47 attempts, 45 exact completions and two query-budget
incompletes. Its older README incorrectly labels those as 600 s timeouts; the
raw [v9 report](../../v9-cutover-2026-09-28/load/v9-pinned-live-2.json) records the budget reason.

| Class | Exact / attempts, v9 → v10 | Logical MB / attempt, v9 → v10 | Change | Private queries / attempt, v9 → v10 | p50 / p95 s, v9 → v10 |
|---|---|---|---:|---|---|
| catch-up-1d | 5/5 → 5/5 | 0.308 → 0.363 | +17.96% | 2.6 → 2.8 | 0.132 / 0.175 → 0.156 / 0.193 |
| catch-up-30d | 5/5 → 5/5 | 0.601 → 0.543 | -9.52% | 4.2 → 3.8 | 0.200 / 0.459 → 0.161 / 0.270 |
| catch-up-7d | 5/5 → 5/5 | 0.550 → 0.393 | -28.54% | 4.4 → 3.2 | 0.192 / 0.398 → 0.164 / 0.240 |
| multi-script | 5/5 → 5/5 | 5.458 → 4.874 | -10.70% | 65.8 → 60.0 | 2.679 / 3.585 → 2.173 / 3.369 |
| restore-6m | 5/5 → 5/5 | 1.203 → 1.137 | -5.51% | 5.4 → 5.4 | 0.331 / 1.075 → 0.232 / 1.003 |
| restore-old | 5/5 → 5/5 | 45.599 → 37.258 | -18.29% | 42.2 → 32.0 | 2.627 / 17.167 → 1.706 / 10.631 |
| reused-tail | 4/6 → 5/6 | 1019.923 → 866.726 | -15.02% | 2428.7 → 2050.3 | 22.047 / 785.919 → 14.599 / 710.655 |
| small-active | 5/5 → 5/5 | 0.433 → 0.415 | -4.13% | 1.2 → 1.2 | 0.087 / 0.160 → 0.072 / 0.176 |
| unused | 6/6 → 6/6 | 31.629 → 26.336 | -16.73% | 0.3 → 0.3 | 1.000 / 2.447 → 0.655 / 0.852 |

All latency distributions and stage totals include **all attempts**, including
the budget-limited history. MB means decimal megabytes. The bytes are logical
wallet-stage payloads; they exclude HTTP/TLS overhead and retransmissions hidden
by the transport retry layer. [The comparison](comparison-v9.json) separates
cacheable public setup, filters, key upload, non-key encrypted-query upload and
private response bytes. Key upload is derived at 27,648 bytes per native query;
the remainder of each query body is non-key upload. The runner reported zero
terminal 503s; it does not provide the regression suite's per-attempt HTTP audit.

The one-day catch-up increase is real in these five attempts: filter bytes grew
from 471,225 to 622,815, manifest payload grew by 21,470 bytes, and the class
fetched four pages instead of three (plus one setup). The same row geometry
does not make every wallet cheaper when shard boundaries and inline/page
placement change. Eight classes used 4.13–28.54% less payload per attempt.

Eight completed synthetic syncs report `unresolved-spends`: three catch-up-7d
and five multi-script. Their requested event ranges exactly match the journal,
but their deliberately empty initial stores lack earlier funding receives. The
harness counts those ranges as complete while the real wallet withholds its anchor.
This is distinct from the one genuinely unfinished query-budget outcome.

The same sample and bounded concurrency/limits were used for both formats, but
the runs occurred at different tips/times with uncontrolled background activity.
There is one run per format and only 5–6 attempts per class. These observations
do not establish a causal latency gain, a sustained request rate or an accepted
M5 operating envelope. The step's 0.0648 completed syncs/s is dominated by its
long in-flight histories and is not fleet capacity.

[Resource observations](../observation-summary.json) cover the client run:
minimum available host memory **73.84%**, maximum reported publication freshness
**23.249 s**, no OOM events, automatic service restarts, readiness failures or
cache-write failures in the samples. There were 23 resource samples per worker
during the four-client run. MemoryPeak is cumulative since service start; it is
not an isolated peak caused by this load.

One of 142 no-retry public-origin samples during the four-client interval got
an init 503 at **22:28:48.730 UTC**; both map endpoints returned the same old map.
The next sample at 22:28:53.730 returned all 200s on the new map. The
[controller log](../deployment/init-transition.log) records activation of height
3,499,635 at 22:28:49.390. The source intentionally refuses init if a publication
epoch changes or no matching upstream snapshot remains while it assembles the
response. This timing is consistent with that existing fail-closed transition
guard; it is not a traced proof of which branch ran. No completed client ledger
mismatched and no client sync failed. The observation remains in the raw traces
and prevents a claim of uninterrupted availability.

The post-load check again verified all six warm binaries, both origins, the
170 sealed setup hashes and canonical height 3,499,638. Public operator routes
`/metrics`, `/v1/ready` and `/v1/health` all returned 404. All commands, initial
input hashes and the successful terminal marker are retained in this directory.
