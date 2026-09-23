# Preprocessing benchmark validation

The standalone `v4-preprocess` example compiled in release mode and completed a
local aarch64 macOS smoke run. It measured 2K/4K/8K units and 4K/8K/16K/32K query
domains, verified 16 encrypted exact answers, and reloaded/reused artifacts without
canonical rereads. It does not change the deployed runtime.

`local-smoke.jsonl` is validation of the tool, not production performance data.
One repetition on a developer machine is not a stable latency estimate. The native c-4 run below followed the failed sealed attempt and did not overlap
a qualification campaign.
See the example README for exact timing boundaries and caveats. Initial fixture
failure and corrected source identity are recorded in `provenance.json`.

## Native c-4 characterization

Three repetitions completed on the actual production c-4 worker, with its
canonical service stopped and the other replica serving traffic. All 48 encrypted
exact-answer checks passed. Timings below are median elapsed **seconds**; raw
JSON stores milliseconds. Reload can benefit from the OS file cache.

| Unit rows | Encoding + offline precomputation | Persist | Reload |
|---:|---:|---:|---:|
| 2048 | 1.364 | 1.291 | 1.062 |
| 4096 | 2.425 | 1.566 | 1.388 |
| 8192 | 4.467 | 2.127 | 2.000 |

| Query-domain rows | Engine cold preparation | Hint assembly | Packing setup | Artifact reload |
|---:|---:|---:|---:|---:|
| 4096 | 4.378 | 0.919 | 6.087 | 1.381 |
| 8192 | 7.337 | 0.911 | 6.066 | 2.005 |
| 16384 | 14.811 | 2.121 | 6.346 | 3.977 |
| 32768 | 29.455 | 4.484 | 6.201 | 7.979 |

Engine cold preparation includes setup, synthetic rows, unit build and durable
persistence, but excludes separately timed plan/hint/packing steps. In-memory
reuse was approximately 1–2 ms. Unit construction excludes fixture generation,
hashing and global setup. These scopes must not be added indiscriminately or
reported as end-to-end publication latency. The 32K domain has one record of
padding to avoid the lifecycle's lending transition at an exact full boundary.

See `native-provenance.json`, `native-c4.jsonl` and `native-summary.json`. This
characterizes isolated offline work, not sustained memory, multi-replica latency,
concurrent production load or a hardware qualification result.
