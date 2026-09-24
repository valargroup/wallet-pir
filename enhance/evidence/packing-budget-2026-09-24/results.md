# Measured results

GiB = 1,073,741,824 bytes. Peaks below come from the systemd unit MemoryPeak property.
Fresh cgroup per trial; no swap. See [method and limitations](README.md).

| Trial | Outcome | Cgroup peak (GiB) | Verified packed responses |
|---|---|---:|---:|
| `prepare16384` | success | 3.091 | — |
| `prepare32-v3` | success | 4.260 | — |
| `prepare32` | exit-code | 4.235 | — |
| `prepare4096` | success | 2.326 | — |
| `prepare8192` | success | 2.517 | — |
| `resident-limit-4g` | oom-kill | 4.000 | — |
| `resident-limit-8g` | oom-kill | 8.000 | — |
| `rows16384-s1-c4` | success | 1.749 | 64 |
| `rows4096-s1-c4` | success | 1.749 | 64 |
| `rows8192-s1-c4` | success | 1.748 | 64 |
| `s3-c4-build-4g` | success | 3.562 | 128 |
| `s4-c1-4g` | success | 3.370 | 32 |
| `s6-c4-build-7g-cold` | success | 6.266 | 512 |
| `s7-c16-build-7g` | success | 6.749 | 256 |
| `s7-c4-build-7g-cold` | success | 6.943 | 512 |
| `s7-c4-build-7g-r1` | success | 6.735 | 512 |
| `s7-c4-build-7g-r2` | success | 6.737 | 512 |
| `s7-c4-build-7g-r3` | success | 6.706 | 512 |
| `s8-c4-build-7g` | oom-kill | 7.000 | — |
| `s8-c4-build-8g` | success | 7.413 | 128 |
| `s9-c4-build-8g` | oom-kill | 8.000 | — |

Preparation runs include databases and client setup; they are not router peaks.
Across completed measurement runs, 3,296 responses matched preverified fixture hashes.
Fixture preparation separately decrypted and checked 28 answers. OOM trials have
no completion event and are excluded from those success counts.

## Packing latency under the proposed memory workloads

These are in-process Packing::pack durations under a closed-loop fixture workload.
They exclude body decoding before the timed call, HTTP and worker evaluation,
and do not establish offered-rate QPS or end-to-end latency.

| Trial | Responses | Pack p50 (ms) | Pack p99 (ms) | Query/build phase (s) |
|---|---:|---:|---:|---:|
| `s6-c4-build-7g-cold` | 512 | 79.83 | 153.32 | 24.77 |
| `s7-c16-build-7g` | 256 | 321.10 | 1852.25 | 8.90 |
| `s7-c4-build-7g-cold` | 512 | 88.95 | 107.51 | 26.99 |
| `s7-c4-build-7g-r1` | 512 | 85.15 | 153.86 | 25.94 |
| `s7-c4-build-7g-r2` | 512 | 80.05 | 108.14 | 25.56 |
| `s7-c4-build-7g-r3` | 512 | 80.63 | 112.36 | 25.92 |
| `s8-c4-build-8g` | 128 | 80.41 | 2579.08 | 8.34 |
