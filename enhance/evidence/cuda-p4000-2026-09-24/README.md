# Optional CUDA worker validation on P4000

Both explicitly invoked GPU tests passed: encrypted bootstrap/cached restart and
real coordinator/two-worker HTTP round trips, retention, failover and restart.
The composed-domain benchmark matched CPU outputs for every timed query.

## Measured matrix evaluation

A 32768 × 12288 database is evaluated through four actual immutable wallet-pir
units. Each of three repetitions evaluates 100 canonical vectors after ten GPU
warmups. CPU and GPU use identical inputs on the same host, with eight Rayon threads.

| Evaluation | Run 1 | Run 2 | Run 3 |
|---|---:|---:|---:|
| CPU, ms/query | 27.499 | 28.049 | 28.647 |
| GPU, one caller, ms/query | 9.794 | 8.987 | 8.988 |
| GPU, two callers, ms/completed query | 8.458 | 8.401 | 8.443 |

The mean single-caller matrix-stage improvement is approximately 3.03×.
The two-caller values measure aggregate throughput, not individual request latency.
Packing remains on CPU and is excluded from this benchmark; this is not a
whole-request PIR speedup or serving-capacity qualification.

Fresh CPU preparation took 27.81 seconds. GPU preparation from the CPU artifacts
took 8.94 seconds, including artifact IO and host reconstruction as well as GPU
compilation/upload. These measure different preparation paths.

The host database was 768 MiB. GPU memory sampled every 100 ms peaked at 887 MiB
and returned to 3 MiB after the process exited. The benchmark verified unit Arc
reuse and release. Combined CPU/GPU process peak RSS was 1,794,192 KiB; this is
not standalone GPU-worker RSS. Multiple retained or candidate units need separate
host and device memory qualification.

## Provenance and limits

Source: `3279d835b5977b6aa3b78b496885180bae5eb247`; upstream IPIR:
`1d8aea341d844df97ef6eb244c24b0a050eb3e9b`. Build: Rust 1.91.0,
`release-fast`, CUDA feature, native CPU target. See [manifest.json](manifest.json)
for commands, workload, hardware and limits, and [environment.txt](environment.txt)
for binary hashes and host details. [benchmark.sh](benchmark.sh) captures the
benchmark and GPU memory samples. All raw logs are retained beside this note.

Paperspace AMS1: Quadro P4000 8 GiB, eight Xeon E5-2623 v4 vCPUs, about 29 GiB
host RAM, Ubuntu 22.04.5, NVIDIA driver 580.178.04 and NVRTC 12.2.140. The card
reported a corrupted infoROM warning through nvidia-smi. No evaluation error was
observed, but this rented VM run does not qualify other hardware or production.
No production deployment or release artifact was changed.
