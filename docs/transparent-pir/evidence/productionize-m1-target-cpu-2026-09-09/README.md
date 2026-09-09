# Target CPU diagnostic — 2026-09-09

The [failed live canary](../productionize-m1-query-tails-2026-09-09/README.md)
revealed that matching four CPUs and memory limits on the generator did not match
recent-01's hardware. The generator has AVX-512; recent-01 exposes `DO-Regular`,
family 6/model 79, AVX2 and no AVX-512F. The library's automatic policy selects a
different first-dimension backend on these machines.

The diagnostic adds the `portable-kernel` Cargo feature, selecting the library's
existing `ChunkedSplitKernel` for both construction and disk restore. Default
production selection remains automatic. Neither algorithm parameters, encoded
snapshot layout, setup identity nor wire binding change. Reports record the
kernel policy; runner manifests include CPU information where available.

One focused portable-backend run (`portable/`) completed a two-publication burst
in 10.675 s on Amsterdam: 79 exact queries, seven retry attempts (8.14%), maximum
completion gap 1.415 s, complete warm readiness/client drain, no cgroup memory
limit/OOM events and 26.4% modeled headroom. It passes timing but misses the old
7.79% retry reference. This diagnostic is not three-repeat qualification.
More importantly, selecting the target backend did not reproduce the much slower
live preparation. Do not attribute the live slowdown solely to backend choice.

A read-only 120-second target sample (`target-profile.ndjson`) captures CPU,
process and cgroup CPU usage, process I/O and scheduling/I/O pressure after the
canary query clients stopped. `target-profile-summary.json` contains one-second
derived intervals. Two preparation windows approached all four CPUs; average
host I/O wait was 0.23%. Maximum one-second steal was 11.52% across the complete
window, with smaller values in most active samples. These observations point to
computation/target CPU performance rather than a disk-read stall; they do not
prove every long preparation has the same cause.

The feature build passed Linux disk ownership/recovery tests and the full
recent/archive cold-versus-restored query equivalence test, including corruption
rejection (`linux-tests.log`, `linux-equivalence.log`). `make check` passed 585
Rust tests, zero failures and two ignored manual benchmarks. The source archive
matches every manifest-listed hash. The benchmark executable SHA is
`de8258a02613b58d59ab213c143116f8f3c7de86b266d11392632de9e14553d4`.

Reproduce with the prior full-residency command, building the executable with
`cargo test --release -p transparent-shard-server --features portable-kernel --lib --test revisions_and_cache --no-run`,
then selecting `--build-slots 2 --repetitions 1`. Generator root:
`/opt/transparent-portable-20260909`; supervisor `transparent-m1-portable.service`
completed successfully on its timing/memory checks. The separate query reference
still rejects promotion. This code was not deployed to the live worker.
