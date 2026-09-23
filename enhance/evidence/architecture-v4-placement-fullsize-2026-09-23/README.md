# Updated placement full-size validation — September 23, 2026

Both normally ignored HTTP campaigns passed against the reorg replication and
memory-relocation changes: two tests, zero failures, in 419.10 seconds. They ran
sequentially on the local 128 GiB macOS host, with 333 GiB free disk at launch.

- Consolidation exercises seven shard domains across four workers, injects a
  reservation failure, verifies the canonical update preserves original placement,
  retries the sealed-shard move, and checks current and retained encrypted queries.
- The full 32K loan/return campaign checks boundary records and old borrower queries,
  rejects a canonical-anchor change after preparation, and verifies reorg recovery.

These are existing functional campaigns rerun on updated sources. They do not
force a multi-shard memory-pressure rearrangement or qualify an 8 GiB host.
The focused relocation tests are recorded in the
[memory relocation report](../architecture-v4-memory-relocation-2026-09-23/README.md).

## Fresh Linux binaries and runtime

All three Linux x86-64-v3 binaries rebuilt offline successfully in 37.50 seconds
using Rust 1.91.0 and the original CI flags. `source-sha256.json` identifies the
frozen source snapshot; `binaries.sha256` identifies the actual ELF outputs.
The source remains uncommitted, so this is not a clean deployable release artifact.

Under native ARM64 QEMU 10.2.3, the updated server help and two-worker/coordinator
HTTP load campaign passed. Two 90-second steps at concurrency 1 and 2 returned
1,265 correct measured answers and 25 correct warmup answers, with zero incorrect
answers or request errors. Generation 1 advanced to generation 4, satisfying the
two-publication requirement with three completed publications. Reports and raw
health/manifests are in `linux-campaign/`.

The emulator identity is in `runtime.json`; launcher scripts are in `qemu-wrappers/`.
Binary hashes inside the campaign manifest identify those launchers, while
`binaries.sha256` identifies the ELF programs they execute. As in the original
Linux diagnosis, direct execution under the default emulator is not the validated
runtime path. The campaign ran concurrently with the native tests on the same
physical host; timing is not production performance evidence. Diagnostic containers
are stopped and retained outside the repository.

Full-size memory/latency qualification on native 8 GiB workers, six-hour campaigns,
wallet conformance, clean release artifacts and deployment remain outstanding.
