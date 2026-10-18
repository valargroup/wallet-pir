# Worker artifact cache reclaim — September 24, 2026

Source `ac7cf9d` adds best-effort Linux `POSIX_FADV_DONTNEED` after durable
large-artifact writes and completed artifact reads. The release binary SHA-256 is
`5d85d894fed6c5c0b827c90dc56336804074849dab390a01e61d90b046ce0493`.
It is running on the coordinator and both 8 GiB workers at
https://enhance-pir.valargroup.dev. The prior `2a83c21` release and service
drop-ins are retained for rollback. Worker `MemoryHigh=7G`, `MemoryMax=7680M`,
`MemorySwapMax=2G`, placement, and admission policy were unchanged.

The active-profile physical workload used four sealed shards plus an active
frontier on isolated worker state. Roman stopped the exercise after 23
publications, before its 1,800-second and 30-publication gate. The persisted
`exercise-interrupted.json` still says `running`; that is the honest on-disk
state at interruption, not a completed workload receipt. The worker traces
cover about 1,572 seconds of the measured window, including composition growth,
tail removal, frontier expansion, and rewind. `partial-assessment.json` records
the derived result. This is **encouraging partial evidence, not a passing focused
assessment or six-hour hardware qualification**.

| Measured worker | Peak cgroup memory | Peak file cache | New `memory.high` events | Peak cgroup swap | Host swap pages in/out |
|---|---:|---:|---:|---:|---:|
| Worker 1 | 5,580 MiB | 193 MiB | 0 | 0 | 0 / 0 |
| Worker 2 | 5,561 MiB | 194 MiB | 0 | 0 | 0 / 0 |

Both worker sample streams had zero errors, about two-second cadence, and one
process identity. The coordinator stream recorded nine errors around its
intentional stop. The original failing run reached the 7,168 MiB soft limit
with roughly 100–124 MiB of worker swap; see the
[original v7 report](../immutable-v7-2026-09-24/README.md). The new traces
support the cache-pressure diagnosis, but cannot establish the uncompleted
duration/publication gate.

After stopping the isolated exercise, the same binary was installed into
canonical serving on all three hosts. The live process SHA-256 matched the
release on each host, with active services and zero restarts. Public health
reported protocol v7, two published replicas, no blocked reason, generation
35, placement revision 1, and anchor height 3,494,715 at the deployment check.
`public-oracle-load.json` records 296/296 correct public HTTPS answers, zero
errors, in a 15-second two-client run after a three-second warmup (62 further
correct answers). This short check is for correctness, not the outstanding p99
comparison gate.

Raw evidence: `worker-1-samples.jsonl.gz`, `worker-2-samples.jsonl.gz`,
`coordinator-samples.jsonl.gz`, `publications.jsonl.gz`, the interrupted
exercise JSON, the partial assessment, and the public oracle report. Checksums
are in `SHA256SUMS`.
